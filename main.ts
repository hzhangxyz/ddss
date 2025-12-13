import { promisify } from "node:util";
import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";
import * as grpc from "@grpc/grpc-js";
import { Search as Search_, type Rule } from "atsds";
import { parse, unparse } from "atsds-bnf";
import {
    type JoinRequest,
    type JoinResponse,
    type LeaveRequest,
    type LeaveResponse,
    type ListRequest,
    type ListResponse,
    type MetaDataRequest,
    type MetaDataResponse,
    type PushDataRequest,
    type PushDataResponse,
    type PullDataRequest,
    type PullDataResponse,
    type Node,
    EngineKind,
    ClusterClient,
    type ClusterServer,
    ClusterService,
    EngineClient,
    type EngineServer,
    EngineService,
} from "./ddss.js";

/**
 * 节点客户端接口
 * 包含集群管理和引擎服务的客户端
 */
interface NodeClient {
    cluster: ClusterClient;
    engine: EngineClient;
}

/**
 * 带客户端的节点信息
 * 存储节点ID、地址和对应的gRPC客户端
 */
interface NodeInfoWithClient {
    id: string;
    addr: string;
    client: NodeClient;
}

/**
 * 积极引擎接口
 * 定义引擎必须实现的基本操作
 */
interface EagerEngine {
    /**
     * 接收数据
     * @param {string} data - 输入数据
     * @returns {string | null} - 如果接受返回格式化后的数据，否则返回 null
     */
    input(data: string): string | null;

    /**
     * 执行一轮搜索
     * @param {function} callback - 处理结果的回调函数
     * @returns {number} - 搜索结果数量
     */
    output(callback: (result: string) => void): number;

    /**
     * 获取引擎元数据
     * @returns {Object} - 包含输入和输出模式的对象
     */
    meta(): { input: string[]; output: string[] };
}

/**
 * 集群管理器
 * 负责节点发现、集群状态管理、节点加入和离开
 */
class ClusterManager {
    private nodes: Map<string, NodeInfoWithClient>;
    private localNodeId: string;
    private localNodeAddr: string;

    constructor(nodeId: string, nodeAddr: string) {
        this.localNodeId = nodeId;
        this.localNodeAddr = nodeAddr;
        this.nodes = new Map();
    }

    /**
     * 获取所有节点
     */
    getNodes(): Map<string, NodeInfoWithClient> {
        return this.nodes;
    }

    /**
     * 获取本节点ID
     */
    getLocalNodeId(): string {
        return this.localNodeId;
    }

    /**
     * 获取本节点地址
     */
    getLocalNodeAddr(): string {
        return this.localNodeAddr;
    }

    /**
     * 更新本节点地址
     */
    setLocalNodeAddr(addr: string): void {
        this.localNodeAddr = addr;
    }

    /**
     * 添加节点到集群
     * @param {string} id - 节点 ID
     * @param {string} addr - 节点地址
     * @param {NodeClient} client - 节点客户端
     */
    addNode(id: string, addr: string, client: NodeClient): void {
        this.nodes.set(id, { id, addr, client });
    }

    /**
     * 移除节点
     */
    removeNode(id: string): void {
        this.nodes.delete(id);
    }

    /**
     * 检查节点是否存在
     */
    hasNode(id: string): boolean {
        return this.nodes.has(id);
    }

    /**
     * 获取节点信息
     */
    getNode(id: string): NodeInfoWithClient | undefined {
        return this.nodes.get(id);
    }

    /**
     * 获取集群服务处理器
     * 返回 gRPC 服务实现
     */
    getClusterServiceHandlers(): ClusterServer {
        return {
            /**
             * 处理节点加入请求
             * 将请求节点加入本地节点列表
             */
            join: async (
                call: grpc.ServerUnaryCall<JoinRequest, JoinResponse>,
                callback: grpc.sendUnaryData<JoinResponse>,
            ) => {
                const node = call.request.node;
                if (node) {
                    const { id, addr } = node;
                    if (!this.nodes.has(id)) {
                        // Create clients to connect back to the joining node
                        const client: NodeClient = {
                            cluster: new ClusterClient(addr, grpc.credentials.createInsecure()),
                            engine: new EngineClient(addr, grpc.credentials.createInsecure()),
                        };
                        this.nodes.set(id, { id, addr, client });
                        console.log(`Joined node: ${id} at ${addr}`);
                    }
                }
                callback(null, {});
            },
            /**
             * 处理节点离开请求
             * 将请求节点从本地节点列表中移除并清理客户端连接
             */
            leave: async (
                call: grpc.ServerUnaryCall<LeaveRequest, LeaveResponse>,
                callback: grpc.sendUnaryData<LeaveResponse>,
            ) => {
                const node = call.request.node;
                if (node) {
                    const { id, addr } = node;
                    if (this.nodes.has(id)) {
                        const nodeInfo = this.nodes.get(id);
                        if (nodeInfo) {
                            // Clean up clients to prevent resource leaks
                            nodeInfo.client.cluster.close();
                            if (nodeInfo.client.engine) {
                                nodeInfo.client.engine.close();
                            }
                        }
                        this.nodes.delete(id);
                        console.log(`Left node: ${id} at ${addr}`);
                    }
                }
                callback(null, {});
            },
            /**
             * 处理节点列表请求
             * 返回当前集群中所有节点的列表
             */
            list: async (
                _call: grpc.ServerUnaryCall<ListRequest, ListResponse>,
                callback: grpc.sendUnaryData<ListResponse>,
            ) => {
                const nodes = Array.from(this.nodes.values()).map((n) => ({
                    id: n.id,
                    addr: n.addr,
                }));
                callback(null, { nodes });
            },
        } as ClusterServer;
    }

    /**
     * 从远程节点获取节点列表
     * @param {string} addr - 要查询的节点地址
     * @returns {Promise<Node[]>} 节点列表
     */
    async listRemoteNodes(addr: string): Promise<Node[]> {
        const client = new ClusterClient(addr, grpc.credentials.createInsecure());
        const listAsync = promisify<ListRequest, ListResponse>(client.list).bind(client);
        const response = await listAsync({});
        client.close();
        console.log(`Listing nodes: ${response.nodes.length} nodes from ${addr}`);
        return response.nodes;
    }

    /**
     * 向指定节点发送加入请求
     */
    async sendJoinRequest(targetNode: NodeInfoWithClient): Promise<void> {
        const joinAsync = promisify<JoinRequest, JoinResponse>(targetNode.client.cluster.join).bind(
            targetNode.client.cluster,
        );
        await joinAsync({
            node: { id: this.localNodeId, addr: this.localNodeAddr },
        });
    }

    /**
     * 向指定节点发送离开请求
     */
    async sendLeaveRequest(targetNode: NodeInfoWithClient): Promise<void> {
        const leaveAsync = promisify<LeaveRequest, LeaveResponse>(targetNode.client.cluster.leave).bind(
            targetNode.client.cluster,
        );
        await leaveAsync({
            node: { id: this.localNodeId, addr: this.localNodeAddr },
        });
        targetNode.client.cluster.close();
        if (targetNode.client.engine) {
            targetNode.client.engine.close();
        }
    }
}

/**
 * 积极模式数据管理器
 * 负责数据存储和同步
 */
class EagerDataManager {
    private data: Set<string>;
    private engine: EagerEngine;

    constructor(engine: EagerEngine) {
        this.engine = engine;
        this.data = new Set();
    }

    /**
     * 获取所有已存储的数据
     * @returns {Array<string>} - 数据数组
     */
    getData(): string[] {
        return Array.from(this.data);
    }

    /**
     * 添加数据到本地存储
     * @param {string} item - 要添加的数据
     * @returns {string | null} - 返回格式化后的数据，如果添加失败返回 null
     */
    addData(item: string): string | null {
        const formattedItem = this.engine.input(item);
        if (formattedItem !== null) {
            this.data.add(formattedItem);
            return formattedItem;
        }
        return null;
    }

    /**
     * 批量添加数据
     * @param {string[]} items - 要添加的数据数组
     */
    addDataBatch(items: string[]): void {
        for (const item of items) {
            this.addData(item);
        }
    }

    /**
     * 添加数据并记录日志
     * @param {string} item - 要添加的数据
     * @param {string} logPrefix - 日志前缀
     * @returns {string | null} - 返回格式化后的数据，如果添加失败返回 null
     */
    addDataWithLog(item: string, logPrefix: string): string | null {
        const formattedItem = this.addData(item);
        if (formattedItem !== null) {
            console.log(`${logPrefix}: ${formattedItem}`);
        }
        return formattedItem;
    }

    /**
     * 获取引擎
     */
    getEngine(): EagerEngine {
        return this.engine;
    }

    /**
     * 推送数据到指定节点
     * @param {NodeInfoWithClient} node - 目标节点
     * @param {string[]} data - 要推送的数据
     */
    async pushDataToNode(node: NodeInfoWithClient, data: string[]): Promise<void> {
        const pushDataAsync = promisify<PushDataRequest, PushDataResponse>(node.client.engine.pushData).bind(
            node.client.engine,
        );
        await pushDataAsync({ data });
    }

    /**
     * 从指定节点拉取数据
     * @param {NodeInfoWithClient} node - 源节点
     * @returns {Promise<string[]>} 拉取的数据
     */
    async pullDataFromNode(node: NodeInfoWithClient): Promise<string[]> {
        const pullAsync = promisify<PullDataRequest, PullDataResponse>(node.client.engine.pullData).bind(
            node.client.engine,
        );
        const response = await pullAsync({});
        return response.data || [];
    }

    /**
     * 同步数据到所有其他节点
     * @param {Map<string, NodeInfoWithClient>} nodes - 所有节点
     * @param {string} localNodeId - 本地节点ID
     * @param {string[]} data - 要同步的数据
     */
    async syncDataToNodes(nodes: Map<string, NodeInfoWithClient>, localNodeId: string, data: string[]): Promise<void> {
        if (data.length === 0) {
            return;
        }

        for (const [id, node] of nodes.entries()) {
            if (id !== localNodeId) {
                try {
                    await this.pushDataToNode(node, data);
                } catch (error) {
                    console.error(`Failed to sync data to node ${id}:`, error);
                }
            }
        }
    }
}

/**
 * 积极模式引擎调度器
 * 负责搜索任务的调度和执行
 */
class EagerEngineScheduler {
    private dataManager: EagerDataManager;
    private isRunning: boolean;

    constructor(dataManager: EagerDataManager) {
        this.dataManager = dataManager;
        this.isRunning = false;
    }

    /**
     * 启动定时循环执行搜索任务
     * 每秒执行一次搜索，并将新发现的数据推送到其他节点
     * @param {Map<string, NodeInfoWithClient>} nodes - 集群节点
     * @param {string} localNodeId - 本地节点ID
     */
    start(nodes: Map<string, NodeInfoWithClient>, localNodeId: string): void {
        if (this.isRunning) {
            return;
        }
        this.isRunning = true;

        const loop = async (): Promise<void> => {
            if (!this.isRunning) {
                return;
            }

            const begin = Date.now();
            const newData: string[] = [];

            // 执行搜索并收集新数据
            this.dataManager.getEngine().output((result: string) => {
                const formattedResult = this.dataManager.addDataWithLog(result, "Found data");
                if (formattedResult !== null) {
                    newData.push(formattedResult);
                }
            });

            // 同步新数据到其他节点
            if (newData.length > 0) {
                await this.dataManager.syncDataToNodes(nodes, localNodeId, newData);
            }

            const end = Date.now();
            const cost = end - begin;
            const waiting = Math.max(1000 - cost, 0);

            setTimeout(loop, waiting);
        };

        setTimeout(loop, 0);
    }

    /**
     * 停止搜索调度
     */
    stop(): void {
        this.isRunning = false;
    }

    /**
     * 检查调度器是否正在运行
     */
    isSchedulerRunning(): boolean {
        return this.isRunning;
    }
}

/**
 * 积极模式网络处理器
 * 负责网络通信的抽象和gRPC服务器管理
 */
class EagerNetworkHandler {
    private server: grpc.Server;
    private clusterManager: ClusterManager;
    private dataManager: EagerDataManager;

    constructor(clusterManager: ClusterManager, dataManager: EagerDataManager) {
        this.server = new grpc.Server();
        this.clusterManager = clusterManager;
        this.dataManager = dataManager;
    }

    /**
     * 注册所有服务
     */
    registerServices(): void {
        // 注册集群管理服务
        this.server.addService(ClusterService, this.clusterManager.getClusterServiceHandlers());

        // 注册引擎服务
        this.server.addService(EngineService, this.getEngineServiceHandlers());
    }

    /**
     * 获取引擎服务处理器
     */
    private getEngineServiceHandlers(): EngineServer {
        return {
            /**
             * 处理元数据查询请求
             * 返回引擎的元数据信息
             */
            metaData: async (
                _call: grpc.ServerUnaryCall<MetaDataRequest, MetaDataResponse>,
                callback: grpc.sendUnaryData<MetaDataResponse>,
            ) => {
                const meta = this.dataManager.getEngine().meta();
                callback(null, {
                    metadata: {
                        id: this.clusterManager.getLocalNodeId(),
                        kind: EngineKind.EAGER,
                        input: meta.input,
                        output: meta.output,
                    },
                });
            },
            /**
             * 处理数据推送请求
             * 接收其他节点推送的数据并添加到本地搜索引擎
             */
            pushData: async (
                call: grpc.ServerUnaryCall<PushDataRequest, PushDataResponse>,
                callback: grpc.sendUnaryData<PushDataResponse>,
            ) => {
                const data = call.request.data;
                if (data) {
                    for (const item of data) {
                        this.dataManager.addDataWithLog(item, "Received data");
                    }
                }
                callback(null, {});
            },
            /**
             * 处理数据拉取请求
             * 返回本地存储的所有数据
             */
            pullData: async (
                _call: grpc.ServerUnaryCall<PullDataRequest, PullDataResponse>,
                callback: grpc.sendUnaryData<PullDataResponse>,
            ) => {
                callback(null, { data: this.dataManager.getData() });
            },
        } as EngineServer;
    }

    /**
     * 启动服务器监听
     * @param {string} addr - 监听地址
     * @returns {Promise<number>} 返回实际绑定的端口
     */
    async listen(addr: string): Promise<number> {
        const bindAsync = promisify<string, grpc.ServerCredentials, number>(this.server.bindAsync).bind(this.server);
        const port = await bindAsync(addr, grpc.ServerCredentials.createInsecure());
        return port;
    }

    /**
     * 获取gRPC服务器实例
     */
    getServer(): grpc.Server {
        return this.server;
    }

    /**
     * 创建引擎客户端
     * @param {string} addr - 目标地址
     * @returns {EngineClient} 引擎客户端实例
     */
    createEngineClient(addr: string): EngineClient {
        return new EngineClient(addr, grpc.credentials.createInsecure());
    }
}

/**
 * 搜索引擎类
 * 扩展 ATSDS 的 Search 类，提供字符串形式的规则处理
 * 实现 EagerEngine 接口
 */
class Search extends Search_ implements EagerEngine {
    /**
     * 构造函数
     * @param {number} limit_size - Size of the buffer for storing the final objects (rules/facts) in the knowledge base (default: 1000)
     * @param {number} buffer_size - Size of the buffer for internal operations like conversions and transformations (default: 10000)
     */
    constructor(limit_size: number = 1000, buffer_size: number = 10000) {
        super(limit_size, buffer_size);
    }

    /**
     * 输入规则到搜索引擎
     * @param {string} rule - 规则字符串
     */
    input(rule: string): string | null {
        const parsedRule = parse(rule);
        if (super.add(parsedRule)) {
            const unparsedRule = unparse(parsedRule);
            return unparsedRule;
        }
        return null;
    }

    /**
     * 执行搜索并输出结果
     * @param {function} callback - 处理搜索结果的回调函数
     * @returns {*} - 搜索执行结果
     */
    output(callback: (result: string) => void): number {
        return super.execute((candidate: Rule) => {
            const result = unparse(candidate.toString());
            callback(result);
            return false;
        });
    }

    /**
     * 获取搜索引擎的元数据
     * @returns {Object} - 包含 input 和 output 字符串数组的对象
     */
    meta(): { input: string[]; output: string[] } {
        return {
            input: ["`x"],
            output: ["`x"],
        };
    }
}

/**
 * 积极节点类
 * 管理分布式搜索引擎集群中的单个节点
 * 使用组件化架构：ClusterManager、EagerDataManager、EagerEngineScheduler、EagerNetworkHandler
 */
class EagerNode {
    private clusterManager: ClusterManager;
    private dataManager: EagerDataManager;
    private engineScheduler: EagerEngineScheduler;
    private networkHandler: EagerNetworkHandler;

    /**
     * 构造函数
     * @param {EagerEngine} engine - 实现 EagerEngine 接口的引擎实例
     * @param {string} addr - 节点绑定地址
     * @param {string} id - 节点唯一标识符，默认随机生成
     */
    constructor(engine: EagerEngine, addr: string, id: string = randomUUID()) {
        this.clusterManager = new ClusterManager(id, addr);
        this.dataManager = new EagerDataManager(engine);
        this.engineScheduler = new EagerEngineScheduler(this.dataManager);
        this.networkHandler = new EagerNetworkHandler(this.clusterManager, this.dataManager);
    }
    /**
     * 设置IO读取循环与信号处理器
     * 持续读取标准输入，每收到一行时将其输入到搜索引擎并推送到所有其他节点, 并设置信号处理器
     */
    private setupIoLoop(): void {
        const rl = createInterface({
            input: process.stdin,
            output: process.stdout,
            terminal: false,
        });
        rl.on("line", async (line: string) => {
            const trimmedLine = line.trim();
            if (trimmedLine.length === 0) {
                return;
            }
            const formattedLine = this.dataManager.addDataWithLog(trimmedLine, "Received input");
            if (formattedLine !== null) {
                await this.dataManager.syncDataToNodes(
                    this.clusterManager.getNodes(),
                    this.clusterManager.getLocalNodeId(),
                    [formattedLine],
                );
            }
        });
        process.on("SIGINT", async () => {
            rl.close();
            await this.leave();
            process.exit(0);
        });
        process.on("SIGUSR1", () => {
            console.log("=== All Nodes Information ===");
            for (const [id, nodeInfo] of this.clusterManager.getNodes().entries()) {
                console.log(`Node ID: ${id}`);
                console.log(`  Address: ${nodeInfo.addr}`);
            }
            console.log(`Total nodes: ${this.clusterManager.getNodes().size}`);
            console.log("=============================");
        });
        process.on("SIGUSR2", () => {
            console.log("=== All Data Managed by This Node ===");
            const data = this.dataManager.getData();
            for (const index in data) {
                console.log(`[${index}] ${data[index]}`);
            }
            console.log(`Total data items: ${data.length}`);
            console.log("=====================================");
        });
    }
    /**
     * 创建节点信息对象
     * @param {string} id - 节点 ID
     * @param {string} addr - 节点地址
     * @returns {Object} 包含节点信息和 gRPC 客户端的对象
     */
    private nodeInfo(id: string, addr: string): NodeInfoWithClient {
        return {
            id: id,
            addr: addr,
            client: {
                cluster: new ClusterClient(addr, grpc.credentials.createInsecure()),
                engine: this.networkHandler.createEngineClient(addr),
            },
        };
    }
    /**
     * 启动节点监听服务
     * 创建 gRPC 服务器并注册集群管理和引擎服务
     * @returns {EagerNode} 返回当前节点实例
     */
    async listen(): Promise<EagerNode> {
        this.networkHandler.registerServices();

        const port = await this.networkHandler.listen(this.clusterManager.getLocalNodeAddr());
        const addr = `${this.clusterManager.getLocalNodeAddr().split(":")[0]}:${port}`;
        this.clusterManager.setLocalNodeAddr(addr);

        this.engineScheduler.start(this.clusterManager.getNodes(), this.clusterManager.getLocalNodeId());
        this.setupIoLoop();

        this.clusterManager.addNode(this.clusterManager.getLocalNodeId(), addr, {
            cluster: null as unknown as ClusterClient,
            engine: null as unknown as EngineClient,
        });

        console.log(`Listening on: ${addr} (Node ID: ${this.clusterManager.getLocalNodeId()})`);
        return this;
    }
    /**
     * 获取节点列表
     * @param {string} addr - 要查询的节点地址
     * @returns {Promise<Node[]>} 节点列表
     */
    async list(addr: string): Promise<Node[]> {
        return this.clusterManager.listRemoteNodes(addr);
    }
    /**
     * 加入现有集群
     * @param {Node[]} nodes - 要加入的节点列表
     */
    async join(nodes: Node[]): Promise<void> {
        const localData = this.dataManager.getData();
        for (const node of nodes) {
            if (!this.clusterManager.hasNode(node.id)) {
                const nodeInfo = this.nodeInfo(node.id, node.addr);
                this.clusterManager.addNode(node.id, node.addr, nodeInfo.client);

                await this.clusterManager.sendJoinRequest(nodeInfo);

                if (localData.length > 0) {
                    await this.dataManager.pushDataToNode(nodeInfo, localData);
                }

                const remoteData = await this.dataManager.pullDataFromNode(nodeInfo);
                if (remoteData) {
                    for (const item of remoteData) {
                        this.dataManager.addDataWithLog(item, "Receiving data");
                    }
                }
                console.log(`Joining node ${node.id} at ${node.addr}`);
            }
        }
    }
    /**
     * 离开集群
     * 向所有其他节点发送离开通知
     */
    async leave(): Promise<void> {
        for (const [id, node] of this.clusterManager.getNodes().entries()) {
            if (id !== this.clusterManager.getLocalNodeId()) {
                await this.clusterManager.sendLeaveRequest(node);
                console.log(`Leaving node ${node.id} at ${node.addr}`);
            }
        }
    }
}

/**
 * 检查字符串是否为整数
 * @param {string} str - 待检查的字符串
 * @returns {boolean} 如果是整数返回true，否则返回false
 */
function isIntegerString(str: string): boolean {
    if (typeof str !== "string") return false;
    return /^\d+$/.test(str);
}

/**
 * 为端口号添加IP地址前缀
 * @param {string} addrOrPort - 完整地址或端口号
 * @param {string} ip - 要添加的IP地址
 * @returns {string} 格式化后的地址
 */
function addAddressPrefixForPort(addrOrPort: string, ip: string): string {
    if (isIntegerString(addrOrPort)) {
        return `${ip}:${addrOrPort}`;
    }
    return addrOrPort;
}

// ============================================================
// 主程序入口
// ============================================================

/**
 * 命令行参数检查
 * 用法: node dist/main.mjs <bind_addr> [<join_addr>]
 *   bind_addr: 本节点绑定的端口号
 *   join_addr: (可选) 要加入的集群中某个节点的端口号
 */
if (process.argv.length < 3 || process.argv.length > 4) {
    console.error("Usage: main <bind_addr> [<join_addr>]");
    process.exit(1);
}

/**
 * 场景一: 加入现有集群
 * 启动节点并加入指定地址的集群
 */
if (process.argv.length === 4) {
    const listenAddr = addAddressPrefixForPort(process.argv[2], "0.0.0.0");
    const joinAddr = addAddressPrefixForPort(process.argv[3], "127.0.0.1");
    console.log(`Starting node at ${listenAddr} and joining ${joinAddr}`);
    const engine = new Search();
    const node = new EagerNode(engine, listenAddr);
    await node.listen();
    await node.join(await node.list(joinAddr));
}

/**
 * 场景二: 创建新集群
 * 启动第一个节点，作为集群的初始节点
 */
if (process.argv.length === 3) {
    const listenAddr = addAddressPrefixForPort(process.argv[2], "0.0.0.0");
    console.log(`Starting node at ${listenAddr}`);
    const engine = new Search();
    const node = new EagerNode(engine, listenAddr);
    await node.listen();
}
