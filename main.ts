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
 * 集群管理器类
 * 负责管理集群中的节点和节点生命周期事件
 */
class ClusterManager {
    private nodes: Map<string, NodeInfoWithClient>;
    private onNodeJoinedHooks: Array<(node: NodeInfoWithClient) => void | Promise<void>>;
    private onNodeLeftHooks: Array<(node: NodeInfoWithClient) => void | Promise<void>>;

    /**
     * 构造函数
     */
    constructor() {
        this.nodes = new Map();
        this.onNodeJoinedHooks = [];
        this.onNodeLeftHooks = [];
    }

    /**
     * 注册节点加入时的钩子函数
     * @param {function} hook - 节点加入时执行的回调函数
     */
    onNodeJoined(hook: (node: NodeInfoWithClient) => void | Promise<void>): void {
        this.onNodeJoinedHooks.push(hook);
    }

    /**
     * 注册节点离开时的钩子函数
     * @param {function} hook - 节点离开时执行的回调函数
     */
    onNodeLeft(hook: (node: NodeInfoWithClient) => void | Promise<void>): void {
        this.onNodeLeftHooks.push(hook);
    }

    /**
     * 添加节点到集群
     * @param {NodeInfoWithClient} node - 要添加的节点信息
     */
    async addNode(node: NodeInfoWithClient): Promise<void> {
        if (!this.nodes.has(node.id)) {
            this.nodes.set(node.id, node);
            for (const hook of this.onNodeJoinedHooks) {
                await hook(node);
            }
        }
    }

    /**
     * 从集群中移除节点
     * @param {string} id - 要移除的节点ID
     */
    async removeNode(id: string): Promise<void> {
        const node = this.nodes.get(id);
        if (node) {
            this.nodes.delete(id);
            for (const hook of this.onNodeLeftHooks) {
                await hook(node);
            }
        }
    }

    /**
     * 获取节点信息
     * @param {string} id - 节点ID
     * @returns {NodeInfoWithClient | undefined} 节点信息
     */
    getNode(id: string): NodeInfoWithClient | undefined {
        return this.nodes.get(id);
    }

    /**
     * 获取所有节点ID
     * @returns {IterableIterator<string>} 节点ID迭代器
     */
    getNodeIds(): IterableIterator<string> {
        return this.nodes.keys();
    }

    /**
     * 获取所有节点
     * @returns {Node[]} 节点列表
     */
    getAllNodes(): Node[] {
        return Array.from(this.nodes.values()).map((n) => ({
            id: n.id,
            addr: n.addr,
        }));
    }

    /**
     * 检查节点是否存在
     * @param {string} id - 节点ID
     * @returns {boolean} 节点是否存在
     */
    hasNode(id: string): boolean {
        return this.nodes.has(id);
    }

    /**
     * 获取节点数量
     * @returns {number} 节点数量
     */
    getNodeCount(): number {
        return this.nodes.size;
    }
}

/**
 * 积极节点类
 * 管理分布式搜索引擎集群中的单个节点
 */
class EagerNode {
    private engine: EagerEngine;
    private addr: string;
    private id: string;
    private server: grpc.Server;
    private clusterManager: ClusterManager;
    private data: Set<string>;

    /**
     * 构造函数
     * @param {EagerEngine} engine - 实现 EagerEngine 接口的引擎实例
     * @param {string} addr - 节点绑定地址
     * @param {string} id - 节点唯一标识符，默认随机生成
     */
    constructor(engine: EagerEngine, addr: string, id: string = randomUUID()) {
        this.engine = engine;
        this.addr = addr;
        this.id = id;
        this.server = new grpc.Server();
        this.clusterManager = new ClusterManager();
        this.data = new Set();
        this.setupClusterHooks();
    }

    /**
     * 设置集群管理器的钩子函数
     * 配置节点加入和离开时的行为
     */
    private setupClusterHooks(): void {
        this.clusterManager.onNodeJoined((node) => {
            console.log(`Joined node: ${node.id} at ${node.addr}`);
        });

        this.clusterManager.onNodeLeft((node) => {
            console.log(`Left node: ${node.id} at ${node.addr}`);
        });
    }
    /**
     * 获取集群节点所有已存储的数据
     * @returns {Array<string>} - 数据数组
     */
    private getData(): string[] {
        return Array.from(this.data);
    }
    /**
     * 设置定时循环执行搜索任务
     * 每秒执行一次搜索，并将新发现的数据推送到其他节点
     */
    private setupSearchLoop(): void {
        const loop = async (): Promise<void> => {
            const begin = Date.now();
            const data: string[] = [];
            this.engine.output((result: string) => {
                this.data.add(result);
                data.push(result);
                console.log(`Found data: ${result}`);
            });
            if (data.length > 0) {
                for (const id of this.clusterManager.getNodeIds()) {
                    if (id !== this.id) {
                        const node = this.clusterManager.getNode(id);
                        if (node) {
                            const pushDataAsync = promisify<PushDataRequest, PushDataResponse>(
                                node.client.engine.pushData,
                            ).bind(node.client.engine);
                            await pushDataAsync({ data });
                        }
                    }
                }
            }
            const end = Date.now();
            const cost = end - begin;
            const waiting = Math.max(1000 - cost, 0);
            setTimeout(loop, waiting);
        };
        setTimeout(loop, 0);
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
            const formattedLine = this.engine.input(trimmedLine);
            if (formattedLine === null) {
                return;
            }
            this.data.add(formattedLine);
            console.log(`Received input: ${formattedLine}`);
            for (const id of this.clusterManager.getNodeIds()) {
                if (id !== this.id) {
                    const node = this.clusterManager.getNode(id);
                    if (node) {
                        const pushDataAsync = promisify<PushDataRequest, PushDataResponse>(
                            node.client.engine.pushData,
                        ).bind(node.client.engine);
                        await pushDataAsync({ data: [formattedLine] });
                    }
                }
            }
        });
        process.on("SIGINT", async () => {
            rl.close();
            await this.leave();
            process.exit(0);
        });
        process.on("SIGUSR1", () => {
            console.log("=== All Nodes Information ===");
            const nodes = this.clusterManager.getAllNodes();
            for (const node of nodes) {
                console.log(`Node ID: ${node.id}`);
                console.log(`  Address: ${node.addr}`);
            }
            console.log(`Total nodes: ${this.clusterManager.getNodeCount()}`);
            console.log("=============================");
        });
        process.on("SIGUSR2", () => {
            console.log("=== All Data Managed by This Node ===");
            const data = this.getData();
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
                engine: new EngineClient(addr, grpc.credentials.createInsecure()),
            },
        };
    }
    /**
     * 启动节点监听服务
     * 创建 gRPC 服务器并注册集群管理和引擎服务
     * @returns {EagerNode} 返回当前节点实例
     */
    async listen(): Promise<EagerNode> {
        this.server.addService(ClusterService, {
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
                    await this.clusterManager.addNode(this.nodeInfo(id, addr));
                }
                callback(null, {});
            },
            /**
             * 处理节点离开请求
             * 将请求节点从本地节点列表中移除
             */
            leave: async (
                call: grpc.ServerUnaryCall<LeaveRequest, LeaveResponse>,
                callback: grpc.sendUnaryData<LeaveResponse>,
            ) => {
                const node = call.request.node;
                if (node) {
                    await this.clusterManager.removeNode(node.id);
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
                callback(null, { nodes: this.clusterManager.getAllNodes() });
            },
        } as ClusterServer);
        this.server.addService(EngineService, {
            /**
             * 处理元数据查询请求
             * 返回引擎的元数据信息
             */
            metaData: async (
                _call: grpc.ServerUnaryCall<MetaDataRequest, MetaDataResponse>,
                callback: grpc.sendUnaryData<MetaDataResponse>,
            ) => {
                const meta = this.engine.meta();
                callback(null, {
                    metadata: {
                        id: this.id,
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
                        const formattedItem = this.engine.input(item);
                        if (formattedItem !== null) {
                            this.data.add(formattedItem);
                            console.log(`Received data: ${item}`);
                        }
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
                callback(null, { data: this.getData() });
            },
        } as EngineServer);
        const bindAsync = promisify<string, grpc.ServerCredentials, number>(this.server.bindAsync).bind(this.server);
        const port = await bindAsync(this.addr, grpc.ServerCredentials.createInsecure());
        this.addr = `${this.addr.split(":")[0]}:${port}`;
        this.setupSearchLoop();
        this.setupIoLoop();
        await this.clusterManager.addNode({
            id: this.id,
            addr: this.addr,
            client: {
                cluster: null as unknown as ClusterClient,
                engine: null as unknown as EngineClient,
            },
        });
        console.log(`Listening on: ${this.addr} (Node ID: ${this.id})`);
        return this;
    }
    /**
     * 获取节点列表
     * @param {string} addr - 要查询的节点地址
     * @returns {Promise<Node[]>} 节点列表
     */
    async list(addr: string): Promise<Node[]> {
        const client = new ClusterClient(addr, grpc.credentials.createInsecure());
        const listAsync = promisify<ListRequest, ListResponse>(client.list).bind(client);
        const response = await listAsync({});
        client.close();
        console.log(`Listing nodes: ${response.nodes.length} nodes from ${addr}`);
        return response.nodes;
    }
    /**
     * 加入现有集群
     * @param {Node[]} nodes - 要加入的节点列表
     */
    async join(nodes: Node[]): Promise<void> {
        const localData = this.getData();
        for (const node of nodes) {
            if (!this.clusterManager.hasNode(node.id)) {
                const nodeInfo = this.nodeInfo(node.id, node.addr);
                await this.clusterManager.addNode(nodeInfo);
                const joinAsync = promisify<JoinRequest, JoinResponse>(nodeInfo.client.cluster.join).bind(
                    nodeInfo.client.cluster,
                );
                await joinAsync({ node: { id: this.id, addr: this.addr } });
                if (localData.length > 0) {
                    const pushAsync = promisify<PushDataRequest, PushDataResponse>(
                        nodeInfo.client.engine.pushData,
                    ).bind(nodeInfo.client.engine);
                    await pushAsync({ data: localData });
                }
                const pullAsync = promisify<PullDataRequest, PullDataResponse>(nodeInfo.client.engine.pullData).bind(
                    nodeInfo.client.engine,
                );
                const dataResponse = await pullAsync({});
                if (dataResponse.data) {
                    for (const item of dataResponse.data) {
                        const formattedItem = this.engine.input(item);
                        if (formattedItem !== null) {
                            this.data.add(formattedItem);
                            console.log(`Receiving data: ${item}`);
                        }
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
        for (const id of this.clusterManager.getNodeIds()) {
            if (id !== this.id) {
                const node = this.clusterManager.getNode(id);
                if (node) {
                    const leaveAsync = promisify<LeaveRequest, LeaveResponse>(node.client.cluster.leave).bind(
                        node.client.cluster,
                    );
                    await leaveAsync({ node: { id: this.id, addr: this.addr } });
                    node.client.cluster.close();
                    node.client.engine.close();
                    console.log(`Leaving node ${node.id} at ${node.addr}`);
                }
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
