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
 * 搜索引擎类
 * 扩展 ATSDS 的 Search 类，提供字符串形式的规则处理和数据管理
 */
class Search extends Search_ {
    private data: Set<string>;

    /**
     * 构造函数
     * @param {number} limit_size - Size of the buffer for storing the final objects (rules/facts) in the knowledge base (default: 1000)
     * @param {number} buffer_size - Size of the buffer for internal operations like conversions and transformations (default: 10000)
     */
    constructor(limit_size: number = 1000, buffer_size: number = 10000) {
        super(limit_size, buffer_size);
        this.data = new Set();
    }

    /**
     * 输入规则到搜索引擎
     * @param {string} rule - 规则字符串
     */
    input(rule: string): string | null {
        const parsedRule = parse(rule);
        if (super.add(parsedRule)) {
            const unparsedRule = unparse(parsedRule);
            this.data.add(unparsedRule);
            return unparsedRule;
        }
        return null;
    }

    /**
     * 执行搜索并输出结果
     * @param {function} callback - 处理搜索结果的回调函数
     * @returns {*} - 搜索执行结果
     */
    output(callback: (result: string) => boolean): number {
        return super.execute((candidate: Rule) => {
            const result = unparse(candidate.toString());
            this.data.add(result);
            return callback(result);
        });
    }

    /**
     * 获取所有已存储的数据
     * @returns {Array<string>} - 数据数组
     */
    getData(): string[] {
        return Array.from(this.data);
    }
}

/**
 * 集群节点类
 * 管理分布式搜索引擎集群中的单个节点
 */
class ClusterNode {
    id: string;
    addr: string;
    engine: Search;
    server: grpc.Server;
    nodes: Map<string, NodeInfoWithClient>;

    /**
     * 构造函数
     * @param {string} addr - 节点绑定地址
     * @param {string} id - 节点唯一标识符，默认随机生成
     * @param {number} limit_size - 搜索引擎的限制大小参数，默认 1000
     * @param {number} buffer_size - 搜索引擎的缓冲区大小参数，默认 10000
     */
    constructor(addr: string, id: string = randomUUID(), limit_size: number = 1000, buffer_size: number = 10000) {
        this.id = id;
        this.addr = addr;
        this.engine = new Search(limit_size, buffer_size);
        this.server = new grpc.Server();
        this.nodes = new Map();
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
                data.push(result);
                return false;
            });
            if (data.length > 0) {
                for (const result of data) {
                    console.log(`Found data: ${result}`);
                }
                for (const id of this.nodes.keys()) {
                    if (id !== this.id) {
                        const node = this.nodes.get(id)!;
                        const pushDataAsync = promisify<PushDataRequest, PushDataResponse>(
                            node.client.engine.pushData,
                        ).bind(node.client.engine);
                        await pushDataAsync({ data });
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
            console.log(`Received input: ${formattedLine}`);
            for (const id of this.nodes.keys()) {
                if (id !== this.id) {
                    const node = this.nodes.get(id)!;
                    const pushDataAsync = promisify<PushDataRequest, PushDataResponse>(
                        node.client.engine.pushData,
                    ).bind(node.client.engine);
                    await pushDataAsync({ data: [formattedLine] });
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
            for (const [id, nodeInfo] of this.nodes.entries()) {
                console.log(`Node ID: ${id}`);
                console.log(`  Address: ${nodeInfo.addr}`);
            }
            console.log(`Total nodes: ${this.nodes.size}`);
            console.log("=============================");
        });
        process.on("SIGUSR2", () => {
            console.log("=== All Data Managed by Search ===");
            const data = this.engine.getData();
            for (const index in data) {
                console.log(`[${index}] ${data[index]}`);
            }
            console.log(`Total data items: ${data.length}`);
            console.log("==================================");
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
     * @returns {ClusterNode} 返回当前节点实例
     */
    async listen(): Promise<ClusterNode> {
        this.server.addService(ClusterService, {
            /**
             * 处理节点加入请求
             * 将请求节点加入本地节点列表
             */
            join: async (
                call: grpc.ServerUnaryCall<JoinRequest, JoinResponse>,
                callback: grpc.sendUnaryData<JoinResponse>,
            ) => {
                const node = call.request.node!;
                const { id, addr } = node;
                if (!this.nodes.has(id)) {
                    this.nodes.set(id, this.nodeInfo(id, addr));
                    console.log(`Joined node: ${id} at ${addr}`);
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
                const node = call.request.node!;
                const { id, addr } = node;
                if (this.nodes.has(id)) {
                    this.nodes.delete(id);
                    console.log(`Left node: ${id} at ${addr}`);
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
                callback(null, {
                    metadata: {
                        id: this.id,
                        kind: EngineKind.EAGER,
                        input: ["`x"],
                        output: ["`x"],
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
                const data = call.request.data!;
                for (const item of data) {
                    this.engine.input(item);
                    console.log(`Received data: ${item}`);
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
                callback(null, { data: this.engine.getData() });
            },
        } as EngineServer);
        const bindAsync = promisify<string, grpc.ServerCredentials, number>(this.server.bindAsync).bind(this.server);
        const port = await bindAsync(this.addr, grpc.ServerCredentials.createInsecure());
        this.addr = `${this.addr.split(":")[0]}:${port}`;
        this.setupSearchLoop();
        this.setupIoLoop();
        this.nodes.set(this.id, {
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
        console.log(`Listed nodes: ${response.nodes.length} nodes from ${addr}`);
        return response.nodes;
    }
    /**
     * 加入现有集群
     * @param {Node[]} nodes - 要加入的节点列表
     */
    async join(nodes: Node[]): Promise<void> {
        const localData = this.engine.getData();
        for (const node of nodes) {
            if (!this.nodes.has(node.id)) {
                const nodeInfo = this.nodeInfo(node.id, node.addr);
                this.nodes.set(node.id, nodeInfo);
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
                        console.log(`Receiving data: ${item}`);
                        this.engine.input(item);
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
        for (const id of this.nodes.keys()) {
            if (id !== this.id) {
                const node = this.nodes.get(id)!;
                const leaveAsync = promisify<LeaveRequest, LeaveResponse>(node.client.cluster.leave).bind(
                    node.client.cluster,
                );
                await leaveAsync({ node: { id: this.id, addr: this.addr } });
                console.log(`Leaving node ${node.id} at ${node.addr}`);
                node.client.cluster.close();
                node.client.engine.close();
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
    const node = new ClusterNode(listenAddr);
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
    const node = new ClusterNode(listenAddr);
    await node.listen();
}
