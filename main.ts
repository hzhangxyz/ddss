// 导入工具模块
import {
    promisify, // 将回调函数转换为 Promise 的工具
} from "node:util";
import {
    randomUUID, // 生成随机 UUID
} from "node:crypto";
import {
    createInterface, // 创建readline接口用于读取stdin
} from "node:readline";
// 导入 gRPC 相关模块
import * as grpc from "@grpc/grpc-js"; // gRPC JavaScript 实现
// 导入 ATSDS（搜索引擎）相关模块
import {
    Search as Search_, // ATSDS 搜索引擎基类
    type Rule,
} from "atsds";
import {
    parse, // 解析规则字符串为内部表示
    unparse, // 将内部表示转换为规则字符串
} from "atsds-bnf";
// 导入生成的 Protocol Buffers 类型和服务
import {
    type JoinRequest,
    type JoinResponse,
    type LeaveRequest,
    type LeaveResponse,
    type MetaDataRequest,
    type MetaDataResponse,
    type PushDataRequest,
    type PushDataResponse,
    type PullDataRequest,
    type PullDataResponse,
    EngineKind,
    ClusterClient,
    ClusterServer,
    ClusterService,
    EngineClient,
    EngineServer,
    EngineService,
} from "./ddss.js";

interface NodeClient {
    cluster: ClusterClient;
    engine: EngineClient;
}

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
        this.data = new Set(); // 存储已添加的数据（使用 Set 避免重复）
    }

    /**
     * 输入规则到搜索引擎
     * @param {string} rule - 规则字符串
     */
    input(rule: string): string | null {
        const parsedRule = parse(rule); // 解析规则字符串
        if (super.add(parsedRule)) {
            const unparsedRule = unparse(parsedRule);
            this.data.add(unparsedRule); // 添加成功则保存到数据集合
            return unparsedRule;
        }
        return null;
    }

    /**
     * 执行搜索并输出结果（迭代器形式）
     * 每次yield一个结果时，execute会暂停；下次调用output()会继续execute
     * @returns {Generator<string>} - 生成器，每次yield一个搜索结果
     */
    *output(): Generator<string> {
        // 执行搜索，每次返回一个结果后立即停止（返回true）
        // execute()应该在下次调用时继续之前的状态
        let hasMore = true;
        while (hasMore) {
            let resultFound = false;
            let currentResult = "";

            super.execute((candidate: Rule) => {
                const result = unparse(candidate.toString());
                this.data.add(result);
                currentResult = result;
                resultFound = true;
                // 返回true停止execute，这样每次只处理一个结果
                return true;
            });

            if (resultFound) {
                yield currentResult;
            } else {
                hasMore = false;
            }
        }
    }

    /**
     * 获取所有已存储的数据
     * @returns {Array<string>} - 数据数组
     */
    getData(): string[] {
        return [...this.data];
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
        this.id = id; // 节点 ID
        this.addr = addr; // 节点地址
        this.engine = new Search(limit_size, buffer_size); // 创建搜索引擎实例
        this.server = new grpc.Server();
        this.nodes = new Map(); // 存储集群中所有节点信息
    }
    /**
     * 设置定时循环执行搜索任务
     * 每秒执行一次搜索，并将新发现的数据推送到其他节点
     */
    private setupSearchLoop(): void {
        const loop = async (): Promise<void> => {
            const begin = Date.now();
            const data: string[] = []; // 存储本轮搜索发现的新数据
            // 执行搜索引擎，处理搜索结果
            for (const result of this.engine.output()) {
                data.push(result); // 添加到待推送列表（结果已由 engine.output 自动保存）
                console.log(`Found data: ${result}`);
            }
            // 如果发现新数据，推送到所有其他节点
            if (data.length > 0) {
                for (const id of this.nodes.keys()) {
                    if (id !== this.id) {
                        // 排除自己
                        const node = this.nodes.get(id)!;
                        const pushDataAsync = promisify<PushDataRequest, PushDataResponse>(
                            node.client.engine.pushData,
                        ).bind(node.client.engine);
                        await pushDataAsync({
                            data,
                        });
                    }
                }
            }
            const end = Date.now();
            const cost = end - begin; // 计算本轮执行耗时
            const waiting = Math.max(1000 - cost, 0); // 计算等待时间，确保每秒执行一次
            setTimeout(loop, waiting); // 安排下次执行
        };
        setTimeout(loop, 0);
    }
    /**
     * 设置stdin读取循环
     * 持续读取标准输入，每收到一行时将其输入到搜索引擎并推送到所有其他节点
     */
    private setupStdinLoop(): ReturnType<typeof createInterface> {
        const rl = createInterface({
            input: process.stdin,
            output: process.stdout,
            terminal: false,
        });
        rl.on("line", async (line: string) => {
            // 去除空白字符
            const trimmedLine = line.trim();
            if (trimmedLine.length === 0) {
                return; // 跳过空行
            }
            // 将输入喂给搜索引擎
            const formattedLine = this.engine.input(trimmedLine);
            if (formattedLine === null) {
                return; // 已存在则跳过
            }
            console.log(`Received input from stdin: ${formattedLine}`);
            // 推送到所有其他节点（排除自己）
            for (const id of this.nodes.keys()) {
                if (id !== this.id) {
                    // 排除自己
                    const node = this.nodes.get(id)!;
                    const pushDataAsync = promisify<PushDataRequest, PushDataResponse>(
                        node.client.engine.pushData,
                    ).bind(node.client.engine);
                    await pushDataAsync({
                        data: [formattedLine],
                    });
                }
            }
        });
        return rl;
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
                // 创建集群管理服务客户端
                cluster: new ClusterClient(addr, grpc.credentials.createInsecure()),
                // 创建引擎服务客户端
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
        // 注册集群管理服务
        this.server.addService(ClusterService, {
            /**
             * 处理节点加入请求
             * 将请求节点加入本地节点列表，并返回当前所有节点信息
             */
            join: async (
                call: grpc.ServerUnaryCall<JoinRequest, JoinResponse>,
                callback: grpc.sendUnaryData<JoinResponse>,
            ) => {
                const node = call.request.node!;
                const { id, addr } = node;
                if (!this.nodes.has(id)) {
                    this.nodes.set(id, this.nodeInfo(id, addr));
                    console.log(`Joined with node ${id} at ${addr}`);
                }
                const nodes = [...this.nodes.values()].map((n) => ({
                    id: n.id,
                    addr: n.addr,
                }));
                callback(null, {
                    nodes,
                });
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
                    console.log(`Left node ${id} at ${addr}`);
                }
                callback(null, {});
            },
        });
        // 注册引擎服务
        this.server.addService(EngineService, {
            /**
             * 处理元数据查询请求
             * 返回引擎的元数据信息
             */
            metaData: async (
                call: grpc.ServerUnaryCall<MetaDataRequest, MetaDataResponse>,
                callback: grpc.sendUnaryData<MetaDataResponse>,
            ) => {
                callback(null, {
                    metadata: {
                        id: this.id,
                        kind: EngineKind.EAGER, // 积极模式
                        input: ["`x"], // 输入模式
                        output: ["`x"], // 输出模式
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
                    console.log(`Received data: ${item}`);
                    this.engine.input(item);
                }
                callback(null, {});
            },
            /**
             * 处理数据拉取请求
             * 返回本地存储的所有数据
             */
            pullData: async (
                call: grpc.ServerUnaryCall<PullDataRequest, PullDataResponse>,
                callback: grpc.sendUnaryData<PullDataResponse>,
            ) => {
                callback(null, {
                    data: this.engine.getData(),
                });
            },
        });
        // 绑定服务器到指定地址
        const bindAsync = promisify<string, grpc.ServerCredentials, number>(this.server.bindAsync).bind(this.server);
        const port = await bindAsync(this.addr, grpc.ServerCredentials.createInsecure());
        this.addr = `${this.addr.split(":")[0]}:${port}`;
        this.setupSearchLoop();
        const rl = this.setupStdinLoop();
        // 注册进程终止信号处理器，确保优雅退出
        process.on("SIGINT", async () => {
            rl.close(); // 关闭 readline 接口
            await this.leave(); // 通知其他节点本节点离开
            process.exit(0);
        });
        // 注册 SIGUSR1 信号处理器，打印所有节点信息
        process.on("SIGUSR1", () => {
            console.log("=== All Nodes Information ===");
            for (const [id, nodeInfo] of this.nodes.entries()) {
                console.log(`Node ID: ${id}`);
                console.log(`  Address: ${nodeInfo.addr}`);
            }
            console.log(`Total nodes: ${this.nodes.size}`);
            console.log("=============================");
        });
        // 注册 SIGUSR2 信号处理器，打印所有数据
        process.on("SIGUSR2", () => {
            console.log("=== All Data Managed by Search ===");
            const data = this.engine.getData();
            data.forEach((item, index) => {
                console.log(`[${index + 1}] ${item}`);
            });
            console.log(`Total data items: ${data.length}`);
            console.log("==================================");
        });
        console.log(`Node ${this.id} listening on ${this.addr}`);
        // 将自己加入节点列表
        this.nodes.set(this.id, {
            id: this.id,
            addr: this.addr,
            client: {
                cluster: null as any,
                engine: null as any,
            },
        });
        return this;
    }
    /**
     * 加入现有集群
     * @param {string} addr - 要加入的集群中某个节点的地址
     */
    async join(addr: string): Promise<void> {
        // 创建目标节点的集群服务客户端
        const client = new ClusterClient(addr, grpc.credentials.createInsecure());
        const joinAsync = promisify<JoinRequest, JoinResponse>(client.join).bind(client);
        // 向目标节点发送加入请求
        const response = await joinAsync({
            node: this,
        });
        // 遍历集群中的所有节点
        const localData = this.engine.getData();
        for (const node of response.nodes) {
            if (!this.nodes.has(node.id)) {
                // 创建节点信息并保存
                const nodeInfo = this.nodeInfo(node.id, node.addr);
                this.nodes.set(node.id, nodeInfo);
                // 向该节点发送加入请求
                const nodeJoinAsync = promisify<JoinRequest, JoinResponse>(nodeInfo.client.cluster.join).bind(
                    nodeInfo.client.cluster,
                );
                await nodeJoinAsync({
                    node: this,
                });
                // 如果本地有数据，推送给新节点
                if (localData.length > 0) {
                    const nodePushAsync = promisify<PushDataRequest, PushDataResponse>(
                        nodeInfo.client.engine.pushData,
                    ).bind(nodeInfo.client.engine);
                    await nodePushAsync({
                        data: localData,
                    });
                }
                // 从该节点拉取历史数据
                const nodePullAsync = promisify<PullDataRequest, PullDataResponse>(
                    nodeInfo.client.engine.pullData,
                ).bind(nodeInfo.client.engine);
                const dataResponse = await nodePullAsync({});
                if (dataResponse.data) {
                    for (const item of dataResponse.data) {
                        console.log(`Receiving data: ${item}`);
                        this.engine.input(item); // 添加到搜索引擎（数据自动保存）
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
                // 排除自己
                const node = this.nodes.get(id)!;
                const leaveAsync = promisify<LeaveRequest, LeaveResponse>(node.client.cluster.leave).bind(
                    node.client.cluster,
                );
                await leaveAsync({
                    node: this,
                });
                console.log(`Leaving node ${node.id} at ${node.addr}`);
            }
        }
    }
}

function isIntegerString(str: string): boolean {
    if (typeof str !== "string") return false;
    return /^\d+$/.test(str);
}

function addAddressPrefixForPort(addrOrPort: string, ip: string): string {
    if (isIntegerString(addrOrPort)) {
        return `${ip}:${addrOrPort}`;
    }
    return addrOrPort;
}

// ============================================================
// 主程序入口
// ============================================================

// 命令行参数检查
// 用法: node dist/main.mjs <bind_addr> [<join_addr>]
//   bind_addr: 本节点绑定的端口号
//   join_addr: (可选) 要加入的集群中某个节点的端口号
if (process.argv.length < 3 || process.argv.length > 4) {
    console.error("Usage: main <bind_addr> [<join_addr>]");
    process.exit(1);
}

// 场景一: 加入现有集群
// 启动节点并加入指定地址的集群
if (process.argv.length === 4) {
    const listenAddr = addAddressPrefixForPort(process.argv[2], "0.0.0.0");
    const joinAddr = addAddressPrefixForPort(process.argv[3], "127.0.0.1");
    console.log(`Starting node at ${listenAddr} and joining ${joinAddr}`);
    const node = new ClusterNode(listenAddr);
    await node.listen(); // 启动服务监听
    await node.join(joinAddr); // 加入集群
}

// 场景二: 创建新集群
// 启动第一个节点，作为集群的初始节点
if (process.argv.length === 3) {
    const listenAddr = addAddressPrefixForPort(process.argv[2], "0.0.0.0");
    console.log(`Starting node at ${listenAddr}`);
    const node = new ClusterNode(listenAddr);
    await node.listen(); // 启动服务监听
}
