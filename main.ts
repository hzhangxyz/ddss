import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";
import * as grpc from "@grpc/grpc-js";
import { Search as Search_, type Rule } from "atsds";
import { parse, unparse } from "atsds-bnf";
import { type Node, ClusterClient, type EngineClient } from "./ddss.js";
import { ClusterManager, type NodeInfoWithClient } from "./ClusterManager.js";
import { DataManager, type EagerEngine } from "./DataManager.js";
import { EngineScheduler } from "./EngineScheduler.js";
import { NetworkHandler } from "./NetworkHandler.js";

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
 * 使用组件化架构：ClusterManager、DataManager、EngineScheduler、NetworkHandler
 */
class EagerNode {
    private clusterManager: ClusterManager;
    private dataManager: DataManager;
    private engineScheduler: EngineScheduler;
    private networkHandler: NetworkHandler;

    /**
     * 构造函数
     * @param {EagerEngine} engine - 实现 EagerEngine 接口的引擎实例
     * @param {string} addr - 节点绑定地址
     * @param {string} id - 节点唯一标识符，默认随机生成
     */
    constructor(engine: EagerEngine, addr: string, id: string = randomUUID()) {
        this.clusterManager = new ClusterManager(id, addr);
        this.dataManager = new DataManager(engine);
        this.engineScheduler = new EngineScheduler(this.dataManager);
        this.networkHandler = new NetworkHandler(this.clusterManager, this.dataManager);
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
            const formattedLine = this.dataManager.addData(trimmedLine);
            if (formattedLine !== null) {
                console.log(`Received input: ${formattedLine}`);
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
                        const formattedItem = this.dataManager.addData(item);
                        if (formattedItem !== null) {
                            console.log(`Receiving data: ${formattedItem}`);
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
