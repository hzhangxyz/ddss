import { promisify } from "node:util";
import * as grpc from "@grpc/grpc-js";
import {
    type JoinRequest,
    type JoinResponse,
    type LeaveRequest,
    type LeaveResponse,
    type ListRequest,
    type ListResponse,
    type Node,
    ClusterClient,
    type ClusterServer,
    EngineClient,
} from "./ddss.js";

/**
 * 节点客户端接口
 * 包含集群管理和引擎服务的客户端
 */
export interface NodeClient {
    cluster: ClusterClient;
    engine: EngineClient;
}

/**
 * 带客户端的节点信息
 * 存储节点ID、地址和对应的gRPC客户端
 */
export interface NodeInfoWithClient {
    id: string;
    addr: string;
    client: NodeClient;
}

/**
 * 集群管理器
 * 负责节点发现、集群状态管理、节点加入和离开
 */
export class ClusterManager {
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
     * 创建节点信息对象
     * @param {string} id - 节点 ID
     * @param {string} addr - 节点地址
     * @param {NodeClient} client - 节点客户端
     * @returns {NodeInfoWithClient} 包含节点信息和 gRPC 客户端的对象
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
             * 将请求节点从本地节点列表中移除
             */
            leave: async (
                call: grpc.ServerUnaryCall<LeaveRequest, LeaveResponse>,
                callback: grpc.sendUnaryData<LeaveResponse>,
            ) => {
                const node = call.request.node;
                if (node) {
                    const { id, addr } = node;
                    if (this.nodes.has(id)) {
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
