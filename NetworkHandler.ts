import { promisify } from "node:util";
import * as grpc from "@grpc/grpc-js";
import {
    type MetaDataRequest,
    type MetaDataResponse,
    type PushDataRequest,
    type PushDataResponse,
    type PullDataRequest,
    type PullDataResponse,
    EngineKind,
    EngineClient,
    type EngineServer,
    EngineService,
    ClusterService,
} from "./ddss.js";
import type { ClusterManager } from "./ClusterManager.js";
import type { DataManager } from "./DataManager.js";

/**
 * 网络处理器
 * 负责网络通信的抽象和gRPC服务器管理
 */
export class NetworkHandler {
    private server: grpc.Server;
    private clusterManager: ClusterManager;
    private dataManager: DataManager;

    constructor(clusterManager: ClusterManager, dataManager: DataManager) {
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
                        if (this.dataManager.addData(item)) {
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
