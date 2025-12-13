import { promisify } from "node:util";
import type { PushDataRequest, PushDataResponse, PullDataRequest, PullDataResponse } from "./ddss.js";
import type { NodeInfoWithClient } from "./ClusterManager.js";

/**
 * 积极引擎接口
 * 定义引擎必须实现的基本操作
 */
export interface EagerEngine {
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
 * 数据管理器
 * 负责数据存储和同步
 */
export class DataManager {
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
