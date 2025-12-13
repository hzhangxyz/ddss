import type { NodeInfoWithClient } from "./ClusterManager.js";
import type { DataManager } from "./DataManager.js";

/**
 * 引擎调度器
 * 负责搜索任务的调度和执行
 */
export class EngineScheduler {
    private dataManager: DataManager;
    private isRunning: boolean;

    constructor(dataManager: DataManager) {
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
                if (this.dataManager.addData(result)) {
                    newData.push(result);
                    console.log(`Found data: ${result}`);
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
