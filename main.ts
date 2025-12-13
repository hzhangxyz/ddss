import { promisify } from "node:util";
import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";
import * as grpc from "@grpc/grpc-js";
import { Search as AtsdsSearch, type Rule } from "atsds";
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

interface NodeClient {
    cluster: ClusterClient;
    engine: EngineClient;
}

interface NodeInfo {
    id: string;
    addr: string;
    client: NodeClient;
}

interface EagerEngine {
    input(rule: string): string | null;
    output(callback: (result: string) => void): number;
    meta(): { input: string[]; output: string[] };
}

class AtsdsSearchEngine extends AtsdsSearch implements EagerEngine {
    constructor(limitSize: number = 1000, bufferSize: number = 10000) {
        super(limitSize, bufferSize);
    }

    input(rule: string): string | null {
        const parsedRule = parse(rule);
        if (super.add(parsedRule)) {
            return unparse(parsedRule);
        }
        return null;
    }

    output(callback: (result: string) => void): number {
        return super.execute((candidate: Rule) => {
            callback(unparse(candidate.toString()));
            return false;
        });
    }

    meta(): { input: string[]; output: string[] } {
        return {
            input: ["`x"],
            output: ["`x"],
        };
    }
}

class DataManager {
    private data: Set<string>;

    constructor() {
        this.data = new Set();
    }

    addData(data: string): boolean {
        if (!this.data.has(data)) {
            this.data.add(data);
            return true;
        }
        return false;
    }

    getData(): string[] {
        return Array.from(this.data);
    }
}

class EagerEngineScheduler {
    private eagerEngine: EagerEngine;
    private onSearchResults: (results: string[]) => Promise<void>;
    private searchInterval: number;
    private dataManager: DataManager;
    private isRunning: boolean;
    private lastSearchTime: number;

    constructor(
        eagerEngine: EagerEngine,
        onSearchResults: (results: string[]) => Promise<void>,
        searchInterval: number = 1000,
    ) {
        this.eagerEngine = eagerEngine;
        this.onSearchResults = onSearchResults;
        this.searchInterval = searchInterval;
        this.dataManager = new DataManager();
        this.isRunning = false;
        this.lastSearchTime = Date.now();
    }

    start(): void {
        if (this.isRunning) return;
        this.isRunning = true;
        this.scheduleNextSearch();
    }

    stop(): void {
        this.isRunning = false;
    }

    addData(input: string): string | null {
        const formatted = this.eagerEngine.input(input);
        if (formatted && this.dataManager.addData(formatted)) {
            return formatted;
        }
        return null;
    }

    addDataBatch(data: string[]): string[] {
        const added: string[] = [];
        for (const item of data) {
            const formatted = this.addData(item);
            if (formatted) {
                added.push(formatted);
            }
        }
        return added;
    }

    getMetaData(): { input: string[]; output: string[] } {
        return this.eagerEngine.meta();
    }

    getData(): string[] {
        return this.dataManager.getData();
    }

    private scheduleNextSearch(): void {
        const now = Date.now();
        const elapsed = now - this.lastSearchTime;
        const delay = Math.max(this.searchInterval - elapsed, 0);

        setTimeout(async () => {
            if (!this.isRunning) return;
            this.lastSearchTime = Date.now();
            await this.performSearch();
            this.scheduleNextSearch();
        }, delay);
    }

    private async performSearch(): Promise<string[]> {
        const results: string[] = [];
        this.eagerEngine.output((result: string) => {
            if (this.dataManager.addData(result)) {
                results.push(result);
            }
        });
        if (results.length > 0) {
            await this.onSearchResults(results);
        }
        return results;
    }
}

class ClusterManager {
    private nodes: Map<string, NodeInfo>;
    private localId: string;

    constructor(localId: string, _localAddr: string) {
        this.nodes = new Map();
        this.localId = localId;
    }

    getAllNodes(): NodeInfo[] {
        return Array.from(this.nodes.values());
    }

    getAllOtherNodes(): NodeInfo[] {
        return this.getAllNodes().filter((node) => node.id !== this.localId);
    }

    getAllNodeInfo(): Node[] {
        return this.getAllNodes().map((node) => ({ id: node.id, addr: node.addr }));
    }

    addNode(id: string, addr: string): NodeInfo {
        if (this.hasNode(id)) {
            return this.nodes.get(id)!;
        }
        const client = this.createNodeClient(addr);
        const nodeInfo: NodeInfo = { id, addr, client };
        this.nodes.set(id, nodeInfo);
        return nodeInfo;
    }

    removeNode(id: string): boolean {
        if (!this.hasNode(id)) {
            return false;
        }
        const node = this.nodes.get(id)!;
        node.client.cluster.close();
        node.client.engine.close();
        return this.nodes.delete(id);
    }

    hasNode(id: string): boolean {
        return this.nodes.has(id);
    }

    createNodeClient(addr: string): NodeClient {
        return {
            cluster: new ClusterClient(addr, grpc.credentials.createInsecure()),
            engine: new EngineClient(addr, grpc.credentials.createInsecure()),
        };
    }
}

interface NetworkHandlerCallbacks {
    onJoin: (node: Node) => Promise<void>;
    onLeave: (node: Node) => Promise<void>;
    onList: () => Promise<Node[]>;
    onPushData: (data: string[]) => Promise<string[]>;
    onMetaData: () => Promise<{ id: string; kind: EngineKind; input: string[]; output: string[] }>;
    onPullData: () => Promise<string[]>;
}

class NetworkHandler {
    private server: grpc.Server;
    private addr: string;
    private callbacks: NetworkHandlerCallbacks;

    constructor(addr: string, _id: string, callbacks: NetworkHandlerCallbacks) {
        this.server = new grpc.Server();
        this.addr = addr;
        this.callbacks = callbacks;
    }

    async start(): Promise<string> {
        this.setupServices();
        const bindAsync = promisify(this.server.bindAsync).bind(this.server);
        const port = await bindAsync(this.addr, grpc.ServerCredentials.createInsecure());
        this.addr = `${this.addr.split(":")[0]}:${port}`;
        return this.addr;
    }

    async stop(): Promise<void> {
        return new Promise((resolve) => {
            this.server.tryShutdown(() => resolve());
        });
    }

    async callJoin(node: Node, client: NodeClient): Promise<void> {
        const joinAsync = promisify<JoinRequest, JoinResponse>(client.cluster.join).bind(client.cluster);
        await joinAsync({ node });
    }

    async callLeave(node: Node, client: NodeClient): Promise<void> {
        const leaveAsync = promisify<LeaveRequest, LeaveResponse>(client.cluster.leave).bind(client.cluster);
        await leaveAsync({ node });
    }

    async callList(client: NodeClient): Promise<Node[]> {
        const listAsync = promisify<ListRequest, ListResponse>(client.cluster.list).bind(client.cluster);
        const response = await listAsync({});
        return response.nodes;
    }

    async callPushData(data: string[], client: NodeClient): Promise<void> {
        const pushAsync = promisify<PushDataRequest, PushDataResponse>(client.engine.pushData).bind(client.engine);
        await pushAsync({ data });
    }

    async callPullData(client: NodeClient): Promise<string[]> {
        const pullAsync = promisify<PullDataRequest, PullDataResponse>(client.engine.pullData).bind(client.engine);
        const response = await pullAsync({});
        return response.data || [];
    }

    private setupServices(): void {
        this.server.addService(ClusterService, {
            join: async (
                call: grpc.ServerUnaryCall<JoinRequest, JoinResponse>,
                callback: grpc.sendUnaryData<JoinResponse>,
            ) => {
                const node = call.request.node;
                if (node) {
                    await this.callbacks.onJoin(node);
                }
                callback(null, {});
            },

            leave: async (
                call: grpc.ServerUnaryCall<LeaveRequest, LeaveResponse>,
                callback: grpc.sendUnaryData<LeaveResponse>,
            ) => {
                const node = call.request.node;
                if (node) {
                    await this.callbacks.onLeave(node);
                }
                callback(null, {});
            },

            list: async (
                _call: grpc.ServerUnaryCall<ListRequest, ListResponse>,
                callback: grpc.sendUnaryData<ListResponse>,
            ) => {
                const nodes = await this.callbacks.onList();
                callback(null, { nodes });
            },
        } as ClusterServer);

        this.server.addService(EngineService, {
            metaData: async (
                _call: grpc.ServerUnaryCall<MetaDataRequest, MetaDataResponse>,
                callback: grpc.sendUnaryData<MetaDataResponse>,
            ) => {
                const metadata = await this.callbacks.onMetaData();
                callback(null, { metadata });
            },

            pushData: async (
                call: grpc.ServerUnaryCall<PushDataRequest, PushDataResponse>,
                callback: grpc.sendUnaryData<PushDataResponse>,
            ) => {
                const data = call.request.data;
                if (data) {
                    await this.callbacks.onPushData(data);
                }
                callback(null, {});
            },

            pullData: async (
                _call: grpc.ServerUnaryCall<PullDataRequest, PullDataResponse>,
                callback: grpc.sendUnaryData<PullDataResponse>,
            ) => {
                const data = await this.callbacks.onPullData();
                callback(null, { data });
            },
        } as EngineServer);
    }
}

class EagerNode {
    private id: string;
    private addr: string;
    private clusterManager: ClusterManager;
    private engineScheduler: EagerEngineScheduler;
    private networkHandler: NetworkHandler;
    private stdinInterface: ReturnType<typeof createInterface> | null = null;

    constructor(engine: EagerEngine, addr: string, id: string = randomUUID()) {
        this.id = id;
        this.addr = addr;

        this.clusterManager = new ClusterManager(id, addr);
        this.engineScheduler = new EagerEngineScheduler(engine, this.handleSearchResults.bind(this));
        this.networkHandler = new NetworkHandler(addr, id, {
            onJoin: this.handleJoinRequest.bind(this),
            onLeave: this.handleLeaveRequest.bind(this),
            onList: this.handleListRequest.bind(this),
            onPushData: this.handlePushDataRequest.bind(this),
            onMetaData: this.handleMetaDataRequest.bind(this),
            onPullData: this.handlePullDataRequest.bind(this),
        });
    }

    async start(): Promise<void> {
        const actualAddr = await this.networkHandler.start();
        this.addr = actualAddr;
        this.clusterManager.addNode(this.id, this.addr);
        this.engineScheduler.start();
        this.setupIoHandling();

        console.log(`Node started: ${this.id} at ${this.addr}`);
    }

    async stop(): Promise<void> {
        this.engineScheduler.stop();
        await this.networkHandler.stop();

        if (this.stdinInterface) {
            this.stdinInterface.close();
        }

        await this.leaveCluster();
        console.log(`Node stopped: ${this.id}`);
    }

    async joinCluster(joinAddr: string): Promise<void> {
        const client = this.clusterManager.createNodeClient(joinAddr);

        const nodes = await this.networkHandler.callList(client);
        const currentNode = { id: this.id, addr: this.addr };

        for (const node of nodes) {
            if (node.id !== this.id) {
                const { client: nodeClient } = this.clusterManager.addNode(node.id, node.addr);
                await this.networkHandler.callJoin(currentNode, nodeClient);

                await this.syncDataWithNode(nodeClient);

                console.log(`Joining node: ${node.id} at ${node.addr}`);
            }
        }

        client.cluster.close();
        client.engine.close();
    }

    private async leaveCluster(): Promise<void> {
        const currentNode = { id: this.id, addr: this.addr };
        const otherNodes = this.clusterManager.getAllOtherNodes();

        for (const node of otherNodes) {
            await this.networkHandler.callLeave(currentNode, node.client);
            this.clusterManager.removeNode(node.id);
            console.log(`Leaving node: ${node.id}`);
        }
    }

    private async handleJoinRequest(node: Node): Promise<void> {
        if (!this.clusterManager.hasNode(node.id)) {
            const { client } = this.clusterManager.addNode(node.id, node.addr);
            console.log(`Node joined: ${node.id} at ${node.addr}`);

            const data = this.engineScheduler.getData();
            if (data.length > 0) {
                await this.networkHandler.callPushData(data, client);
            }
        }
    }

    private async handleLeaveRequest(node: Node): Promise<void> {
        if (this.clusterManager.removeNode(node.id)) {
            console.log(`Node left: ${node.id} at ${node.addr}`);
        }
    }

    private async handleListRequest(): Promise<Node[]> {
        return this.clusterManager.getAllNodeInfo();
    }

    private async handlePushDataRequest(data: string[]): Promise<string[]> {
        const formatted = this.engineScheduler.addDataBatch(data);
        for (const item of formatted) {
            console.log(`Data received: ${item}`);
        }
        return formatted;
    }

    private async handleMetaDataRequest(): Promise<{
        id: string;
        kind: EngineKind;
        input: string[];
        output: string[];
    }> {
        const meta = this.engineScheduler.getMetaData();
        return {
            id: this.id,
            kind: EngineKind.EAGER,
            input: meta.input,
            output: meta.output,
        };
    }

    private async handlePullDataRequest(): Promise<string[]> {
        return this.engineScheduler.getData();
    }

    private async handleSearchResults(results: string[]): Promise<void> {
        if (results.length === 0) return;

        for (const result of results) {
            console.log(`Found result: ${result}`);
        }
        const otherNodes = this.clusterManager.getAllOtherNodes();
        for (const node of otherNodes) {
            await this.networkHandler.callPushData(results, node.client);
        }
    }

    private async syncDataWithNode(client: NodeClient): Promise<void> {
        const localData = this.engineScheduler.getData();
        if (localData.length > 0) {
            await this.networkHandler.callPushData(localData, client);
        }

        const remoteData = await this.networkHandler.callPullData(client);
        if (remoteData.length > 0) {
            this.engineScheduler.addDataBatch(remoteData);
            console.log(`Synced ${remoteData.length} data items from remote`);
        }
    }

    private setupIoHandling(): void {
        this.stdinInterface = createInterface({
            input: process.stdin,
            output: process.stdout,
            terminal: false,
        });

        this.stdinInterface.on("line", async (line: string) => {
            const trimmed = line.trim();
            if (trimmed.length === 0) return;

            const formatted = this.engineScheduler.addData(trimmed);
            if (formatted) {
                console.log(`Input processed: ${formatted}`);

                const otherNodes = this.clusterManager.getAllOtherNodes();
                for (const node of otherNodes) {
                    await this.networkHandler.callPushData([formatted], node.client);
                }
            }
        });

        process.on("SIGINT", async () => {
            console.log("\nShutting down...");
            await this.stop();
            process.exit(0);
        });

        process.on("SIGUSR1", () => {
            console.log("\n=== Cluster Information ===");
            console.log(`Current Node: ${this.id} at ${this.addr}`);
            console.log("\nConnected Nodes:");

            const nodes = this.clusterManager.getAllOtherNodes();
            nodes.forEach((node, index) => {
                console.log(`  ${index + 1}. ${node.id} at ${node.addr}`);
            });

            console.log(`Total nodes: ${nodes.length + 1}`);
            console.log("===========================\n");
        });

        process.on("SIGUSR2", () => {
            console.log("\n=== Data Information ===");
            const data = this.engineScheduler.getData();

            data.forEach((item, index) => {
                console.log(`  ${index + 1}. ${item}`);
            });

            console.log(`Total data items: ${data.length}`);
            console.log("========================\n");
        });
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

async function main() {
    if (process.argv.length < 3 || process.argv.length > 4) {
        console.error("Usage: main <bind_addr> [<join_addr>]");
        process.exit(1);
    }

    const listenAddr = addAddressPrefixForPort(process.argv[2], "0.0.0.0");
    const engine = new AtsdsSearchEngine();
    const node = new EagerNode(engine, listenAddr);

    await node.start();

    if (process.argv.length === 4) {
        const joinAddr = addAddressPrefixForPort(process.argv[3], "127.0.0.1");
        console.log(`Joining cluster at ${joinAddr}...`);
        await node.joinCluster(joinAddr);
    } else {
        console.log("Starting as first node in new cluster...");
    }

    process.stdin.resume();
}

if (import.meta.main) {
    main();
}
