export type WorkerStartMessage = {
	filePath: string;
	useSecondaryDb: boolean;
	workerName: string;
}

export type WorkerProgressMessage = {
	type: "progress";
	workerName: string;
	rows: number;        // filas insertadas en este batch
	rejected: number;    // filas que terminaron en rejects este batch
	batchMs: number;     // latencia del batch
}

export type WorkerDoneMessage = {
	type: "done";
	workerName: string;
	count: number;
	rejected: number;
}

export type WorkerMessage = WorkerProgressMessage | WorkerDoneMessage

export type WorkerResult = {
	workerName: string;
	count: number;
	rejected: number;
}

export type ProgressCallback = (result: WorkerResult, totals: ProgressTotals) => void;

export type ProgressTotals = {
	completedWorkers: number;
	totalWorkers: number;
	totalRecords: number;
	totalRejected: number;
}

export class WorkerProgressTracker {
	private completedWorkers = 0;
	private totalRecords = 0;
	private totalRejected = 0;
	private totalWorkers: number;
	private onProgress?: ProgressCallback;

	constructor(totalWorkers: number, onProgress?: ProgressCallback) {
		this.totalWorkers = totalWorkers;
		this.onProgress = onProgress;
	}

	setTotalWorkers(n: number) {
		this.totalWorkers = n
	}

	addResult(result: WorkerResult): ProgressTotals {
		this.completedWorkers++;
		this.totalRecords += result.count;
		this.totalRejected += result.rejected;

		const totals = this.getTotals()
		this.onProgress?.(result, totals);
		return totals;
	}

	getTotals(): ProgressTotals {
		return {
			completedWorkers: this.completedWorkers,
			totalWorkers: this.totalWorkers,
			totalRecords: this.totalRecords,
			totalRejected: this.totalRejected,
		};
	}
}

export type WorkerPromiseParams = {
	workerPath: string;
	name: string;
	startMessage: Omit<WorkerStartMessage, "workerName">;
	onProgress?: (msg: WorkerProgressMessage) => void;
}

export type WorkerHandle = {
	name: string
	worker: Worker
	promise: Promise<WorkerResult>
	requestShutdown: () => void
	terminate: () => void
}

export function spawnWorker({
	workerPath,
	name,
	startMessage,
	onProgress,
}: WorkerPromiseParams): WorkerHandle {
	const worker = new Worker(new URL(workerPath, import.meta.url), {
		type: "module",
		name,
	});

	const promise = new Promise<WorkerResult>((resolve, reject) => {
		worker.addEventListener("message", (event: MessageEvent<WorkerMessage>) => {
			const msg = event.data
			if (msg.type === "progress") {
				onProgress?.(msg)
				return
			}
			if (msg.type === "done") {
				resolve({ workerName: msg.workerName, count: msg.count, rejected: msg.rejected });
			}
		})

		worker.addEventListener("error", (message) => {
			reject(message);
		})
	})

	worker.postMessage({ ...startMessage, workerName: name });

	return {
		name,
		worker,
		promise,
		requestShutdown: () => worker.postMessage({ type: "shutdown" }),
		terminate: () => worker.terminate(),
	}
}

// Backwards-compatible: la firma vieja se mantiene para callers que no necesiten
// el handle (no quedan en este repo, pero por si acaso).
export function WorkerPromise(params: WorkerPromiseParams): Promise<WorkerResult> {
	return spawnWorker(params).promise
}
