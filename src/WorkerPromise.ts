export type WorkerStartMessage = {
	filePath: string;
	useSecondaryDb: boolean;
	workerName: string;
}

export type WorkerResult = {
	workerName: string;
	count: number;
}

export type ProgressCallback = (result: WorkerResult, totals: ProgressTotals) => void;

export type ProgressTotals = {
	completedWorkers: number;
	totalWorkers: number;
	totalRecords: number;
}

export class WorkerProgressTracker {
	private completedWorkers = 0;
	private totalRecords = 0;
	private totalWorkers: number;
	private onProgress?: ProgressCallback;

	constructor(totalWorkers: number, onProgress?: ProgressCallback) {
		this.totalWorkers = totalWorkers;
		this.onProgress = onProgress;
	}

	addResult(result: WorkerResult): ProgressTotals {
		this.completedWorkers++;
		this.totalRecords += result.count;

		const totals: ProgressTotals = {
			completedWorkers: this.completedWorkers,
			totalWorkers: this.totalWorkers,
			totalRecords: this.totalRecords,
		};

		this.onProgress?.(result, totals);
		return totals;
	}

	getTotals(): ProgressTotals {
		return {
			completedWorkers: this.completedWorkers,
			totalWorkers: this.totalWorkers,
			totalRecords: this.totalRecords,
		};
	}
}

export function WorkerPromise({
	workerPath,
	name,
	startMessage,
	progressTracker,
}: {
	workerPath: string;
	name: string;
	startMessage: Omit<WorkerStartMessage, 'workerName'>;
	progressTracker?: WorkerProgressTracker;
}): Promise<WorkerResult> {
	return new Promise((resolve, reject) => {
		const worker = new Worker(new URL(workerPath, import.meta.url), {
			type: "module",
			name,
		});

		worker.postMessage({ ...startMessage, workerName: name });

		worker.addEventListener("message", (event: MessageEvent<WorkerResult>) => {
			const result = event.data;
			progressTracker?.addResult(result);
			resolve(result);
		})

		worker.addEventListener("error", (message) => {
			reject(message);
		})
	})
}