export type WorkerStartMessage = {
	filePath: string;
	useSecondaryDb: boolean;
	workerName: string;
}

export function WorkerPromise({
	workerPath,
	name,
	startMessage,
}: {
	workerPath: string;
	name: string;
	startMessage: Omit<WorkerStartMessage, 'workerName'>;
}) {
	return new Promise((resolve, reject) => {
		const worker = new Worker(new URL(workerPath, import.meta.url), {
			type: "module",
			name,
		});

		worker.postMessage({ ...startMessage, workerName: name });

		worker.addEventListener("message", (message) => {
			resolve(message);
		})

		worker.addEventListener("error", (message) => {
			reject(message);
		})
	})
}