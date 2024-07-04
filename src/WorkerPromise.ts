export function WorkerPromise({
	path,
	name,
	startMessage,
}: {
	path: string;
	name: string;
	startMessage: {
		filePath: string;
		useSecondaryDb: boolean;
	}
}) {
	return new Promise((resolve, reject) => {
		const worker = new Worker(new URL(path, import.meta.url), {
			type: "module",
			name,
		});

		worker.postMessage(startMessage);

		worker.addEventListener("message", (message) => {
			resolve(message);
		})

		worker.addEventListener("error", (message) => {
			reject(message);
		})
	})

}