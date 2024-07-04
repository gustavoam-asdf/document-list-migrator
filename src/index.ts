import { WorkerPromise } from "./WorkerPromise";
import { updateRucsFile } from "./updateRucsFile";

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

const {
	dnisPath,
	rucsPath,
} = await updateRucsFile();

const dniWorker = WorkerPromise({
	path: "./workers/dniWorker.ts",
	name: "dniWorker",
	startMessage: {
		filePath: dnisPath,
		useSecondaryDb: false,
	}
})

const rucWorker = WorkerPromise({
	path: "./workers/rucWorker.ts",
	name: "rucWorker",
	startMessage: {
		filePath: rucsPath,
		useSecondaryDb: false,
	}
})

await Promise.all([
	dniWorker.then(() => {
		const endTime = Date.now();
		console.log(`Done DNI in ${endTime - startTime}ms`);
	}),
	rucWorker.then(() => {
		const endTime = Date.now();
		console.log(`Done RUC in ${endTime - startTime}ms`);
	}),
])

const endTime = Date.now();
console.log(`Done all in ${endTime - startTime}ms`);