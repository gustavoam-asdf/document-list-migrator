import { updateRucsFile } from "./updateRucsFile";

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

const {
	dnisPath,
	rucsPath,
} = await updateRucsFile();

type WorkerStatus = {
	isDone: true;
	endAt: number;
} | {
	isDone: false;
	endAt: null;
}

let dniStatus: WorkerStatus = {
	isDone: false,
	endAt: null,
};
let rucStatus: WorkerStatus = {
	isDone: false,
	endAt: null,
};

const dniWorker = new Worker(new URL("./workers/dniWorker.ts", import.meta.url), {
	type: "module",
	name: "dniWorker",
});

dniWorker.postMessage(dnisPath);

dniWorker.addEventListener("message", (message) => {
	console.log({
		dniMessage: message
	})
	dniStatus = {
		isDone: true,
		endAt: Date.now(),
	};

	if (dniStatus.isDone && rucStatus.isDone) {
		const endTime = Date.now();
		console.log(`Done DNI in ${dniStatus.endAt - startTime}ms`);
		console.log(`Done RUC in ${rucStatus.endAt - startTime}ms`);
		console.log(`Done all in ${endTime - startTime}ms`);
	}
})

dniWorker.addEventListener("error", (message) => {
	console.error({
		dniMessage: message
	})
})

const rucWorker = new Worker(new URL("./workers/rucWorker.ts", import.meta.url), {
	type: "module",
	name: "rucWorker",
});

rucWorker.postMessage(rucsPath);

rucWorker.addEventListener("message", (message) => {
	console.log({
		rucMessage: message
	})

	rucStatus = {
		isDone: true,
		endAt: Date.now(),
	};

	if (dniStatus.isDone && rucStatus.isDone) {
		const endTime = Date.now();
		console.log(`Done DNI in ${dniStatus.endAt - startTime}ms`);
		console.log(`Done RUC in ${rucStatus.endAt - startTime}ms`);
		console.log(`Done all in ${endTime - startTime}ms`);
	}
})

rucWorker.addEventListener("error", (message) => {
	console.error({
		rucMessage: message
	})
})