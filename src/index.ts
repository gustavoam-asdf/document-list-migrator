import { filesDir } from "./constants";
import { updateRucsFile } from "./updateRucsFile";

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

const {
	dnisPath,
	rucsPath,
} = {
	dnisPath: `${filesDir}/dnis.txt`,
	rucsPath: `${filesDir}/padron_reducido_ruc.txt`,
}; //await updateRucsFile();

const dniWorker = new Worker(new URL("./workers/dniWorker.ts", import.meta.url), {
	type: "module",
	name: "dniWorker",
});

dniWorker.postMessage(dnisPath);

dniWorker.addEventListener("message", (message) => {
	console.log({
		dniMessage: message
	})
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
})

rucWorker.addEventListener("error", (message) => {
	console.error({
		rucMessage: message
	})
})

const endTime = Date.now();

console.log(`Done in ${endTime - startTime}ms`);