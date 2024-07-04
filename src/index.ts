import { primarySql, secondarySql } from "./db";

import { WorkerPromise } from "./WorkerPromise";
import fs from "node:fs/promises";
import { redis } from "./redis";
import { updateRucsFile } from "./updateRucsFile";

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

const {
	dnisPath,
	rucsPath,
} = await updateRucsFile();

const secondaryTimeStart = Date.now();

await secondarySql.begin(async sql => {
	await sql`TRUNCATE "PersonaNatural"`
	await sql`TRUNCATE "PersonaJuridica"`

	console.log("Truncated secondary tables");
})

const secondaryDniWorker = WorkerPromise({
	path: "./workers/dniWorker.ts",
	name: "dniWorker",
	startMessage: {
		filePath: dnisPath,
		useSecondaryDb: true,
	}
})

const secondaryRucWorker = WorkerPromise({
	path: "./workers/rucWorker.ts",
	name: "rucWorker",
	startMessage: {
		filePath: rucsPath,
		useSecondaryDb: true,
	}
})

await Promise.all([
	secondaryDniWorker.then(() => {
		const endTime = Date.now();
		console.log(`Done secondary DNI in ${endTime - secondaryTimeStart}ms`);
	}),
	secondaryRucWorker.then(() => {
		const endTime = Date.now();
		console.log(`Done secondary RUC in ${endTime - secondaryTimeStart}ms`);
	}),
])

type UpdateDataState = {
	isUpdating: false
	lastUpdateAt: Date
} | {
	isUpdating: true
	startedAt: Date
}

const stateKey = "document-list:update-data-state";
const updatingState: UpdateDataState = {
	isUpdating: true,
	startedAt: new Date(),
};

console.log("Setting state to non-updating");
await redis.set(stateKey, JSON.stringify(updatingState));

const primaryTimeStart = Date.now();

await primarySql.begin(async sql => {
	await sql`TRUNCATE "PersonaNatural"`
	await sql`TRUNCATE "PersonaJuridica"`

	console.log("Truncated primary tables");
})

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
		console.log(`Done DNI in ${endTime - primaryTimeStart}ms`);
	}),
	rucWorker.then(() => {
		const endTime = Date.now();
		console.log(`Done RUC in ${endTime - primaryTimeStart}ms`);
	}),
])

const endTime = Date.now();

const nonUpdatingState: UpdateDataState = {
	isUpdating: false,
	lastUpdateAt: new Date(),
};

console.log("Setting state to non-updating");
await redis.set(stateKey, JSON.stringify(nonUpdatingState));

await fs.rm(dnisPath);
await fs.rm(rucsPath);

console.log(`Done all in ${endTime - startTime}ms`);