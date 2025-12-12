import { dnisDir, rucsDir } from "./constants";
import { dropConstraintSafely, recreateConstraintSafely } from "./constraintManager";
import { primarySql, secondarySql } from "./db";

import { WorkerPromise } from "./WorkerPromise";
import fs from "node:fs/promises";
import { redis } from "./redis";
import { updateRucsFile } from "./updateRucsFile";

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

const { dniChunkFiles, rucChunkFiles } = await updateRucsFile()
	.then(res => {
		console.log("Updated document files");
		return res;
	})
	.catch(error => {
		console.warn("Error updating document, will try again...")
		console.warn(error);
		return updateRucsFile()
			.catch(err => {
				console.error("Error updating document again, exiting...");
				console.error(err)
				process.exit(1);
			})
	})

console.log(`Found ${dniChunkFiles.length} DNI chunk files and ${rucChunkFiles.length} RUC chunk files`);

const secondaryTimeStart = Date.now();

// Desactivar FK constraint para mejorar rendimiento de INSERT masivo
await dropConstraintSafely(secondarySql, "secondary");

await secondarySql.begin(async sql => {
	await sql`TRUNCATE "PersonaNatural"`
	await sql`TRUNCATE "PersonaJuridica"`

	console.log("Truncated secondary tables");
})

console.log(`Creating ${dniChunkFiles.length} DNI workers and ${rucChunkFiles.length} RUC workers for secondary database`);

// Create parallel workers for secondary database
const secondaryDniStartTime = Date.now();
const secondaryDniPromise = Promise.all(
	dniChunkFiles.map((filePath, index) =>
		WorkerPromise({
			workerPath: "./workers/dniWorker.ts",
			name: `dni-secondary-${index}`,
			startMessage: {
				filePath,
				useSecondaryDb: true,
			}
		})
	)
).then(() => {
	console.log(`Done secondary DNI in ${Date.now() - secondaryDniStartTime}ms`);
});

const secondaryRucStartTime = Date.now();
const secondaryRucPromise = Promise.all(
	rucChunkFiles.map((filePath, index) =>
		WorkerPromise({
			workerPath: "./workers/rucWorker.ts",
			name: `ruc-secondary-${index}`,
			startMessage: {
				filePath,
				useSecondaryDb: true,
			}
		})
	)
).then(() => {
	console.log(`Done secondary RUC in ${Date.now() - secondaryRucStartTime}ms`);
});

await Promise.all([secondaryDniPromise, secondaryRucPromise]).then(() => {
	console.log(`Done secondary DB in ${Date.now() - secondaryTimeStart}ms`);
});

// Recrear FK constraint después del INSERT masivo
await recreateConstraintSafely(secondarySql, "secondary");

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

console.log("Setting state to updating");
await redis.set(stateKey, JSON.stringify(updatingState));

const primaryTimeStart = Date.now();

// Desactivar FK constraint para mejorar rendimiento de INSERT masivo
await dropConstraintSafely(primarySql, "primary");

await primarySql.begin(async sql => {
	await sql`TRUNCATE "PersonaNatural"`
	await sql`TRUNCATE "PersonaJuridica"`

	console.log("Truncated primary tables");
})

console.log(`Creating ${dniChunkFiles.length} DNI workers and ${rucChunkFiles.length} RUC workers for primary database`);

// Create parallel workers for primary database
const primaryDniStartTime = Date.now();
const primaryDniPromise = Promise.all(
	dniChunkFiles.map((filePath, index) =>
		WorkerPromise({
			workerPath: "./workers/dniWorker.ts",
			name: `dni-primary-${index}`,
			startMessage: {
				filePath,
				useSecondaryDb: false,
			}
		})
	)
).then(() => {
	console.log(`Done primary DNI in ${Date.now() - primaryDniStartTime}ms`);
});

const primaryRucStartTime = Date.now();
const primaryRucPromise = Promise.all(
	rucChunkFiles.map((filePath, index) =>
		WorkerPromise({
			workerPath: "./workers/rucWorker.ts",
			name: `ruc-primary-${index}`,
			startMessage: {
				filePath,
				useSecondaryDb: false,
			}
		})
	)
).then(() => {
	console.log(`Done primary RUC in ${Date.now() - primaryRucStartTime}ms`);
});

await Promise.all([primaryDniPromise, primaryRucPromise]).then(() => {
	console.log(`Done primary DB in ${Date.now() - primaryTimeStart}ms`);
});

// Recrear FK constraint después del INSERT masivo
await recreateConstraintSafely(primarySql, "primary");

const endTime = Date.now();

const nonUpdatingState: UpdateDataState = {
	isUpdating: false,
	lastUpdateAt: new Date(),
};

console.log("Setting state to non-updating");
await redis.set(stateKey, JSON.stringify(nonUpdatingState));

console.log("Removing chunk files...");
await fs.rm(dnisDir, { recursive: true });
await fs.rm(rucsDir, { recursive: true });

console.log(`Done all in ${endTime - startTime}ms`);

process.exit(0);