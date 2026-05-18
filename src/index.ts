import { dnisDir, rejectsDir, rucsDir } from "./constants";
import { dropConstraintSafely, recreateConstraintSafely } from "./constraints/constraintManager";
import { getMaxConnections, primarySql, secondarySql } from "./db";
import { installSignalHandlers, shouldExit, shutdownReasonText } from "./shared/shutdown";
import { runPhase, type PhaseResult } from "./supervisor/runPhase";

import fs from "node:fs/promises";
import { redis } from "./redis";
import { updateRucsFile } from "./split/updateRucsFile";

const startTime = Date.now();
console.log(`PID: ${process.pid}`);

installSignalHandlers();

async function shutdownAndExit(code: number): Promise<never> {
	console.log("[main] closing pools and exiting");
	try { await Promise.race([primarySql.end({ timeout: 5 }), wait(7_000)]) } catch (e) { console.warn("[main] primarySql.end failed", e) }
	try { await Promise.race([secondarySql.end({ timeout: 5 }), wait(7_000)]) } catch (e) { console.warn("[main] secondarySql.end failed", e) }
	try { redis.disconnect() } catch (e) { console.warn("[main] redis.disconnect failed", e) }
	process.exit(code);
}

function wait(ms: number) {
	return new Promise<void>(resolve => setTimeout(resolve, ms))
}

function exitIfShuttingDown(stage: string): void {
	if (shouldExit()) {
		console.warn(`[main] shutdown active (${shutdownReasonText()}) — skipping ${stage}`)
	}
}

const splitResult = await updateRucsFile()
	.catch(async error => {
		console.warn("[main] split failed, retrying once...", error);
		return updateRucsFile().catch(err => {
			console.error("[main] split failed twice, exiting:", err);
			return shutdownAndExit(1);
		});
	});

if (!splitResult) {
	await shutdownAndExit(1);
}

const { dniChunkFiles, rucChunkFiles, parseStats } = splitResult;

console.log(
	`[main] split summary | source=${parseStats.totalSourceLines.toLocaleString()} ` +
	`| dni=${parseStats.totalDniEmitted.toLocaleString()} (${dniChunkFiles.length} chunks, ${parseStats.dniRejectCount} rejects) ` +
	`| ruc=${parseStats.totalRucEmitted.toLocaleString()} (${rucChunkFiles.length} chunks, ${parseStats.rucRejectCount} rejects)`,
);

type UpdateDataState =
	| { isUpdating: false; lastUpdateAt: Date }
	| { isUpdating: true; startedAt: Date }

const stateKey = "document-list:update-data-state";

function logPhaseSummary(result: PhaseResult) {
	console.log(
		`\n========== PHASE ${result.phase.toUpperCase()} ==========\n` +
		`elapsed: ${(result.elapsedMs / 1000).toFixed(1)}s\n` +
		`rows inserted: ${result.totalInserted.toLocaleString()} ` +
		`(dni ${result.dniInserted.toLocaleString()} + ruc ${result.rucInserted.toLocaleString()})\n` +
		`rows rejected: ${result.totalRejected}\n` +
		`rows terminated workers: ${result.terminatedWorkers}\n` +
		`shutdown triggered: ${result.shutdownTriggered}\n` +
		`throughput avg: ${Math.round(result.avgRps).toLocaleString()} rps\n` +
		`concurrency: initial=${result.initialTarget} peak=${result.peakConcurrency} ` +
		`final=${result.finalTarget} max=${result.maxConcurrency}\n` +
		`rejects dir: ${rejectsDir}\n` +
		`==================================\n`,
	);
}

// ============ SECONDARY PHASE ============
const secondaryMaxConnections = await getMaxConnections(secondarySql);
await dropConstraintSafely(secondarySql, "secondary");

console.log("[main] truncating secondary tables...");
await secondarySql.begin(async sql => {
	await sql`TRUNCATE "PersonaNatural"`;
	await sql`TRUNCATE "PersonaJuridica"`;
});

const secondaryResult = await runPhase({
	phase: "secondary",
	useSecondaryDb: true,
	dniChunkFiles,
	rucChunkFiles,
	maxConnections: secondaryMaxConnections,
});
logPhaseSummary(secondaryResult);

if (!secondaryResult.shutdownTriggered) {
	await recreateConstraintSafely(secondarySql, "secondary");
} else {
	console.warn("[main] skipping FK recreate on secondary — phase was aborted");
	await shutdownAndExit(130);
}

exitIfShuttingDown("primary phase");
if (shouldExit()) await shutdownAndExit(130);

// ============ SWITCH FLAG ============
console.log("[main] setting redis state to updating");
const updatingState: UpdateDataState = { isUpdating: true, startedAt: new Date() };
await redis.set(stateKey, JSON.stringify(updatingState));

// ============ PRIMARY PHASE ============
const primaryMaxConnections = await getMaxConnections(primarySql);
await dropConstraintSafely(primarySql, "primary");

console.log("[main] truncating primary tables...");
await primarySql.begin(async sql => {
	await sql`TRUNCATE "PersonaNatural"`;
	await sql`TRUNCATE "PersonaJuridica"`;
});

const primaryResult = await runPhase({
	phase: "primary",
	useSecondaryDb: false,
	dniChunkFiles,
	rucChunkFiles,
	maxConnections: primaryMaxConnections,
});
logPhaseSummary(primaryResult);

if (!primaryResult.shutdownTriggered) {
	await recreateConstraintSafely(primarySql, "primary");
} else {
	console.warn("[main] skipping FK recreate on primary — phase was aborted");
	// Dejamos el flag en updating para que los lectores no asuman datos completos.
	await shutdownAndExit(130);
}

// ============ FINAL FLAG + CLEANUP ============
const nonUpdatingState: UpdateDataState = { isUpdating: false, lastUpdateAt: new Date() };
console.log("[main] setting redis state to non-updating");
await redis.set(stateKey, JSON.stringify(nonUpdatingState));

console.log("[main] removing chunk dirs");
await fs.rm(dnisDir, { recursive: true });
await fs.rm(rucsDir, { recursive: true });

const totalElapsed = Date.now() - startTime;
console.log(
	`\n========== ALL DONE in ${(totalElapsed / 1000).toFixed(1)}s ==========\n` +
	`secondary: ${secondaryResult.totalInserted.toLocaleString()} inserted, ${secondaryResult.totalRejected} rejected\n` +
	`primary:   ${primaryResult.totalInserted.toLocaleString()} inserted, ${primaryResult.totalRejected} rejected\n` +
	`parse rejects: dni=${parseStats.dniRejectCount}, ruc=${parseStats.rucRejectCount}\n` +
	`rejects directory preserved at: ${rejectsDir}\n`,
);

await shutdownAndExit(0);
