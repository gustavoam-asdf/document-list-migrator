import { primarySql, secondarySql } from "../db"

import { BATCH_ROWS_DNI } from "../constants"
import { runCopyWorker } from "./runCopyWorker"

// prevents TS errors
declare var self: Worker

let shuttingDown = false
let started = false

self.addEventListener("message", async (event: MessageEvent<any>) => {
	const data = event.data

	if (data?.type === "shutdown") {
		shuttingDown = true
		return
	}

	if (started) return
	started = true

	const { filePath, useSecondaryDb, workerName } = data as {
		filePath: string
		useSecondaryDb: boolean
		workerName: string
	}
	const sql = useSecondaryDb ? secondarySql : primarySql

	await runCopyWorker({
		filePath,
		useSecondaryDb,
		workerName,
		batchRows: BATCH_ROWS_DNI,
		createCopyStream: () => sql`COPY "PersonaNatural" ("dni", "nombreCompleto") FROM STDIN`.writable(),
		postMessage: msg => self.postMessage(msg),
		shouldStop: () => shuttingDown,
	})

	process.exit(0)
})
