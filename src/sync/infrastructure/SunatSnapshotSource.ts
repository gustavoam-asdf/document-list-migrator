import { $ } from "bun"
import fs from "node:fs/promises"

import type { PreparedSnapshot, SnapshotSource } from "../application/SnapshotSource"
import type { Entity } from "../domain/Entity"
import type { LineChunks } from "../domain/diffSortedSnapshots"
import { parseDniLine, parseRucLine, type ParseResult } from "../domain/parsePadronLine"
import { RejectsWriter } from "../../rejects/RejectsWriter"
import { fileLineChunks } from "../../stream/fileLineChunks"
import { KeyOrderTrackingWriter } from "./KeyOrderTrackingWriter"
import { readLineChunks } from "../../stream/readLineChunks"
import { streamToFile } from "../../stream/streamToFile"

type Config = {
	remoteZipUrl: string
	workDir: string
	rejectsDir: string
}

export class SunatSnapshotSource implements SnapshotSource {
	constructor(private readonly config: Config) {}

	async prepare(id: string): Promise<PreparedSnapshot> {
		const dir = `${this.config.workDir}/${id}`
		await fs.rm(this.config.workDir, { recursive: true, force: true })
		await fs.mkdir(dir, { recursive: true })
		await fs.mkdir(this.config.rejectsDir, { recursive: true })

		const sourceFile = await this.#download(dir)
		// SUNAT suele entregar el padrón ordenado por RUC: entonces no hace falta sort.
		const writers: Record<Entity, KeyOrderTrackingWriter> = {
			dni: new KeyOrderTrackingWriter(`${dir}/dni.raw.tsv`),
			ruc: new KeyOrderTrackingWriter(`${dir}/ruc.raw.tsv`),
		}
		const rejects: Record<Entity, RejectsWriter> = {
			dni: new RejectsWriter(`${this.config.rejectsDir}/parse-dni.jsonl`),
			ruc: new RejectsWriter(`${this.config.rejectsDir}/parse-ruc.jsonl`),
		}

		console.log("[snapshot] procesando padrón")
		let sourceLine = 0
		let headerSkipped = false
		const route = (entity: Entity, result: ParseResult, line: string) => {
			if (result.kind === "row") {
				writers[entity].write(result.row)
			} else if (result.kind === "reject") {
				rejects[entity].write({ source: `parse-${entity}`, rawLine: line, error: result.reason, sourceLine })
			}
		}
		for await (const lines of readLineChunks(Bun.file(sourceFile).stream(), "latin1")) {
			for (const line of lines) {
				sourceLine++
				if (!headerSkipped && line.startsWith("RUC")) {
					headerSkipped = true
					continue
				}
				route("dni", parseDniLine(line), line)
				route("ruc", parseRucLine(line), line)
			}
		}
		await fs.rm(sourceFile)

		const files: Record<Entity, string> = { dni: `${dir}/dni.tsv`, ruc: `${dir}/ruc.tsv` }
		// En paralelo: cada sort usa hasta MIGRATOR_SORT_BUFFER de memoria (2 x 512M por defecto).
		await Promise.all(
			(["dni", "ruc"] as const).map(async entity => {
				const start = Date.now()
				const { sortedExternally } = await writers[entity].finish(files[entity], `${dir}/tmp-${entity}`)
				if (sortedExternally) {
					console.log(`[snapshot] ${entity} no venía ordenado: sort externo en ${Date.now() - start}ms`)
				}
			}),
		)

		const [dniRejects, rucRejects] = await Promise.all([rejects.dni.close(), rejects.ruc.close()])
		console.log(
			`[snapshot] ${id} listo | líneas=${sourceLine} | dni=${writers.dni.rows} (rechazos ${dniRejects}) ` +
			`| ruc=${writers.ruc.rows} (rechazos ${rucRejects})`,
		)
		return {
			id,
			files,
			rows: { dni: writers.dni.rows, ruc: writers.ruc.rows },
			parseRejects: { dni: dniRejects, ruc: rucRejects },
		}
	}

	read(snapshot: PreparedSnapshot, entity: Entity): LineChunks {
		return fileLineChunks(snapshot.files[entity])
	}

	async #download(dir: string): Promise<string> {
		const zipFile = `${dir}/padron.zip`
		console.log("[snapshot] descargando padrón")
		const response = await fetch(this.config.remoteZipUrl)
		if (!response.ok || !response.body) {
			throw new Error(`descarga del padrón: HTTP ${response.status}`)
		}
		const bytes = await streamToFile(response.body, zipFile)
		console.log(`[snapshot] padrón descargado: ${(bytes / 1024 / 1024).toFixed(0)} MB`)

		console.log("[snapshot] descomprimiendo")
		await $`unzip -o ${zipFile} -d ${dir}`.quiet()
		await fs.rm(zipFile)

		const extracted = (await fs.readdir(dir)).filter(name => name.endsWith(".txt"))
		const [txt] = extracted
		if (extracted.length !== 1 || txt === undefined) {
			throw new Error(`el zip del padrón trae ${extracted.length} archivos .txt, se esperaba 1`)
		}
		return `${dir}/${txt}`
	}
}
