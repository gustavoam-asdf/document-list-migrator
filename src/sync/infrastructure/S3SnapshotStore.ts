import { S3Client } from "bun"
import fs from "node:fs/promises"

import { ENTITIES, type Entity } from "../domain/Entity"
import type { LineChunks } from "../domain/diffSortedSnapshots"
import type { PreparedSnapshot } from "../application/SnapshotSource"
import type { SnapshotStore } from "../application/SnapshotStore"
import { fileLineChunks } from "../../stream/fileLineChunks"
import { streamToFile } from "../../stream/streamToFile"
import { resolveAwsCredentials, type AwsCredentials } from "./awsCredentials"

type Config = {
	bucket: string
	region: string
	prefix: string
	endpoint: string | undefined
	// Copia local de los snapshots leídos: el diff no depende de un stream HTTP que
	// queda pausado mientras se escriben lotes en la BD.
	cacheDir: string
}

const PART_SIZE = 64 * 1024 * 1024
const UPLOAD_QUEUE = 4
const UPLOAD_RETRIES = 3
const CREDENTIALS_REFRESH_MARGIN_MS = 10 * 60 * 1000

// Layout: <prefix>/<id>/{dni,ruc}.tsv + manifest.json. El manifest se escribe al
// final: su existencia es lo que marca un snapshot como completo.
export class S3SnapshotStore implements SnapshotStore {
	#client: S3Client | undefined
	#credentials: AwsCredentials | undefined

	constructor(private readonly config: Config) {}

	async exists(id: string): Promise<boolean> {
		const client = await this.#getClient()
		return client.file(this.#manifestKey(id)).exists()
	}

	read(id: string, entity: Entity): LineChunks {
		const download = () => this.#download(id, entity)
		return {
			async *[Symbol.asyncIterator]() {
				yield* fileLineChunks(await download())
			},
		}
	}

	async save(snapshot: PreparedSnapshot): Promise<void> {
		const client = await this.#getClient()
		await Promise.all(
			ENTITIES.map(entity =>
				client.write(this.#dataKey(snapshot.id, entity), Bun.file(snapshot.files[entity]), {
					partSize: PART_SIZE,
					queueSize: UPLOAD_QUEUE,
					retry: UPLOAD_RETRIES,
					type: "text/tab-separated-values",
				}),
			),
		)
		const manifest = { id: snapshot.id, rows: snapshot.rows, savedAt: new Date().toISOString() }
		await client.write(this.#manifestKey(snapshot.id), JSON.stringify(manifest), { type: "application/json" })
	}

	async pruneExcept(keep: ReadonlySet<string>): Promise<void> {
		const client = await this.#getClient()
		const root = `${this.config.prefix}/`
		const stale: string[] = []
		let continuationToken: string | undefined
		do {
			const page = await client.list({ prefix: root, continuationToken })
			for (const object of page.contents ?? []) {
				const id = object.key.slice(root.length).split("/")[0]
				if (id && !keep.has(id)) {
					stale.push(object.key)
				}
			}
			continuationToken = page.isTruncated ? page.nextContinuationToken : undefined
		} while (continuationToken)

		// El manifest primero: si se corta a mitad, el snapshot ya no cuenta como completo.
		stale.sort((a, b) => Number(b.endsWith("manifest.json")) - Number(a.endsWith("manifest.json")))
		for (const key of stale) {
			await client.delete(key)
		}
	}

	async #download(id: string, entity: Entity): Promise<string> {
		const local = `${this.config.cacheDir}/${id}/${entity}.tsv`
		if (await Bun.file(local).exists()) {
			return local
		}
		await fs.mkdir(`${this.config.cacheDir}/${id}`, { recursive: true })
		const client = await this.#getClient()
		const partial = `${local}.partial`
		const start = Date.now()
		await streamToFile(client.file(this.#dataKey(id, entity)).stream(), partial)
		await fs.rename(partial, local)
		console.log(`[s3] ${id}/${entity}.tsv descargado en ${Date.now() - start}ms`)
		return local
	}

	#dataKey(id: string, entity: Entity): string {
		return `${this.config.prefix}/${id}/${entity}.tsv`
	}

	#manifestKey(id: string): string {
		return `${this.config.prefix}/${id}/manifest.json`
	}

	async #getClient(): Promise<S3Client> {
		const expiresAt = this.#credentials?.expiresAt
		const expiring = expiresAt !== undefined && expiresAt.getTime() - Date.now() < CREDENTIALS_REFRESH_MARGIN_MS
		if (!this.#client || expiring) {
			const credentials = await resolveAwsCredentials()
			this.#credentials = credentials
			this.#client = new S3Client({
				bucket: this.config.bucket,
				region: this.config.region,
				endpoint: this.config.endpoint,
				accessKeyId: credentials.accessKeyId,
				secretAccessKey: credentials.secretAccessKey,
				sessionToken: credentials.sessionToken,
			})
		}
		return this.#client
	}
}
