export const remoteZipFile = "http://www2.sunat.gob.pe/padron_reducido_ruc.zip"
export const rootDir = process.cwd()
export const filesDir = `${rootDir}/files`
export const localZipFile = `${filesDir}/list.zip`
export const localFile = `${filesDir}/padron_reducido_ruc.txt`

export const dnisDir = `${filesDir}/dnis`
export const rucsDir = `${filesDir}/rucs`
export const CHUNK_SIZE = parseInt(Bun.env.CHUNK_SIZE || '5000000', 10) // Default: 5M lines per file/worker