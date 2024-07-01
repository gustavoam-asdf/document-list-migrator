import { $ } from "bun";
import fs from "node:fs/promises";

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

const remoteZipFile = "http://www2.sunat.gob.pe/padron_reducido_ruc.zip"
const rootDir = process.cwd()
const filesDir = `${rootDir}/files`

console.log("Cleaning files directory...");
await fs.rmdir(filesDir, { recursive: true })
await fs.mkdir(filesDir, { recursive: true })

const localZipFile = `${filesDir}/list.zip`
const localFile = `${filesDir}/padron_reducido_ruc.txt`

console.log("Downloading file...");
const dataZipped = await fetch(remoteZipFile)

console.log("Saving zip file...");
await Bun.write(localZipFile, dataZipped)

console.log("Unzipping file...");
await $`unzip ${localZipFile} -d ${filesDir} > /dev/null`

const endTime = Date.now();

console.log(`Done in ${endTime - startTime}ms`);

