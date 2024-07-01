import { $ } from "bun";

const remoteZipFile = "http://www2.sunat.gob.pe/padron_reducido_ruc.zip"
const filesDir = "./files"
const localZipFile = `${filesDir}/list.zip`
const localFile = `${filesDir}/padron_reducido_ruc.txt`

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

console.log("Downloading file...");
const dataZipped = await fetch(remoteZipFile)

console.log("Saving zip file...");
await Bun.write(localZipFile, dataZipped)

console.log("Unzipping file...");
await $`unzip ${localZipFile} -d ${filesDir}`

const endTime = Date.now();

console.log(`Done in ${endTime - startTime}ms`);

