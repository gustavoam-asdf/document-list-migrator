import { Open as OpenZip } from "unzipper";

const remoteZipFile = "http://www2.sunat.gob.pe/padron_reducido_ruc.zip"
const localZipFile = "./list.zip"

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

console.log("Downloading file...");
const dataZipped = await fetch(remoteZipFile)

console.log("Writing file...");
await Bun.write(localZipFile, dataZipped)

console.log("Unzipping file...");
const directory = await OpenZip.file(localZipFile)
await directory.extract({ path: "./" })


const endTime = Date.now();

console.log(`Done in ${endTime - startTime}ms`);