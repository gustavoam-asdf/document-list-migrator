const remoteZipFile = "http://www2.sunat.gob.pe/padron_reducido_ruc.zip"
const localZipFile = "./list.zip"

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

console.log("Downloading file...");
const dataZipped = await fetch(remoteZipFile)

console.log("Writing file...");
await Bun.write("./list.zip", dataZipped)

const endTime = Date.now();

console.log(`Done in ${endTime - startTime}ms`);