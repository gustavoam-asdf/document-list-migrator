import { unzip } from "fflate";

const remoteZipFile = "http://www2.sunat.gob.pe/padron_reducido_ruc.zip"
const localZipFile = "./list.zip"
const localFile = "./list.txt"

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

console.log("Downloading file...");
const dataZipped = await fetch(remoteZipFile)
	.then(res => res.arrayBuffer())
	.then(data => new Uint8Array(data))

console.log("Unzipping file...");
unzip(dataZipped, { filter: (file) => file.name.endsWith(".txt") }, async (err, unzipped) => {
	if (err) {
		console.error(err);
		return;
	}

	console.log(unzipped);
	console.log("Writing file...");

	const file = unzipped[0];

	await Bun.write(localFile, file.buffer);

	const endTime = Date.now();

	console.log(`Done in ${endTime - startTime}ms`);
})

