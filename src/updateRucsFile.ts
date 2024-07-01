import { filesDir, localZipFile, remoteZipFile } from "./constants";

import { $ } from "bun";
import fs from "node:fs/promises";

export async function updateRucsFile() {
	console.log("Cleaning files directory...");
	await fs.rmdir(filesDir, { recursive: true })
	await fs.mkdir(filesDir, { recursive: true })

	console.log("Downloading file...");
	const dataZipped = await fetch(remoteZipFile)

	console.log("Saving zip file...");
	await Bun.write(localZipFile, dataZipped)

	console.log("Unzipping file...");
	await $`unzip ${localZipFile} -d ${filesDir} > /dev/null`
}