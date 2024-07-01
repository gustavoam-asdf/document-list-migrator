import { Open as OpenZip } from "unzipper";
import { fileCompressedUrl } from "./constants";

const startTime = Date.now();

console.log("Downloading file...");
const dataZipped = await fetch(fileCompressedUrl)

console.log("Writing file...");
await Bun.write("./list.zip", dataZipped)

console.log("Unzipping file...");
const directory = await OpenZip.file("./list.zip")
await directory.extract({ path: "./" })


const endTime = Date.now();

console.log(`Done in ${endTime - startTime}ms`);