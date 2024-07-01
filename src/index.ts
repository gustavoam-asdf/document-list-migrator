import { fileCompressedUrl } from "./constants";

console.log("Downloading file...");
const dataZipped = await fetch(fileCompressedUrl)

console.log("Writing file...");
await Bun.write("./list.zip", dataZipped)

