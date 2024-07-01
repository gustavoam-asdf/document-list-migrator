import { updateRucsFile } from "./updateRucsFile";

const startTime = Date.now();

const pid = process.pid;
console.log(`PID: ${pid}`);

await updateRucsFile();

const endTime = Date.now();

console.log(`Done in ${endTime - startTime}ms`);
