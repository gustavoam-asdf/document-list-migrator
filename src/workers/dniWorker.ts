// prevents TS errors
declare var self: Worker;

self.onmessage = (event: MessageEvent) => {
	console.log(`DNI worker: ${event.data}`);

	self.postMessage("DNI worker done");
	process.exit(0);
};