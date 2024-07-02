// prevents TS errors
declare var self: Worker;

self.onmessage = (event: MessageEvent) => {
	console.log(`DNI worker: ${event.data}`);
};