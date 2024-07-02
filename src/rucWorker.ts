// prevents TS errors
declare var self: Worker;

self.onmessage = (event: MessageEvent) => {
	console.log(`RUC worker: ${event.data}`);
};