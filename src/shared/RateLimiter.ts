type Clock = {
	now: () => number
	sleep: (ms: number) => Promise<void>
}

const realClock: Clock = { now: () => Date.now(), sleep: ms => Bun.sleep(ms) }

// Tras una pausa más larga que esto (p. ej. entre fases) la ventana se reinicia: el
// tiempo ocioso no se acumula como crédito para una ráfaga posterior.
const IDLE_RESET_MS = 1000

// Acota el ritmo promedio a `maxPerSecond` unidades (filas). Compartido entre llamadores
// concurrentes, el tope vale para el total. 0 o negativo = sin límite.
export class RateLimiter {
	#windowStart = 0
	#unitsInWindow = 0
	#lastConsume = 0

	constructor(
		private readonly maxPerSecond: number,
		private readonly clock: Clock = realClock,
	) {}

	async consume(units: number): Promise<void> {
		if (this.maxPerSecond <= 0 || units <= 0) {
			return
		}
		const now = this.clock.now()
		if (this.#unitsInWindow === 0 || now - this.#lastConsume > IDLE_RESET_MS) {
			this.#windowStart = now
			this.#unitsInWindow = 0
		}
		this.#unitsInWindow += units
		const earliest = this.#windowStart + (this.#unitsInWindow / this.maxPerSecond) * 1000
		const wait = earliest - now
		if (wait > 0) {
			await this.clock.sleep(wait)
		}
		this.#lastConsume = this.clock.now()
	}
}
