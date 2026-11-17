import { describe, expect, test } from "bun:test"

import { RateLimiter } from "../../src/shared/RateLimiter"

// Reloj falso: sleep avanza el tiempo y registra cuánto se esperó.
function fakeClock() {
	let now = 0
	const sleeps: number[] = []
	return {
		sleeps,
		advance: (ms: number) => { now += ms },
		clock: {
			now: () => now,
			sleep: async (ms: number) => {
				sleeps.push(ms)
				now += ms
			},
		},
	}
}

describe("RateLimiter", () => {
	test("espera lo necesario para no superar el ritmo promedio", async () => {
		const { clock, sleeps } = fakeClock()
		const limiter = new RateLimiter(1000, clock)

		await limiter.consume(500)
		await limiter.consume(500)

		expect(sleeps).toEqual([500, 500])
	})

	test("no espera si el trabajo ya fue más lento que el límite", async () => {
		const { clock, sleeps, advance } = fakeClock()
		const limiter = new RateLimiter(1000, clock)

		await limiter.consume(100)
		advance(500)
		await limiter.consume(100)

		expect(sleeps).toEqual([100])
	})

	test("el tiempo ocioso entre fases no se acumula como crédito para una ráfaga", async () => {
		const { clock, sleeps, advance } = fakeClock()
		const limiter = new RateLimiter(1000, clock)

		await limiter.consume(1000)
		advance(60_000)
		await limiter.consume(1000)

		expect(sleeps).toEqual([1000, 1000])
	})

	test("con límite 0 no frena", async () => {
		const { clock, sleeps } = fakeClock()

		await new RateLimiter(0, clock).consume(1_000_000)

		expect(sleeps).toEqual([])
	})
})
