import { Redis } from "ioredis";

const redis = new Redis({
	host: Bun.env.REDIS_HOST,
	port: Number(Bun.env.REDIS_PORT),
	password: Bun.env.REDIS_PASSWORD,
	db: Number(Bun.env.REDIS_DB),
	lazyConnect: true,
	showFriendlyErrorStack: true,
	tls: {},
})


redis.on("reconnecting", () => {
	console.warn("Reconnecting to Redis")
})

redis.on("error", error => {
	console.error("Redis error", error)
})

redis.on("connect", () => {
	console.info("Connected to Redis")
})

export { redis }