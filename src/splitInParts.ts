
type Params<T> = {
	values: T[]
	size: number
}

export function splitInParts<T>({ values, size }: Params<T>) {
	const parts: T[][] = []

	for (let i = 0; i < values.length; i += size) {
		const startIndex = i
		const endIndex = i + size

		const part = values.slice(startIndex, endIndex)

		parts.push(part)
	}

	return parts
}