// Huella de una publicación del padrón (el ETag del zip de SUNAT). Sin huella no se
// puede afirmar que el padrón no cambió, y la corrida se hace completa.
export type SourceVersion =
	| { kind: "known"; tag: string }
	| { kind: "unknown" }

export function isSamePublication(a: SourceVersion, b: SourceVersion): boolean {
	return a.kind === "known" && b.kind === "known" && a.tag === b.tag
}
