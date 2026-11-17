import { describe, expect, test } from "bun:test"

import { parseDniLine, parseRucLine } from "../../../src/sync/domain/parsePadronLine"

const padronLine = (ruc: string, nombre: string, address = "150101|AV.|LIMA|-|-|123|-|-|-|-|-") =>
	`${ruc}|${nombre}|ACTIVO|HABIDO|${address}|`

describe("parseDniLine", () => {
	test("extrae el DNI de un RUC de persona natural (10 + DNI + dígito verificador)", () => {
		expect(parseDniLine(padronLine("10456789012", "PEREZ  GOMEZ JUAN"))).toEqual({
			kind: "row",
			row: "45678901\tPEREZ GOMEZ JUAN",
		})
	})

	test("ignora los RUC que no son de persona natural", () => {
		expect(parseDniLine(padronLine("20123456789", "EMPRESA SAC"))).toEqual({ kind: "skip" })
	})

	test("ignora las filas anuladas por error de SUNAT", () => {
		expect(parseDniLine("10456789012|ANULACION - ERROR SU|...")).toEqual({ kind: "skip" })
	})
})

describe("parseRucLine", () => {
	test("persona jurídica: campos con solo guiones quedan NULL y el ubigeo va al final", () => {
		const result = parseRucLine(padronLine("20123456789", "EMPRESA SAC"))

		expect(result).toEqual({
			kind: "row",
			row: "20123456789\tEMPRESA SAC\tACTIVO\tHABIDO\tAV.\tLIMA\t\\N\t\\N\t123\t\\N\t\\N\t\\N\t\\N\t\\N\t150101",
		})
	})

	test("persona natural (RUC 10): toda la dirección va NULL", () => {
		const result = parseRucLine(padronLine("10456789012", "PEREZ GOMEZ JUAN"))

		expect(result).toEqual({
			kind: "row",
			row: "10456789012\tPEREZ GOMEZ JUAN\tACTIVO\tHABIDO" + "\t\\N".repeat(11),
		})
	})

	test("rechaza filas sin razón social", () => {
		expect(parseRucLine("20123456789|-|ACTIVO|HABIDO|")).toEqual({ kind: "reject", reason: "empty razonSocial" })
	})
})
