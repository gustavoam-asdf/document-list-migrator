import { describe, expect, test } from "bun:test"

import { escapeCopyField } from "../../src/copy/escapeCopyField"
import { unescapeCopyField } from "../../src/copy/unescapeCopyField"

describe("unescapeCopyField", () => {
	test("recupera el valor original de un campo escapado para COPY", () => {
		const original = "A\\B\tC\nD\rE\bF\fG\vH"

		expect(unescapeCopyField(escapeCopyField(original))).toBe(original)
	})

	test("deja intactas las claves numéricas", () => {
		expect(unescapeCopyField("20123456789")).toBe("20123456789")
	})
})
