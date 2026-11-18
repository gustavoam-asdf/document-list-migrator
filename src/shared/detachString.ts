// En JSC un substring (split, slice) apunta al string del que salió. Una línea guardada
// retiene así el chunk entero de lectura (512 KB en Linux, ×2 si es UTF-16): con cambios
// repartidos por todo ruc.tsv se retenía el archivo completo (3,4 GB en Fargate).
// La copia vía bytes no comparte memoria con el original.
export function detachString(value: string): string {
	return Buffer.from(value, "utf8").toString("utf8")
}
