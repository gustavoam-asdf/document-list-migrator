# Ubigeo: fuente, decisiones y mantenimiento

Contexto para no repetir la investigación (fue larga). Resume de dónde sale el
catálogo de ubigeo, por qué, y cómo mantenerlo.

## Qué es y por qué el FK está dropeado

`PersonaJuridica.codigoUbigeo` viene del **campo #5 del padrón RUC de SUNAT** —
es un código de ubigeo de 6 dígitos. La tabla `Ubigeo` (código → región/provincia/
distrito) es una **tabla de consulta solo para mostrar el nombre**: el app la carga
en memoria (`ubigeoMap` en el api) y hace lookup; **no usa un join de Prisma**.

El **FK `PersonaJuridica_codigoUbigeo_fkey` está dropeado** a propósito
(migración `drop_ubigeo_fk` en document-list-api). Razón: ninguna referencia
externa de nombres es un superset perfecto en tiempo real de los códigos que SUNAT
ya usa. Con FK estricto, un código no listado tumbaba toda la carga de 18M (o había
que nulearlo). Sin FK, la persona **conserva su código SUNAT real** y el app
**muestra el distrito si está en `Ubigeo`, y si no, no lo muestra**. Simple y robusto.

## Fuente: INEI SISCONCODE (pública, alcanzable desde AWS)

`src/ubigeo.ts` descarga el catálogo de **INEI SISCONCODE**:

- Base: `https://webapp.inei.gob.pe:8443/sisconcode` — **pública, sin login**.
- **Verificado que responde desde AWS/Fargate** (a diferencia de SEACE, que da 403
  a IPs cloud). Cert TLS no verificable en :8443 → se **salta la verificación**
  (`fetch(..., { tls: { rejectUnauthorized: false } })`). HTML en **ISO-8859-1** →
  se decodifica con `TextDecoder("latin1")`.
- **Autodetecta la versión más reciente** (Paso 1, `loadModuloBusqueda.htm`); hoy
  2025 = `8-1`. No hardcodear.
- **Un solo request** trae todo el árbol (deptos 2 díg, provincias 4 díg, distritos
  6 díg) como líneas `CODE Nombre`. Se parsean y arman las filas.
- Los nombres se **pasan a mayúsculas** para matchear la convención de la tabla.

## Los 9 códigos SUNAT divergentes (suplemento hardcodeado)

INEI y SUNAT usan la misma numeración **excepto 9 distritos** que INEI **renumeró**
al crear provincias nuevas (Loreto/Huancavelica). El padrón usa los códigos SUNAT
viejos, así que van hardcodeados en `SUNAT_DIVERGENT` (`src/ubigeo.ts`) con los
nombres de SUNAT. Son históricos y estables:

| SUNAT (padrón) | INEI actual | Distrito |
|---|---|---|
| 090708 | 090119 | HUANDO (Tayacaja, Huancavelica) |
| 160109 | 160801 | PUTUMAYO (Maynas, Loreto) |
| 160111 | 160511 | YAQUERANA (Maynas) |
| 160114 | 160803 | TENIENTE MANUEL CLAVERO (Maynas) |
| 160203 | 160701 | BARRANCA (Alto Amazonas) |
| 160204 | 160702 | CAHUAPANAS (Alto Amazonas) |
| 160207 | 160703 | MANSERICHE (Alto Amazonas) |
| 160208 | 160704 | MORONA (Alto Amazonas) |
| 160209 | 160705 | PASTAZA (Alto Amazonas) |

Resultado: INEI (~1892) + 9 suplemento = ~1901 filas, cubriendo todos los códigos
del padrón.

## Cadencia de refresco

El **migrador** refresca `Ubigeo` (upsert desde INEI + suplemento):

- **Domingos** (el job corre 05:00 UTC) → refresco semanal.
- **Bootstrap** (tabla vacía) → primer llenado.
- Otros días: se deja como está (no se golpea INEI a diario).

Es **best-effort**: si el fetch de INEI falla, se loguea y la carga de personas
continúa con lo que ya hay en la tabla (el FK está dropeado, así que no rompe nada).

## Fuentes descartadas (para no volver a probarlas)

- **SEACE** (OSCE): tenía el catálogo pero con nombres distintos/erróneos
  (130112=VIRU vs SUNAT ALTO TRUJILLO) y **bloquea IPs de AWS (403)**. Descartada.
- **SUNAT SOL Form 3128 (`di[]`)**: catálogo actual y autoritativo, pero **detrás
  de login SOL** → inviable para automatizar desde el cloud.
- **Archivos públicos SUNAT** (`ubigeo.js` 2008, XLS 2010, PDF 2015): congelados,
  les faltan los distritos nuevos. Descartados.
- **CSV comunidad (GitHub MichaelSuarez0/ubigeos_peru)**: útil como cross-check,
  pero es un espejo de INEI 1 versión atrás; mejor la fuente oficial (SISCONCODE).

## Mantenimiento (raro)

Solo cuando SUNAT/INEI crean un **distrito nuevo**:

1. El refresco semanal ya trae los nuevos de INEI automáticamente. No haces nada.
2. Si el nuevo distrito tiene código **divergente** SUNAT≠INEI (raro, solo al crear
   provincias), agrégalo a `SUNAT_DIVERGENT` en `src/ubigeo.ts`, rebuild + deploy.
   Sabrás que pasó si ves personas sin distrito en un código nuevo.
