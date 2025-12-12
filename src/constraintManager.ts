import type { Sql } from "postgres";

const FK_CONSTRAINT_NAME = "PersonaJuridica_codigoUbigeo_fkey";
const FK_TABLE = "PersonaJuridica";

async function constraintExists(sql: Sql, constraintName: string): Promise<boolean> {
	const result = await sql`
		SELECT 1 FROM information_schema.table_constraints 
		WHERE constraint_name = ${constraintName} 
		AND table_name = ${FK_TABLE}
		LIMIT 1
	`;
	return result.length > 0;
}

export async function dropConstraintSafely(sql: Sql, dbName: string, maxRetries = 3): Promise<void> {
	for (let attempt = 1; attempt <= maxRetries; attempt++) {
		try {
			const exists = await constraintExists(sql, FK_CONSTRAINT_NAME);
			if (!exists) {
				console.log(`[${dbName}] FK constraint already dropped or doesn't exist`);
				return;
			}

			await sql`ALTER TABLE ${sql(FK_TABLE)} DROP CONSTRAINT IF EXISTS ${sql(FK_CONSTRAINT_NAME)}`;
			console.log(`[${dbName}] Dropped FK constraint successfully`);
			return;
		} catch (error) {
			console.warn(`[${dbName}] Attempt ${attempt}/${maxRetries} to drop FK constraint failed:`, error);
			if (attempt === maxRetries) {
				console.error(`[${dbName}] Failed to drop FK constraint after ${maxRetries} attempts, continuing anyway...`);
			} else {
				await new Promise(resolve => setTimeout(resolve, 1000 * attempt)); // Exponential backoff
			}
		}
	}
}

export async function recreateConstraintSafely(sql: Sql, dbName: string, maxRetries = 3): Promise<void> {
	for (let attempt = 1; attempt <= maxRetries; attempt++) {
		try {
			const exists = await constraintExists(sql, FK_CONSTRAINT_NAME);
			if (exists) {
				console.log(`[${dbName}] FK constraint already exists, skipping recreation`);
				return;
			}

			await sql`
				ALTER TABLE ${sql(FK_TABLE)} 
				ADD CONSTRAINT ${sql(FK_CONSTRAINT_NAME)} 
				FOREIGN KEY ("codigoUbigeo") REFERENCES "Ubigeo"(codigo) 
				ON UPDATE CASCADE ON DELETE SET NULL
			`;
			console.log(`[${dbName}] Recreated FK constraint successfully`);
			return;
		} catch (error) {
			console.warn(`[${dbName}] Attempt ${attempt}/${maxRetries} to recreate FK constraint failed:`, error);
			if (attempt === maxRetries) {
				console.error(`[${dbName}] Failed to recreate FK constraint after ${maxRetries} attempts`);
				throw error; // Re-throw on final attempt since constraint recreation is important
			}
			await new Promise(resolve => setTimeout(resolve, 1000 * attempt)); // Exponential backoff
		}
	}
}
