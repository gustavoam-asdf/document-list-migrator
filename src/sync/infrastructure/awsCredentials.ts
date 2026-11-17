export type AwsCredentials = {
	accessKeyId: string
	secretAccessKey: string
	sessionToken: string | undefined
	expiresAt: Date | undefined
}

// En Fargate las credenciales del task role salen del endpoint de credenciales del
// contenedor; el cliente S3 de Bun solo lee las variables AWS_* estáticas.
const ECS_CREDENTIALS_HOST = "http://169.254.170.2"

export async function resolveAwsCredentials(): Promise<AwsCredentials> {
	const relativeUri = Bun.env.AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
	const url = relativeUri ? `${ECS_CREDENTIALS_HOST}${relativeUri}` : Bun.env.AWS_CONTAINER_CREDENTIALS_FULL_URI
	if (url) {
		const token = Bun.env.AWS_CONTAINER_AUTHORIZATION_TOKEN
		const res = await fetch(url, token ? { headers: { Authorization: token } } : undefined)
		if (!res.ok) {
			throw new Error(`credenciales ECS: HTTP ${res.status}`)
		}
		return parseContainerCredentials(await res.json())
	}

	const accessKeyId = Bun.env.AWS_ACCESS_KEY_ID
	const secretAccessKey = Bun.env.AWS_SECRET_ACCESS_KEY
	if (!accessKeyId || !secretAccessKey) {
		throw new Error("sin credenciales AWS: falta el task role o AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY")
	}
	return { accessKeyId, secretAccessKey, sessionToken: Bun.env.AWS_SESSION_TOKEN, expiresAt: undefined }
}

function parseContainerCredentials(body: unknown): AwsCredentials {
	if (typeof body !== "object" || body === null) {
		throw new Error("credenciales ECS: respuesta no es un objeto")
	}
	const field = (name: string): string | undefined => {
		const value: unknown = Reflect.get(body, name)
		return typeof value === "string" && value.length > 0 ? value : undefined
	}
	const accessKeyId = field("AccessKeyId")
	const secretAccessKey = field("SecretAccessKey")
	if (!accessKeyId || !secretAccessKey) {
		throw new Error("credenciales ECS: faltan AccessKeyId/SecretAccessKey")
	}
	const expiration = field("Expiration")
	return {
		accessKeyId,
		secretAccessKey,
		sessionToken: field("Token"),
		expiresAt: expiration ? new Date(expiration) : undefined,
	}
}
