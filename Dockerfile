# use the official Bun image
# see all versions at https://hub.docker.com/r/oven/bun/tags
FROM oven/bun:1-debian AS base
WORKDIR /app

# install dependencies into temp directory
# this will cache them and speed up future builds
FROM base AS install
RUN mkdir -p /temp/dev
COPY package.json bun.lock /temp/dev/
RUN cd /temp/dev && bun install --frozen-lockfile

# install with --production (exclude devDependencies)
RUN mkdir -p /temp/prod
COPY package.json bun.lock /temp/prod/
RUN cd /temp/prod && bun install --frozen-lockfile --production

# copy node_modules from temp directory
# then copy all (non-ignored) project files into the image
FROM base AS prerelease
COPY --from=install /temp/dev/node_modules node_modules
COPY . .

# # [optional] tests & build
# ENV NODE_ENV=production
# RUN bun test
# RUN bun run build

# copy production dependencies and source code into final image
FROM base AS release

RUN apt-get update
RUN apt-get install unzip -y

RUN groupadd --system --gid 1001 migrator-user
RUN useradd --system --uid 1001 --gid migrator-user --no-create-home migrator-user

COPY --chown=migrator-user:migrator-user --from=install /temp/prod/node_modules node_modules
COPY --chown=migrator-user:migrator-user --from=prerelease /app/src ./src
COPY --chown=migrator-user:migrator-user --from=prerelease /app/package.json .

RUN mkdir -p /app/files
RUN chown -R migrator-user:migrator-user /app/files

# run the app
USER migrator-user

EXPOSE 3000/tcp
ENTRYPOINT bun start