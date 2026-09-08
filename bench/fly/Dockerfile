# Build from the repo root:  fly deploy --dockerfile bench/fly/Dockerfile
FROM hexpm/elixir:1.17.3-erlang-27.1-debian-bookworm-20240926-slim AS build

ENV MIX_ENV=bench
RUN apt-get update && apt-get install -y --no-install-recommends build-essential git \
  && rm -rf /var/lib/apt/lists/*
RUN mix local.hex --force && mix local.rebar --force

WORKDIR /app
COPY mix.exs mix.lock ./
COPY rel ./rel
RUN mix deps.get --only bench
COPY lib ./lib
COPY bench/fly ./bench/fly
RUN mix compile
RUN mix release echo_pubsub

FROM debian:bookworm-slim AS app
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 openssl libncurses6 \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/_build/bench/rel/echo_pubsub ./
CMD ["/app/bin/echo_pubsub", "start"]
