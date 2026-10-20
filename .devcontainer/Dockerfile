# syntax=docker/dockerfile:1.7
FROM ghcr.io/astral-sh/uv:0.13.4 AS uv

FROM python:3.14.7-slim

ARG USERNAME=dev
ARG USER_UID=1000

ENV UV_PROJECT_ENVIRONMENT=/usr/local \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/home/${USERNAME}/.dagster

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    && . /etc/os-release \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    gh git less openssh-client postgresql-client-18 procps sudo \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /uvx /usr/local/bin/

RUN groupadd --gid "${USER_UID}" "${USERNAME}" \
    && useradd --uid "${USER_UID}" --gid "${USER_UID}" --create-home --shell /bin/bash "${USERNAME}" \
    && chown -R "${USERNAME}:${USERNAME}" /usr/local \
    && echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" > "/etc/sudoers.d/${USERNAME}" \
    && chmod 0440 "/etc/sudoers.d/${USERNAME}"

USER ${USERNAME}

# Created here so the volumes mounted over them are owned by the dev user.
RUN mkdir -p "/home/${USERNAME}/.cache/uv" "/home/${USERNAME}/.config/gh"

WORKDIR /workspace
