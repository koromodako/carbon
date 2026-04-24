# =============================================================================
# FUSION DIRECTIVES
# =============================================================================
# fusion:image=cert-edf/carbon-be
# fusion:package=edf-fusion
# fusion:package=edf-carbon-core
# fusion:package=edf-carbon-server
# =============================================================================
# BUILD IMAGE
# =============================================================================
FROM python:3.12-alpine AS build
# -----------------------------------------------------------------------------
# SETUP VENV
# -----------------------------------------------------------------------------
RUN apk add build-base linux-headers && \
    python3 -m venv /venv && \
    /venv/bin/python -m pip install uv && \
    /venv/bin/uv pip install --python /venv/bin/python argon2-cffi python-keycloak
# -----------------------------------------------------------------------------
# SETUP FUSION PACKAGES (PROD)
# -----------------------------------------------------------------------------
RUN /venv/bin/uv pip install --python /venv/bin/python edf-carbon-server
# -----------------------------------------------------------------------------
# SETUP FUSION PACKAGES (DEV)
# -----------------------------------------------------------------------------
#COPY pkg /pkg
#RUN /venv/bin/uv pip install --python /venv/bin/python --pre --find-links /pkg edf-carbon-server
# =============================================================================
# PRODUCTION IMAGE
# =============================================================================
FROM python:3.12-alpine AS production
# -----------------------------------------------------------------------------
# COPY TEMPLATES
# -----------------------------------------------------------------------------
COPY tpl /tpl
# -----------------------------------------------------------------------------
# COPY FROM BUILD IMAGE
# -----------------------------------------------------------------------------
COPY --from=build /venv /venv
# -----------------------------------------------------------------------------
# ENTRYPOINT
# -----------------------------------------------------------------------------
COPY --chmod=755 docker-entrypoint.sh /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
