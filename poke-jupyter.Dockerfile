# -------------------------------------------------------------
# Choose a base image that already contains Python.
# -------------------------------------------------------------
FROM python:3.12-slim-bookworm

# -------------------------------------------------------------
# Install OS‑level packages that some wheels need.
# ----------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------------
# Create a non‑root user (good practice for containers)
# ------------------------------------------------------------
ARG USERNAME=appuser
ARG UID=1000
ARG GID=1000
RUN groupadd -g ${GID} ${USERNAME} && \
    useradd -m -u ${UID} -g ${GID} -s /bin/bash ${USERNAME}

# ------------------------------------------------------------
# Copy the source tree into /app and install deps
# -------------------------------------------------------------
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /app/logs && chown -R 1000:1000 /app/logs

# Copy the actual Python package (the `app/` folder)
COPY app/ /app
COPY .env /app
COPY logging_config.yaml /app

# -------------------------------------------------------------
# Switch to the non‑root user
# ----------------------------------------------------------
USER ${USERNAME}

# should help with unbuffered output,
ENV PYTHONUNBUFFERED=1

# -------------------------------------------------------------
# Default command – can be overridden in compose if you like
# -----------------------------------------------
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser", "--allow-root", "--NotebookApp.token=''"]