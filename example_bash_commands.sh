# From the project root (where Dockerfile & docker-compose.yml live)
cd PokemonETL

# -------------------------------------------------------------------------
# SECTION: LOCAL ISSUES WITH WRITING IN PYCHARM
# -------------------------------------------------------------------------
# Make sure the host folder (./logs relative to the compose file) exists and is writable:
mkdir -p app/logs
chmod 775 app/logs

# -------------------------------------------------------------------------
# SECTION: NEED TO RESTART DOCKER
# -------------------------------------------------------------------------
# Build the image (only needed the first time or after code changes)
docker compose down && docker compose build --no-cache && docker compose up --force-recreate --remove-orphans --abort-on-container-exit


# -------------------------------------------------------------------------
# SECTION: DELETE AND START OVER IN DOCKER
# -------------------------------------------------------------------------
# get project names
docker compose ls


PROJECT=pokemonetl
docker compose -p $PROJECT down -v && \
docker container prune -f && \
docker network prune -f && \
docker rmi -f $(docker images --filter=reference="${PROJECT}*" -q) && \
docker compose -p $PROJECT build --no-cache && \
docker compose -p $PROJECT up -d