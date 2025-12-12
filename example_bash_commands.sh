# 1️⃣ From the project root (where Dockerfile & docker-compose.yml live)
cd PokemonETL

# 2️⃣ Build the image (only needed the first time or after code changes)
docker compose down && docker compose build --no-cache && docker compose up --force-recreate --remove-orphans --abort-on-container-exit


# 3️⃣ Run the container – it will execute the script and then exit
docker compose up --abort-on-container-exit

# 4️⃣ Inspect the result
cat output/generations.json

# Make sure the host folder (./logs relative to the compose file) exists and is writable:
mkdir -p app/logs
chmod 775 app/logs


PROJECT=pokemonetl
docker compose -p $PROJECT down -v && \
docker container prune -f && \
docker network prune -f && \
docker rmi -f $(docker images --filter=reference="${PROJECT}*" -q) && \
docker compose -p $PROJECT build --no-cache && \
docker compose -p $PROJECT up -d