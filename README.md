# scrape-baby-products

## Steps for Project Initialization

### 1. Initializing the Prefect Webserver
First, initialize the Prefect webserver. This setup includes a PostgreSQL database in the same Docker Compose, which acts as its backend. Note that the scraped data will also be saved in a separate PostgreSQL database on the host machine. This setup can easily be transferred to a cloud instance.

### 2. Enabling Container-to-Host Communication
To enable the containers performing the scraping to communicate with the PostgreSQL instance on the host, you need to open external ports in Docker Compose. Add this line to your configuration:

"extra_hosts:
    - "host.docker.internal:172.17.0.1"


### 3. Environment Variables
For this project, since everything is containerized and managed through Docker Compose, the only environment variables needed are the usernames and passwords (our secrets). These have been defined in the Compose file, and they are also set on the host machine of the application.

### 4. Starting the Containers
- Build the custom Prefect Server image with the required requirements:`sudo docker compose --profile server up -d`
- Access the Prefect CLI using this command: `sudo docker compose run cli`
- Deploy your first flow: `python3 prefect_flow.py`
