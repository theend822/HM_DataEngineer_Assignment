# FROM ghcr.io/dbt-labs/dbt-postgres:1.9.latest

# WORKDIR /opt/dbt

# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# COPY . .

# # RUN apt-get update && apt-get install -y coreutils
# ENTRYPOINT []
# # you're essentially telling the container to watch for new content in a file that never changes and never has any content
# # if no task to run, container will shut down immediately. however, we will manually trigger transformation task in airflow and the timing is not fixed. therefore, need the container to be up and runnig
# CMD ["tail", "-f", "/dev/null"]