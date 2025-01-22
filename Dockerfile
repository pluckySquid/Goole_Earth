FROM python:3.12-slim-bullseye
# Using slim-bullseye helps match Debian 11 exactly. If you must keep python:3.12-slim (which is Debian 12),
# you can still try the unified approach below, but it may lead to other repository mismatches.

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# 1) Install *only* your non-ODBC, non-Microsoft packages here
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    gcc \
    g++ \
    dos2unix \
    netcat-openbsd \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# 2) Add Microsoft’s repo
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg \
 && echo "deb [arch=amd64] https://packages.microsoft.com/debian/11/prod bullseye main" \
    > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc unixodbc-dev \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . /app
RUN dos2unix /app/wait_for_it.sh
RUN chmod +x /app/wait_for_it.sh

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000
CMD ["bash", "-c", "./wait_for_it.sh sql_server:1433 -- python src/test.py && python src/db_to_kmz.py"]
