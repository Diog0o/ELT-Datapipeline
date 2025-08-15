FROM python:3.8-slim

# Install dependencies and PostgreSQL client 17
RUN apt-get update \
    && apt-get install -y wget gnupg lsb-release \
    && echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update \
    && apt-get install -y postgresql-client-17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set workdir
WORKDIR /app

# Copy your ELT script and other files
COPY elt_script.py .

# Install any Python dependencies (if you have a requirements.txt)
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# Default command
CMD ["python", "elt_script.py"]
