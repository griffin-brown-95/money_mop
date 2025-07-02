# Use the Astronomer-certified Airflow image
FROM quay.io/astronomer/astro-runtime:8.2.0

# Install OS-level packages (optional)
COPY packages.txt /packages.txt
RUN if [ -f /packages.txt ]; then apt-get update && xargs apt-get install -y < /packages.txt && apt-get clean; fi

# Copy Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs, plugins, include files
COPY dags/ /usr/local/airflow/dags/
COPY plugins/ /usr/local/airflow/plugins/
COPY include/ /usr/local/airflow/include/
