FROM apache/airflow:2.10.2

# Add the requirements file
ADD requirements.txt .

# Install required Python packages
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

# Copy the shell script to the container
COPY dags/scripts/create_airflow_users.sh /opt/airflow/dags/scripts/create_airflow_users.sh

# Make the script executable
RUN chmod +x /opt/airflow/dags/scripts/create_airflow_users.sh

# Define the startup commands
CMD bash -c "airflow db upgrade && airflow webserver & sleep 10 && /opt/airflow/dags/scripts/create_airflow_users.sh && wait"
