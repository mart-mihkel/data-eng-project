FROM apache/airflow:2.10.2

# Add the requirements file
ADD requirements.txt .

# Install required Python packages
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

# Copy the user creation script from /dags into the container
COPY dags/create_airflow_users.py /opt/airflow/dags/create_airflow_users.py

# Set the entrypoint to initialize Airflow and run the user creation script
CMD ["bash", "-c", "airflow db upgrade && airflow webserver & sleep 10 && python /opt/airflow/dags/create_airflow_users.py && wait"]


