# Admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin_password

# Viewer user
airflow users create \
    --username viewer \
    --firstname Viewer \
    --lastname User \
    --role Viewer \
    --email viewer@example.com \
    --password viewer_password

# Editor user
airflow users create \
    --username editor \
    --firstname Editor \
    --lastname User \
    --role User \
    --email editor@example.com \
    --password editor_password
