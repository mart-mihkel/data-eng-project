# data-eng-project

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
echo -e "AVAANDMED_API_KEY=[your open data portal api-key]" > .env
echo -e "AVAANDMED_API_KEY_ID=[your open data portal api-key id]" > .env
docker compose up airflow-init
```
