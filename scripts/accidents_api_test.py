import os
import base64
import requests

API = "https://avaandmed.eesti.ee/api"
LOGIN = f"{API}/key-login"

DATASET_ID = "d43cbb24-f58f-4928-b7ed-1fcec2ef355b"
FILE_ID = "3c255d23-8fa7-479f-b4bb-9c8c636dbba9"

AVAANDMED_API_KEY = os.environ["AVAANDMED_API_KEY"]
AVAANDMED_API_KEY_ID = os.environ["AVAANDMED_API_KEY_ID"]

url = f"{API}/datasets/{DATASET_ID}/files/{FILE_ID}"
res = requests.get(url)
