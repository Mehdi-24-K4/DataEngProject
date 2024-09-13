import requests
import time

API_KEY = '625766118f3124ca6140bd0d9f7862bcd2a802ad05ebc8a239194ca8108e2308'

# Ajouter le paramètre 'limit' pour limiter à 10 résultats
url = "https://api.openaq.org/v2/latest"
params = {
    'sort': 'desc',
}
headers = {
    "X-API-Key": API_KEY, 
    "Cache-Control": "no-cache",
}
res = requests.get(url, headers=headers, params=params)
print('data:')
print(res.json())  # pour afficher le contenu en format JSON