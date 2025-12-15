import os
import requests
from dotenv import load_dotenv

load_dotenv()

RIOT_API_KEY = os.getenv("RIOT_API_KEY")

url = "https://na1.api.riotgames.com/tft/league/v1/challenger"
headers = {"X-Riot-Token": RIOT_API_KEY}

response = requests.get(url, headers=headers)

print(f"Status: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    print(data['entries'])
else:
    print(f"Error: {response.text}")