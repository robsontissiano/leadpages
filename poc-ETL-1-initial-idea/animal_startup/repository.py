import httpx
from models import AnimalDetail
from config import API_BASE_URL, MAX_BATCH_SIZE, RETRY_LIMIT

class AnimalRepository:
    def __init__(self):
        self.client = httpx.Client(timeout=30)

    def fetch_all_animals(self):
        animals = []
        page = 1
        while True:
            response = self.client.get(f"{API_BASE_URL}/animals/v1/animals", params={"page": page})
            # print("###################")
            # print(f"{API_BASE_URL}/animals/v1/animals")
            response.raise_for_status()  # Handle non-2xx responses
            data = response.json()
            animals.extend(data['items'])

            # trying to get the next page in the pagination
            # if not data['next']:
            #     break
            page += 1
        return animals

    def fetch_animal_details(self, animal_id: int):
        response = self.client.get(f"{API_BASE_URL}/animals/v1/animals/{animal_id}")
        response.raise_for_status()
        return AnimalDetail(**response.json())

    def post_animals(self, animals):
        response = self.client.post(f"{API_BASE_URL}/animals/v1/home", json=animals)
        response.raise_for_status()
        print(response.json)
        return response.json()
