import time
import random
from repository import AnimalRepository
from utils import transform_animal
from httpx import HTTPStatusError

class AnimalService:
    def __init__(self):
        self.repo = AnimalRepository()

    def load_animals(self):
        animals = self.repo.fetch_all_animals()
        batch = []
        for animal in animals:
            try:
                details = self.repo.fetch_animal_details(animal['id'])
                transformed = transform_animal(details)
                batch.append(transformed)

                if len(batch) == 100:
                    self._post_batch(batch)
                    batch = []

            except HTTPStatusError as e:
                print(f"Error fetching animal: {e}")
                self._handle_error(e)

        if batch:
            self._post_batch(batch)

    def _post_batch(self, batch):
        retries = 0
        while retries < RETRY_LIMIT:
            try:
                self.repo.post_animals(batch)
                print(f"Successfully posted batch of {len(batch)} animals.")
                break
            except HTTPStatusError as e:
                retries += 1
                print(f"Error posting batch: {e}")
                self._handle_error(e)

    def _handle_error(self, error):
        if error.status_code in [500, 502, 503, 504]:
            pause_time = random.uniform(5, 15)
            print(f"Server error {error.status_code}, pausing for {pause_time} seconds.")
            time.sleep(pause_time)
        else:
            raise error
