import httpx
import asyncio
import datetime
import logging
from typing import List, Optional
from pydantic import BaseModel
import time
import random

# Setup logging
logging.basicConfig(filename='animal_loader.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BASE_URL = 'http://localhost:3123/animals/v1'
MAX_BATCH_SIZE = 100
RETRY_WAIT_TIME = [3, 30]  # Random delay between 5 and 15 seconds for retries
MAX_RETRIES = 5  # Maximum retry attempts for 500-504 errors

# Pydantic models to structure the animal data
class AnimalDetail(BaseModel):
    id: int
    name: str
    born_at: Optional[int]
    friends: Optional[str]

class TransformedAnimal(BaseModel):
    id: int
    name: str
    born_at: Optional[datetime.datetime]
    friends: List[str]

# Function to fetch animal data (paginated)
async def fetch_animals():
    page = 1
    animals = []
    async with httpx.AsyncClient() as client:
        while True:
            logging.info(f"Fetching animals on page {page}")
            response = await client.get(f"{BASE_URL}/animals", params={'page': page})
            response.raise_for_status()
            data = response.json()

            # Append animals from current page
            animals.extend(data['items'])

            # Break if all pages are fetched
            if page >= data['total_pages']:
                break
            page += 1

    return animals

# Function to fetch detailed animal data by ID
async def fetch_animal_detail(animal_id: int):
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/animals/{animal_id}")
        response.raise_for_status()
        return AnimalDetail(**response.json())

# Transform the animal data
def transform_animal(animal_detail: AnimalDetail) -> TransformedAnimal:
    # Transform friends into a list
    friends_list = animal_detail.friends.split(",") if animal_detail.friends else []

    # Convert born_at to ISO8601 UTC
    if animal_detail.born_at:
        born_at_utc = datetime.datetime.utcfromtimestamp(animal_detail.born_at / 1000).isoformat() + "Z"
    else:
        born_at_utc = None

    return TransformedAnimal(
        id=animal_detail.id,
        name=animal_detail.name,
        born_at=born_at_utc,
        friends=friends_list
    )

# Function to post batch of animals
async def post_animals_batch(animals_batch: List[TransformedAnimal]):
    async with httpx.AsyncClient() as client:
        retries = 0
        while retries < MAX_RETRIES:
            try:
                logging.info(f"Posting batch of {len(animals_batch)} animals")
                response = await client.post(f"{BASE_URL}/home", json=[animal.dict() for animal in animals_batch])
                response.raise_for_status()
                logging.info(f"Successfully posted batch: {response.json()}")
                return
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                if response.status_code in [500, 502, 503, 504]:
                    retries += 1
                    wait_time = random.randint(RETRY_WAIT_TIME[0], RETRY_WAIT_TIME[1])
                    logging.warning(f"Server error ({exc}). Retrying in {wait_time} seconds (Attempt {retries}/{MAX_RETRIES})")
                    await asyncio.sleep(wait_time)
                else:
                    logging.error(f"Failed to post batch after {retries} retries: {exc}")
                    return

# Main function to orchestrate the ETL process
async def main():
    try:
        # Step 1: Fetch all animals
        animals = await fetch_animals()
        logging.info(f"Total animals fetched: {len(animals)}")

        # Step 2: Fetch detailed animal data and transform
        all_transformed_animals = []
        for animal in animals:
            animal_detail = await fetch_animal_detail(animal['id'])
            transformed_animal = transform_animal(animal_detail)
            all_transformed_animals.append(transformed_animal)

        # Step 3: Post in batches of 100
        for i in range(0, len(all_transformed_animals), MAX_BATCH_SIZE):
            batch = all_transformed_animals[i:i + MAX_BATCH_SIZE]
            await post_animals_batch(batch)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")

# Run the ETL process
if __name__ == "__main__":
    asyncio.run(main())
