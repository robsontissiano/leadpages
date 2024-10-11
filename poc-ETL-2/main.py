import httpx
import asyncio
import datetime
import logging
import traceback
from typing import List, Optional
from pydantic import BaseModel
import random
from concurrent.futures import ThreadPoolExecutor

# Setup logging
logging.basicConfig(filename='animal_loader.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BASE_URL = 'http://localhost:3123/animals/v1'
MAX_BATCH_SIZE = 100
RETRY_WAIT_TIME = [3, 30]  # Random delay between 3 and 30 seconds for retries
MAX_RETRIES = 5  # Maximum retry attempts for 500-504 errors

# ThreadPoolExecutor for parallel processing of failed requests
executor = ThreadPoolExecutor(max_workers=10)

# Pydantic models to structure the animal data
class AnimalDetail(BaseModel):
    id: int
    name: str
    born_at: Optional[int]
    friends: Optional[str]

class TransformedAnimal(BaseModel):
    id: int
    name: str
    born_at: Optional[str]  # Must be a string in ISO8601 format for correct serialization
    friends: List[str]  # Should be a list of strings, not a single string

# Function to retry fetching animal data (paginated) in the background
async def retry_fetch_page(page: int):
    retries = 0
    async with httpx.AsyncClient() as client:
        while retries < MAX_RETRIES:
            try:
                logging.info(f"Retrying fetching animals on page {page} (Attempt {retries + 1})")
                response = await client.get(f"{BASE_URL}/animals", params={'page': page})
                response.raise_for_status()
                data = response.json()
                logging.info(f"Successfully fetched animals on page {page} after {retries + 1} retries.")
                return data['items']  # Return the items if successful
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                if hasattr(exc, 'response') and exc.response is not None:
                    if exc.response.status_code in [500, 502, 503, 504]:
                        retries += 1
                        wait_time = random.randint(RETRY_WAIT_TIME[0], RETRY_WAIT_TIME[1])
                        logging.warning(f"Error on page {page} with status {exc.response.status_code}. Retrying in {wait_time} seconds.")
                        await asyncio.sleep(wait_time)
                    else:
                        logging.error(f"Unrecoverable error fetching page {page}. Skipping...")
                        return []
                else:
                    logging.error(f"Failed to fetch animals on page {page} after {retries} retries.")
                    logging.error(f"Exception details: {exc}")
                    logging.error(traceback.format_exc())
                    return []

# Function to fetch animal data (paginated) and handle server errors (500-504) by retrying in the background
async def fetch_animals():
    page = 1
    animals = []
    async with httpx.AsyncClient() as client:
        while True:
            try:
                logging.info(f"####### Fetching animals on page {page}")
                response = await client.get(f"{BASE_URL}/animals", params={'page': page})
                response.raise_for_status()
                data = response.json()
                animals.extend(data['items'])

                # Break if all pages are fetched
                if page >= data['total_pages']:
                    return animals
                page += 1
            except (httpx.ReadTimeout, httpx.RequestError, httpx.HTTPStatusError) as exc:
                logging.error(f"Error occurred while fetching animals on page {page}: {exc}")
                logging.info(f"Continuing to the next page ({page + 1}) while retrying page {page} in the background.")
                # Retry the page in the background
                asyncio.create_task(retry_fetch_page(page))
                page += 1  # Continue with the next page immediately

# Function to fetch detailed animal data by ID with retry logic, now runs in background on timeout
async def fetch_animal_detail(animal_id: int):
    retries = 0
    async with httpx.AsyncClient() as client:
        while retries < MAX_RETRIES:
            try:
                logging.info(f"Fetching details for animal ID {animal_id}")
                response = await client.get(f"{BASE_URL}/animals/{animal_id}")
                response.raise_for_status()  # Raise exception for HTTP error codes
                return AnimalDetail(**response.json())  # Return the animal detail if successful

            except (httpx.HTTPStatusError, httpx.ReadTimeout, httpx.RequestError) as exc:
                if isinstance(exc, httpx.ReadTimeout):
                    logging.error(f"ReadTimeout occurred while fetching animal ID {animal_id}: {exc}")
                else:
                    logging.error(f"Error fetching animal ID {animal_id}: {exc}")

                if hasattr(exc, 'response') and exc.response is not None:
                    status_code = exc.response.status_code
                    if status_code in [500, 502, 503, 504]:
                        retries += 1
                        wait_time = random.randint(RETRY_WAIT_TIME[0], RETRY_WAIT_TIME[1])
                        logging.warning(f"Error fetching animal ID {animal_id} with status {status_code}. Retrying in {wait_time} seconds (Attempt {retries + 1}/{MAX_RETRIES})")
                        await asyncio.sleep(wait_time)
                    else:
                        logging.error(f"Failed to fetch details for animal ID {animal_id} after {retries} retries.")
                        logging.error(traceback.format_exc())
                        return None
                else:
                    logging.error(f"Unrecoverable error or no response available for animal ID {animal_id}. Details: {traceback.format_exc()}")
                    return None

# Transform the animal data
def transform_animal(animal_detail: AnimalDetail) -> TransformedAnimal:
    # Transform friends into a list
    friends_list = animal_detail.friends.split(",") if animal_detail.friends else []

    # Convert born_at to ISO8601 UTC if present
    born_at_utc = None
    if animal_detail.born_at:
        born_at_utc = datetime.datetime.utcfromtimestamp(animal_detail.born_at / 1000).isoformat() + "Z"

    return TransformedAnimal(
        id=animal_detail.id,
        name=animal_detail.name,
        born_at=born_at_utc,
        friends=friends_list
    )

# Function to handle retries for 500 errors in a separate thread
def retry_failed_post(batch: List[TransformedAnimal]):
    asyncio.run(post_animals_batch(batch))

# Function to post batch of animals using Pydantic .json() and retry on 500-series errors
async def post_animals_batch(animals_batch: List[TransformedAnimal]):
    async with httpx.AsyncClient() as client:
        retries = 0
        while retries < MAX_RETRIES:
            try:
                logging.info(f"Posting batch of {len(animals_batch)} animals")

                # Use Pydantic's .json() method to serialize data with datetime support
                json_data = [animal.dict() for animal in animals_batch]

                response = await client.post(f"{BASE_URL}/home", json=json_data)  # Fix: Use "json=" for async requests
                response.raise_for_status()
                logging.info(f"Successfully posted batch: {response.json()}")
                return
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 422:
                    logging.error(f"Unprocessable Entity (422) error: {exc.response.text}")
                    break
                elif exc.response.status_code in [500, 502, 503, 504]:
                    retries += 1
                    if retries < MAX_RETRIES:
                        wait_time = random.randint(RETRY_WAIT_TIME[0], RETRY_WAIT_TIME[1])
                        logging.warning(f"Server error ({exc.response.status_code}). Retrying in {wait_time} seconds (Attempt {retries + 1}/{MAX_RETRIES})")
                        await asyncio.sleep(wait_time)
                    else:
                        logging.error(f"Exceeded maximum retries for batch. Offloading retry to a separate thread.")
                        executor.submit(retry_failed_post, animals_batch)
                        return
            except Exception as e:
                logging.error(f"Failed to post batch after {retries} retries. Exception: {e}")
                logging.error(traceback.format_exc())
                return

# Function to process animals in batches, keeping the main process going while retrying errors in the background
async def process_animals_in_batches(all_animals):
    all_transformed_animals = []
    tasks = []
    for animal in all_animals:
        tasks.append(fetch_animal_detail(animal['id']))

        # Once we've accumulated 100 animals, process them in a batch
        if len(tasks) % MAX_BATCH_SIZE == 0:
            # Gather animal details
            animal_details = await asyncio.gather(*tasks)
            tasks = []  # Reset the tasks list for the next batch

            # Transform animal data and add to the transformed list
            transformed_batch = [transform_animal(detail) for detail in animal_details if detail]
            all_transformed_animals.extend(transformed_batch)

            # Post batch
            await post_animals_batch(transformed_batch)

    # Process any remaining animals (less than 100)
    if tasks:
        animal_details = await asyncio.gather(*tasks)
        transformed_batch = [transform_animal(detail) for detail in animal_details if detail]
        await post_animals_batch(transformed_batch)

# Main function to orchestrate the ETL process
async def main():
    try:
        # Step 1: Fetch all animals
        all_animals = await fetch_animals()
        logging.info(f"Total animals fetched: {len(all_animals)}")

        # Step 2: Process animals in batches of 100, retry in background if needed
        await process_animals_in_batches(all_animals)

    except Exception as e:
        logging.error(f"Unhandled error occurred: {str(e)}")
        logging.error(traceback.format_exc())

# Run the ETL process
if __name__ == "__main__":
    asyncio.run(main())
