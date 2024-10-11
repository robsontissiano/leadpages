import pytest
import asyncio
from main import (
    AnimalDetail,
    transform_animal,
    fetch_animal_detail,
    transform_animal,
    post_animals_batch,
    TransformedAnimal,
)
from pydantic import ValidationError
import httpx
from httpx import AsyncClient
from pytest_httpx import HTTPXMock


@pytest.mark.asyncio
async def test_fetch_animal_detail(httpx_mock: HTTPXMock):
    # Mocking a successful response from the server
    animal_id = 1
    url = f"http://localhost:3123/animals/v1/animals/{animal_id}"
    httpx_mock.add_response(
        method="GET",
        url=url,
        json={
            "id": 1,
            "name": "Lion",
            "born_at": 1655323200000,
            "friends": "Tiger,Elephant",
        },
        status_code=200,
    )

    result = await fetch_animal_detail(animal_id)
    assert result.id == 1
    assert result.name == "Lion"
    assert result.born_at == 1655323200000
    assert result.friends == "Tiger,Elephant"


def test_transform_animal():
    animal_detail_dict = {
        "id": 1,
        "name": "Lion",
        "born_at": 1655323200000,
        "friends": "Tiger,Elephant",
    }

    # Convert the dictionary to an AnimalDetail instance
    animal_detail = AnimalDetail(**animal_detail_dict)

    transformed = transform_animal(animal_detail)

    assert transformed.id == 1
    assert transformed.name == "Lion"
    assert transformed.born_at == "2022-06-15T20:00:00Z"
    assert transformed.friends == ["Tiger", "Elephant"]


@pytest.mark.asyncio
async def test_post_animals_batch(httpx_mock: HTTPXMock):
    url = "http://localhost:3123/animals/v1/home"
    transformed_animals = [
        TransformedAnimal(
            id=1,
            name="Lion",
            born_at="2022-06-16T07:00:00Z",
            friends=["Tiger", "Elephant"],
        )
    ]

    # Mocking a successful post response
    httpx_mock.add_response(method="POST", url=url, status_code=200, json={})

    await post_animals_batch(transformed_animals)
    # Ensure the mock was called
    assert len(httpx_mock.get_requests()) == 1
