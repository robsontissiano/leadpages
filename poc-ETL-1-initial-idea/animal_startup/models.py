from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List
import pytz

class AnimalDetail(BaseModel):
    id: int
    name: str
    species: str
    born_at: Optional[datetime]
    friends: str

    def transform(self):
        if self.born_at:
            self.born_at = self.born_at.astimezone(pytz.UTC).isoformat()
        self.friends = self.friends.split(',')
        return self.dict()

