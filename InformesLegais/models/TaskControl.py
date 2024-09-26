from pydantic import BaseModel
from uuid import UUID , uuid4
from datetime import datetime , timedelta
from typing import Optional
from bson import Binary


class TaskModel(BaseModel):
    id : UUID = uuid4()
    name: str
    start_time : datetime
    end_time : Optional[datetime]
    status : str

    def to_mongo(self):
        # Convert UUID to bson.Binary before saving to MongoDB
        return {
            "id": Binary.from_uuid(self.id),  # Convert UUID to BSON Binary
            "name": self.name ,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'status': self.status
        }

