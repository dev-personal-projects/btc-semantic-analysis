from datetime import datetime
from pydantic import BaseModel

class TelegramMessage(BaseModel):
    channel: str
    id: int
    date: datetime
    text: str