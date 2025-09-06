from datetime import datetime
from typing import List, Optional
from pydantic  import BaseModel
class TelegramMessage(BaseModel):
    channel: str
    id: int
    date: datetime
    text: str
