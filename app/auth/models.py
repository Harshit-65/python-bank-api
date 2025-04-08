from pydantic import BaseModel

class KeyfolioResponse(BaseModel):
    valid: bool
    limit: int
    remaining: int
    reset_after_ms: int | None = None 