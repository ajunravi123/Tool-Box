from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Optional

app = FastAPI()



class TimeRequest(BaseModel):
    hour: Optional[int] = None  # Python 3.8 compatible annotation

@app.post("/greet", summary="Get a greeting message")
async def get_greeting(request: TimeRequest):
    # Get the current UTC time and convert to IST (UTC+5:30)
    now_utc = datetime.utcnow()
    now_bengaluru = now_utc + timedelta(hours=5, minutes=30)

    hour = request.hour if request.hour is not None else now_bengaluru.hour

    if not (0 <= hour <= 23):
        raise HTTPException(status_code=400, detail="Hour must be between 0 and 23.")

    if 5 <= hour < 12:
        greeting = "Good morning!"
    elif 12 <= hour < 18:
        greeting = "Good afternoon!"
    elif 18 <= hour < 22:
        greeting = "Good evening!"
    else:
        greeting = "Good night!"

    return {"greeting": greeting}


# Pydantic model for TextRequest (used by /process)
class TextRequest(BaseModel):
    text: str

# Process text endpoint
EMOJI_MAP = {
    "morning": "☀️",
    "afternoon": "🌤️",
    "evening": "🌙",
    "night": "🌜",
    "hello": "👋",
    "hi": "😊",
    "hey": "🙌",
}

@app.post("/process", summary="Process text")
async def process_text(request: TextRequest):
    text = request.text.strip()

    if not text:
        raise HTTPException(status_code=400, detail="Text cannot be empty.")

    # Find a relevant emoji based on keywords in the text
    emoji = next((emoji for keyword, emoji in EMOJI_MAP.items() if keyword in text.lower()), "✨")
    
    # Append emoji to the processed text
    processed_text = f"{text} {emoji}"

    return {"result": processed_text}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)