from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import httpx

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/api/users/{user_id}")
async def get_user(user_id: int):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"http://localhost:8000/users/{user_id}",
                timeout=10.0
            )
            return response.json()
        except Exception as e:
            return {"error": str(e)}

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/")
async def root():
    return FileResponse("static/index.html")
