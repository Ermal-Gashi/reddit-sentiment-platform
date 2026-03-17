import uvicorn
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from backend.db import init_pool, _pool
from backend.api.routes import router as api_router

#npm run dev

# python -m uvicorn backend.api_main:app --host 127.0.0.1 --port 8000 --reload

app = FastAPI(
    title="Reddit Sentiment API",
    version="0.3.1",
    description="FastAPI backend for Reddit Sentiment Dashboard",
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # DEV MODE
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    init_pool(minconn=1, maxconn=10)
    print(" Database pool initialized.")

    try:
        yield
    finally:
        # Shutdown
        if _pool:
            _pool.closeall()
            print(" Database pool closed gracefully.")


app.router.lifespan_context = lifespan


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")


app.include_router(api_router)


if __name__ == "__main__":
    uvicorn.run(
        "backend.api_main:app",
        host="127.0.0.1",
        port=8000,
        log_level="info",
    )
