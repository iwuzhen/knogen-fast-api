from fastapi import FastAPI
from app.routers import openalex, baikedemo, wikipedia, metapedia_v1

app = FastAPI()

app.include_router(openalex.router)
app.include_router(baikedemo.router)
app.include_router(wikipedia.router)
app.include_router(metapedia_v1.router)


@app.get("/")
async def read_root():
    """Main function of the web application
    
    Returns:
        List -- Hello World
    """
    return {"Hello": "World"}

