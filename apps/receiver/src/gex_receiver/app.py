from fastapi import FastAPI

from gex_receiver.config import APP_SETTINGS

app = FastAPI()
print(APP_SETTINGS)
