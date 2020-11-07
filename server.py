from fastapi import FastAPI
from pydantic import BaseModel

from db_chat import DATABASE_URL, DatabaseChat

app = FastAPI()
db_chat = DatabaseChat(DATABASE_URL)


class DBInsertRequest(BaseModel):
    sender_name: str
    receiver_name: str
    message_content: str
    advertisement_id: int


@app.get("/")
async def root():
    return {"message": "Hello"}


@app.post("/new_message")
async def new_message(db_insert: DBInsertRequest) -> str:
    db_chat.insert_message(
        sender_name=db_insert.sender_name,
        receiver_name=db_insert.receiver_name,
        message=db_insert.message_content,
        advertisement_id=db_insert.advertisement_id,
    )
    return "OK"


@app.get("/chat")
async def get_chat(sender_name: str, receiver_name: str, advertisement_id: int) -> str:
    chat = db_chat.get_conversation(sender_name, receiver_name, advertisement_id)
    return chat
