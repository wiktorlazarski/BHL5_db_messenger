import os
from datetime import datetime

import pandas as pd
import sqlalchemy
from sqlalchemy import Column, DateTime, ForeignKey, Integer, Text, and_, create_engine, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.environ["DATABASE_URL"]


class Message(declarative_base()):
    __tablename__ = "messages"

    message_id = Column(Integer, primary_key=True)
    sender = Column(Integer)
    receiver = Column(Integer)
    message_content = Column(Text)
    advertisement_id = Column(Integer)
    message_timestamp = Column(DateTime, nullable=False)


class DatabaseChat:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

    def get_conversation(self, sender_name: str, receiver_name: str, advertisement_id: int) -> str:
        session = self.Session()
        sender_id = self._get_user_id(session, sender_name)
        receiver_id = self._get_user_id(session, receiver_name)

        chat_query = f"""
            SELECT sender, receiver, message_content, msg.message_timestamp
            FROM messages AS msg
            WHERE msg.advertisement_id = {advertisement_id}
                AND ((msg.sender = {sender_id} AND msg.receiver = {receiver_id})
                        OR
                    ((msg.sender = {receiver_id} AND msg.receiver = {sender_id})))
            ORDER BY msg.message_timestamp"""

        messages_df = pd.read_sql(chat_query, con=self.engine)
        return messages_df.to_dict(orient="records")

    def insert_message(
        self, sender_name: str, receiver_name: str, message: str, advertisement_id: int
    ) -> None:
        session = self.Session()
        sender_id = self._get_user_id(session, sender_name)
        receiver_id = self._get_user_id(session, receiver_name)
        new_record = Message(
            sender=sender_id,
            receiver=receiver_id,
            message_content=message,
            advertisement_id=advertisement_id,
            message_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        session.add(new_record)
        session.commit()

    def _get_user_id(self, session: sqlalchemy.orm.session.Session, user_name: str) -> int:
        user_id = session.execute(
            f"SELECT user_id FROM user_profile WHERE user_name = '{user_name}';"
        )
        return user_id.first()[0]
