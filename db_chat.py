from datetime import datetime

import pandas as pd
import sqlalchemy
from sqlalchemy import Column, DateTime, ForeignKey, Integer, Text, and_, create_engine, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "postgres://oraqbjleiezmdo:4734b6ba63f8f1f7ac99f733540eeedc8f52cffbb3bbca4cabf022f783a4efb3@ec2-34-254-24-116.eu-west-1.compute.amazonaws.com:5432/d7hgh8kuosqcj0"


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

        def map_name(user_id: int) -> str:
            return sender_name if user_id == sender_id else receiver_name

        messages_df["sender"] = messages_df["sender"].map(
            lambda user_id: sender_name if user_id == sender_id else receiver_name
        )
        messages_df["receiver"] = messages_df["receiver"].map(
            lambda user_id: receiver_name if user_id == receiver_id else sender_name
        )

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
