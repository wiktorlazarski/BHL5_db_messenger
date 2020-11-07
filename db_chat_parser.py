import os
import pandas as pd

from sqlalchemy import Column, DateTime, ForeignKey, Integer, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# DATABASE_URL = os.environ['DATABASE_URL']
DATABASE_URL = "postgres://localhost:5432/test_db?user=postgres&password=mispluszowy"
engine = create_engine(DATABASE_URL, echo=True)


Base = declarative_base()


class Message(Base):
    __tablename__ = "messages"

    message_id = Column(Integer, primary_key=True)
    sender = Column(Integer, ForeignKey("user_profile.user_id"))
    receiver = Column(Integer, ForeignKey("user_profile.user_id"))
    message_content = Column(Text)
    advertisement_id = Column(Integer), ForeignKey("advertisement.advertisement_id")
    message_timestamp = Column(DateTime, nullable=False)

    def __repr__(self):
        return f"{self.message_id}\t{self.message_content[:20]}\t{self.message_timestamp}"

Session = sessionmaker(bind=engine)
session = Session()

query_statement = session.query(Message).order_by(Message.message_timestamp).statement
messages_df = pd.read_sql(query_statement, con=engine)

messages_df.drop('message_timestamp', inplace=True, axis="columns")
messages_df.to_json('./sample.json', orient="index")