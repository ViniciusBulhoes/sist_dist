from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from collections import defaultdict
import requests
import threading
import time
import sys
import uvicorn

app = FastAPI()

myProcessId = 0
timeStamp = 0
posts = []
replies = []

processes = [
    "8080",
    "8081",
    "8082"
]

class Event (BaseModel):
    processId: int
    timeStamp: Optional[int] = None
    parentId: Optional[str] = None
    author: str
    content: str 
    eventId: str

@app.post("/post")
def post(msg: Event):
    if msg.processId == myProcessId:
        global timeStamp
        timeStamp += 1
        msg.timeStamp = timeStamp

        if msg.parentId is None:
            posts.append(msg)
        else:
            replies.append(msg)

        for i, port in enumerate(processes):
            if i != myProcessId:
                url = f"http://localhost:{port}/share"
                async_send(url, msg.model_dump())

    showFeed()

@app.post("/share")
def share(msg: Event):
    if msg.parentId is None:
        posts.append(msg)
    else:
        replies.append(msg)

    showFeed()

def showFeed():
    for p in posts:
        print("Posts:")
        print(f"Post {p.eventId} by {p.author}: {p.content} (TS: {p.timeStamp})")
        print("Replies:")
        for r in replies:
            if r.parentId == p.eventId:
                print(f"--Reply {r.eventId} by {r.author}: {r.content} (TS: {r.timeStamp})")

    print("----------------------")
    print("Replies orfãs:")
    for r in replies:
        if not any(p.eventId == r.parentId for p in posts):
            print(f"--Reply {r.eventId} by {r.author}: {r.content} (TS: {r.timeStamp})")

def async_send(url: str, payload: dict):
    def send_request():
        if myProcessId == 0:
            time.sleep(10)
        try:
            response = requests.post(url, json=payload)
            # Optionally handle the response, e.g., print(response.status_code)
        except Exception as e:
            print(f"Error sending to {url}: {e}")
    
    thread = threading.Thread(target=send_request)
    thread.start()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python eventual.py <processId>")
        sys.exit(1)

    myProcessId = int(sys.argv[1])
    uvicorn.run(app, host="0.0.0.0", port=int(processes[myProcessId]))
