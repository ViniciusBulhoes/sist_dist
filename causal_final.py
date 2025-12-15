from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from typing import List
from collections import defaultdict
import requests
import threading
import time
import sys
import uvicorn

app = FastAPI()

myProcessId = 0
timeStamp = [0, 0, 0]
posts = []
replies = []
buffer = []
state_lock = threading.Lock()

processes = [
    "8080",
    "8081",
    "8082"
]

class Event (BaseModel):
    processId: int
    timeStamp: Optional[List[int]] = None
    parentId: Optional[str] = None
    author: str
    content: str 
    eventId: str

@app.post("/post")
def post(msg: Event):
    global timeStamp

    with state_lock:
        if msg.processId == myProcessId:
            timeStamp[myProcessId] += 1
            msg.timeStamp = timeStamp.copy()  # IMPORTANT: copy list

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
    global timeStamp

    with state_lock:
        if not can_deliver(msg):
            buffer.append(msg)
            return {"status": "buffered"}

        timeStamp[msg.processId] += 1

        if msg.parentId is None:
            posts.append(msg)
        else:
            replies.append(msg)

    deliver_buffered()
    showFeed()
    return {"status": "delivered"}

def showFeed():
    print("Feed:")
    for i in posts:
        print("-------")
        print(f"Post {i.eventId} by {i.author}: {i.content}")
        print("Replies:")
        for r in replies:
            if r.parentId == i.eventId:
                print(f"--Reply {r.eventId} by {r.author}: {r.content}")

def deliver_buffered():
    delivered = True
    while delivered:
        delivered = False
        for m in buffer[:]:
            if can_deliver(m):
                timeStamp[m.processId] += 1
                if m.parentId is None:
                    posts.append(m)
                else:
                    replies.append(m)
                buffer.remove(m)
                delivered = True

def can_deliver(msg):
    sender = msg.processId

    if msg.timeStamp[sender] != timeStamp[sender] + 1:
        return False

    for i in range(len(timeStamp)):
        if i != sender and msg.timeStamp[i] > timeStamp[i]:
            return False

    return True

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
