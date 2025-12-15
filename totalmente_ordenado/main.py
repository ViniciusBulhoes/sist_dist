from fastapi import FastAPI
from pydantic import BaseModel
import threading
import uvicorn
import os
import time
import random
import heapq
import requests

app = FastAPI()

# ----- Data Models -----
class Message(BaseModel):
    MsgId: str
    ProcessId: int
    Msg: str
    Timestamp: int

# ----- Global State -----
messages = []         # priority queue: (timestamp, Message)
acks = {}             # MsgId -> count of ACKs
processed = set()     # keep track of fully processed messages
servers = [
    "lamport-0.lamport-svc:8080",
    "lamport-1.lamport-svc:8080",
    "lamport-2.lamport-svc:8080"
]

lock = threading.Lock()
msg_count = 0
initTime = random.randint(1, 10)

# ----- Detect pod index automatically -----
pod_name = os.environ.get("POD_NAME", "lamport-0")
thisProcess = int(pod_name.split("-")[-1])
serverCode = ["a", "b", "c"][thisProcess]
totalServers = len(servers)

# ----- Utility Functions -----
def generateMsgId():
    global msg_count
    msg_count += 1
    return serverCode + str(msg_count)

def generateMessage():
    msg_id = generateMsgId()
    return Message(
        MsgId=msg_id,
        ProcessId=thisProcess,
        Msg=f"Hello from server {thisProcess}, message {msg_id}",
        Timestamp=initTime + 1
    )

def message_exists(messages_list, new_msg):
    return any(
        (m.MsgId == new_msg.MsgId and m.ProcessId == new_msg.ProcessId)
        for _, m in messages_list
    )

# ----- Message Processing -----
def process_messages():
    while True:
        with lock:
            if messages:
                _, msg = messages[0]
                count = acks.get(msg.MsgId, 0)
                #print("size o message queue", len(messages))
                if count >= totalServers and msg.MsgId not in processed:
                    print(
                        f"[Process {thisProcess}] Message {msg.MsgId} with time stamp {msg.Timestamp} fully acknowledged: {msg.Msg}",
                        flush=True
                    )
                    processed.add(msg.MsgId)
                    heapq.heappop(messages)

        time.sleep(0.5)

# ----- Communication -----
def broadcast_message(message: Message):
    for i, server in enumerate(servers):
        if i != thisProcess:
            try:
                r = requests.post(
                    f"http://{server}/message",
                    json=message.model_dump(),
                    timeout=2
                )
                print(
                    f"[Process {thisProcess}] Message {message.MsgId} sent to server {i}: {r.status_code}",
                    flush=True
                )
            except Exception as e:
                print(
                    f"[Process {thisProcess}] Failed to send message to server {i}: {e}",
                    flush=True
                )

def send_ack_to_servers(message: Message):
    for i, server in enumerate(servers):
        if i != thisProcess:
            try:
                r = requests.post(
                    f"http://{server}/ack",
                    json=message.model_dump(),
                    timeout=2
                )
                print(
                    f"[Process {thisProcess}] ACK sent to server {i}: {r.status_code}",
                    flush=True
                )
            except Exception as e:
                print(
                    f"[Process {thisProcess}] Failed to send ACK to server {i}: {e}",
                    flush=True
                )

# ----- FastAPI Endpoints -----
@app.post("/message")
def receive_message(payload: Message):
    with lock:
        if not message_exists(messages, payload):
            heapq.heappush(messages, (payload.Timestamp, payload))

        acks.setdefault(payload.MsgId, 0)
        acks[payload.MsgId] += 1  # increment on receiving message

    send_ack_to_servers(payload)
    #process_messages()
    return {"status": "stored in priority queue", "message": payload}

@app.post("/ack")
def receive_ack(payload: Message):
    with lock:
        acks.setdefault(payload.MsgId, 0)
        acks[payload.MsgId] += 1  # increment on receiving ACK
        print("amounts of acks for", payload.MsgId, ":", acks[payload.MsgId])

    #process_messages()
    return {"status": "ACK counted", "message": payload}

# ----- Main -----
if __name__ == "__main__":
    # Start FastAPI server in separate thread
    server_thread = threading.Thread(
        target=lambda: uvicorn.run(app, host="0.0.0.0", port=8080),
        daemon=True
    )
    server_thread.start()

    # Start processing loop
    processor_thread = threading.Thread(
        target=process_messages,
        daemon=True
    )
    processor_thread.start()

    #wait for server to be ready
    time.sleep(15)

    # Broadcast initial message (own message)
    first_msg = generateMessage()

    with lock:
        heapq.heappush(messages, (first_msg.Timestamp, first_msg))
        acks[first_msg.MsgId] = 1  # count self

    for i in range(3):
        broadcast_message(first_msg)

    # Keep main thread alive
    server_thread.join()