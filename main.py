from typing import List
from typing import Union
from fastapi import Cookie, Depends, Query, status
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import connection_manager as connection
import logging
import sys
import configparser
import psycopg2

app = FastAPI()

# Logging
config = configparser.ConfigParser()
config.read('config/config.ini')

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger('websockets.server')
logger2 = logging.getLogger('asyncio')
logger2.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
logger2.addHandler(logging.StreamHandler())

# Manager for websockets
manager = connection.ConnectionManager()

# PostgreSQL connection
conn = psycopg2.connect(filename='database.ini')
cur = conn.cursor()


def query():
    """ query part and vendor data from multiple tables"""
    conn = None
    try:
        iter_row = cur.execute("""
            SELECT part_name, vendor_name
            FROM parts
            INNER JOIN vendor_parts ON vendor_parts.part_id = parts.part_id
            INNER JOIN vendors ON vendors.vendor_id = vendor_parts.vendor_id
            ORDER BY part_name;
        """)
        for row in iter_row(cur, 10):
            print(row)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


async def get_cookie_or_token(
        websocket: WebSocket,
        session: Union[str, None] = Cookie(default=None),
        token: Union[str, None] = Query(default=None),
):
    if session is None and token is None:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
    return session or token


@app.websocket("/ws/{client_id}")
async def websocket_bronce(
        websocket: WebSocket,
        item_id: str,
        q: Union[int, None] = None,
        cookie_or_token: str = Depends(get_cookie_or_token),
):
    await websocket.accept()
    while True:
        data = await websocket.receive_text()
        await websocket.send_text(
            f"Session cookie or query token value is: {cookie_or_token}"
        )
        if q is not None:
            await websocket.send_text(f"Query parameter q is: {q}")
        await websocket.send_text(f"Message text was: {data}, for item ID: {item_id}")


@app.websocket("/ws/{client_id}")
async def websocket_silver(websocket: WebSocket, client_id: int):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.send_personal_message(f"You wrote: {data}", websocket)
            await manager.broadcast(f"Client #{client_id} says: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.broadcast(f"Client #{client_id} left the chat")


@app.websocket("/ws/{client_id}")
async def websocket_gold(websocket: WebSocket, client_id: int):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.send_personal_message(f"You wrote: {data}", websocket)
            await manager.broadcast(f"Client #{client_id} says: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.broadcast(f"Client #{client_id} left the chat")
