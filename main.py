import asyncio
import json
import logging
import multiprocessing
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, Optional
import httpx
import requests
import uvicorn
import valkey
from decouple import config
from fastapi import (
    FastAPI,
    HTTPException,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import JSONResponse
from peewee import SQL, DoesNotExist, fn
from playhouse.shortcuts import model_to_dict
from pydantic import BaseModel
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware
import multiprocessing
from dbconn import keep_db_alive
from models import (
    db,
)
from models import (
    Transaction as Transactions,
)
from OCPP_Requests import (
    ChargePoint,
)
from fastapi.middleware.gzip import GZipMiddleware
from send_transaction_to_be import start_worker, shutdown_worker
from start_txn_hook_queue import start_hook_worker, shutdown_hook_worker

CHARGER_DATA_KEY = "charger_data_cache"
CACHE_EXPIRY = 7200  # Cache TTL in seconds (2 hours)  # Cache for 2 hours

API_KEY_NAME = "x-api-key"

valkey_uri = config("VALKEY_URI")
valkey_client = valkey.from_url(valkey_uri)


class VerifyAPIKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip API key check for WebSocket connections
        if "sec-websocket-key" not in request.headers:
            if request.url.path.startswith("/api/") and request.url.path != "/api/hello":
                api_key = request.headers.get("x-api-key")
                expected_api_key = config("API_KEY")
                logging.info(
                    f"Received API Key: {api_key}, Expected API Key: {expected_api_key}"
                )
                if api_key != expected_api_key:
                    raise HTTPException(status_code=403, detail="Invalid API key")
        response = await call_next(request)
        return response


async def refresh_cache():
    try:
        charger_data = await central_system.get_charger_data()
        valkey_client.setex(
            CHARGER_DATA_KEY, CACHE_EXPIRY, json.dumps(charger_data)
        )  # serialize as JSON string
        print("Charger data has been cached.")
    except Exception as e:
        print(f"Failed to refresh cache: {e}")


# Startup event calling the global refresh_cache function


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Lifespan startup triggered.")
    try:
        await refresh_cache()
        print("✅ refresh_cache() completed.")
        app.state.db_heartbeat_task = asyncio.create_task(keep_db_alive())
        print("🔄 DB heartbeat task started.")
        # app.state.inactivity_watchdog_task = asyncio.create_task(central_system.periodic_inactivity_watchdog())
        # print("Charger inactivity watchdog started.")
        start_worker()
        print("Service to send transaction data to BE started successfully.")
        start_hook_worker()
        print("Service to manageautocutoff data started")

    except Exception as e:
        print(f"❌ Exception during startup: {e}")

    print("🔥 Yielding to start app...")
    yield
    print("🧹 App is shutting down...")

    try:
        valkey_client.delete(CHARGER_DATA_KEY)
        print("🧼 Cache cleared on shutdown.")

        for ws in central_system.active_connections.values():
            try:
                await ws.close(code=1001, reason="Server shutdown")
                print("✅ WebSocket closed on shutdown.")
            except Exception as e:
                print(f"⚠️ Error closing WebSocket during shutdown: {e}")

        try:
            central_system.active_connections.clear()
            print("✅ active_connections cleared.")
        except Exception as e:
            print(f"⚠️ Failed to clear active_connections: {e}")

        try:
            central_system.charge_points.clear()
            print("✅ charge_points cleared.")
        except Exception as e:
            print(f"⚠️ Failed to clear charge_points: {e}")

        try:
            central_system.frontend_connections.clear()
            print("✅ frontend_connections cleared.")
        except Exception as e:
            print(f"⚠️ Failed to clear frontend_connections: {e}")

        try:
            central_system.pending_start_transactions.clear()
            print("✅ pending_start_transactions cleared.")
        except Exception as e:
            print(f"⚠️ Failed to clear pending_start_transactions: {e}")

        try:
            central_system.verification_failures.clear()
            print("✅ verification_failures cleared.")
        except Exception as e:
            print(f"⚠️ Failed to clear verification_failures: {e}")

        try:
            await shutdown_worker()
            print("Worker to send transaction data to BE shutdown")
        except Exception as e:
            print("Worker to send transaction data to BE couldn't be shut down: {e}")

        try:
            await shutdown_hook_worker()
            print("Worker to manage charging autocutoff shutdown")
        except Exception as e:
            print("Worker to manage charging autocutoff couldn't be shut down: {e}")

    except Exception as e:
        print(f"❌ Error during shutdown: {e}")


middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    ),
    Middleware(VerifyAPIKeyMiddleware),
    Middleware(GZipMiddleware),
]

app = FastAPI(
    middleware=middleware,
    lifespan=lifespan,
)


class WebSocketAdapter:
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket

    async def recv(self):
        return await self.websocket.receive_text()

    async def send(self, message):
        await self.websocket.send_text(message)

    async def close(self):
        await self.websocket.close()


class CentralSystem:
    def __init__(self):
        self.charge_points = {}
        self.active_connections = {}
        self.verification_failures = {}
        self.frontend_connections = {}
        self.pending_start_transactions = {}
        max_workers = int(os.getenv("MAX_CONCURRENT_REQUESTS", multiprocessing.cpu_count() * 3))
        self.send_request_semaphore = asyncio.Semaphore(max_workers)

    def currdatetime(self):
        return datetime.now(timezone.utc)

    async def handle_charge_point(self, websocket: WebSocket, charge_point_id: str):
        # 🛡 Verify charger ID
        if not await self.verify_charger_id(charge_point_id):
            await websocket.close(code=1000)
            return

        # 🔄 Handle reconnect: cleanup previous WS/Valkey state
        if charge_point_id in self.active_connections:
            try:
                await self.active_connections[charge_point_id].close()
                logging.warning(f"[Reconnect] Closed stale WebSocket for charger {charge_point_id}")
            except Exception as e:
                logging.error(f"[Reconnect] Error closing old WebSocket for {charge_point_id}: {e}")

            self.active_connections.pop(charge_point_id, None)
            self.charge_points.pop(charge_point_id, None)

            try:
                valkey_client.delete(f"active_connections:{charge_point_id}")
            except Exception as e:
                logging.warning(f"[Reconnect] Could not delete Valkey key for {charge_point_id}: {e}")

        await websocket.accept()
        logging.info(f"Charge point {charge_point_id} connected.")

        # 💾 Track WebSocket connection
        valkey_client.set(f"active_connections:{charge_point_id}", os.getpid())
        self.active_connections[charge_point_id] = websocket

        # 🧱 Register charge point
        ws_adapter = WebSocketAdapter(websocket)
        charge_point = ChargePoint(charge_point_id, ws_adapter)
        self.charge_points[charge_point_id] = charge_point
        charge_point.online = True

        # 📢 Frontend update
        await self.notify_frontend(charge_point_id, online=True)

        try:
            # 🚀 Start main loop
            start_task = asyncio.create_task(charge_point.start())

            # ⏳ Grace period for charger to get ready
            await asyncio.sleep(10)

            # 🔧 Push configuration
            await self.enforce_remote_only_mode(charge_point_id)

            # 🧘‍♂️ Wait until charger disconnects
            await start_task

        except WebSocketDisconnect:
            logging.info(f"Charge point {charge_point_id} disconnected.")

        except Exception as e:
            logging.error(f"Error occurred while handling charge point {charge_point_id}: {e}")

        finally:
            # 🧹 Always cleanup on disconnect or crash
            valkey_client.delete(f"active_connections:{charge_point_id}")
            self.active_connections.pop(charge_point_id, None)

            if charge_point_id in self.charge_points:
                self.charge_points[charge_point_id].online = False

            await self.notify_frontend(charge_point_id, online=False)
            try:
                await websocket.close()
            except RuntimeError as e:
                if "websocket.close" in str(e):
                    logging.warning(f"[Cleanup] WebSocket for {charge_point_id} already closed.")
                else:
                    logging.error(f"[Cleanup] Error closing WebSocket for {charge_point_id}: {e}")


    async def handle_frontend_websocket(self, websocket: WebSocket, uid: str):
        await websocket.accept()

        # Track the frontend connection
        if uid not in self.frontend_connections:
            self.frontend_connections[uid] = []
        self.frontend_connections[uid].append(websocket)

        # Notify the newly connected frontend about the current status of the charger (online/offline)
        if uid in self.charge_points and self.charge_points[uid].online:
            # Charger is online, notify the frontend immediately
            await self.notify_frontend(uid, online=True)
        else:
            # Charger is offline, notify the frontend immediately
            await self.notify_frontend(uid, online=False)

        try:
            while True:
                # Frontend will just be listening, no need to handle incoming messages
                await websocket.receive_text()

        except WebSocketDisconnect:
            logging.info(f"Frontend WebSocket for charger {uid} disconnected.")

            # Safely remove the WebSocket from the frontend connections list
            if uid in self.frontend_connections:
                self.frontend_connections[uid].remove(websocket)

                # If no other connections are active, remove the entry entirely
                if not self.frontend_connections[uid]:
                    del self.frontend_connections[uid]

        except Exception as e:
            logging.error(f"Error handling frontend WebSocket for charger {uid}: {e}")

            # Ensure the WebSocket is properly removed in case of an error
            if (
                uid in self.frontend_connections
                and websocket in self.frontend_connections[uid]
            ):
                self.frontend_connections[uid].remove(websocket)

                if not self.frontend_connections[uid]:
                    del self.frontend_connections[uid]

    async def notify_frontend(self, charge_point_id: str, online: bool):
        if charge_point_id in self.frontend_connections:
            websockets = list(self.frontend_connections[charge_point_id])
            payload = {
                "charger_id": charge_point_id,
                "status": "Online" if online else "Offline",
            }

            send_tasks = []
            for ws in websockets:
                send_tasks.append(self._safe_send(ws, payload, charge_point_id))

            await asyncio.gather(*send_tasks, return_exceptions=True)
    
    async def _safe_send(self, ws: WebSocket, payload: dict, charge_point_id: str):
        try:
            await ws.send_json(payload)
        except Exception as e:
            logging.error(
                f"Error sending WebSocket message to frontend for charger {charge_point_id}: {e}"
            )

    async def enforce_remote_only_mode(self, charge_point_id: str):
        try:
            # Desired configuration values
            desired_config = {
                "HeartbeatInterval": "15",
                "MeterValueSampleInterval": "15",
                "AuthorizeRemoteTxRequests": "true",
                "LocalAuthorizeOffline": "false",
                "LocalPreAuthorize": "false",
                "AuthorizationCacheEnabled": "false",
                "AllowOfflineTxForUnknownId": "false",
                "StopTransactionOnInvalidId": "true",
                "ChargePointAuthEnable": "true",
                "FreevendEnabled": "false"
            }

            # Fetch current configuration
            current_config_resp = await self.send_request(
                charge_point_id=charge_point_id,
                request_method="get_configuration"
            )

            # Extract config list
            if hasattr(current_config_resp, "configuration_key"):
                raw_config_list = current_config_resp.configuration_key
            elif isinstance(current_config_resp, dict) and "configuration_key" in current_config_resp:
                raw_config_list = current_config_resp["configuration_key"]
            else:
                logging.warning(f"[ConfigSync] Could not parse configuration response from {charge_point_id}")
                return

            # Map current config into dict for comparison
            current_config = {
                entry["key"]: entry.get("value")
                for entry in raw_config_list
                if entry.get("key") is not None
            }

            for key, desired_value in desired_config.items():
                existing_value = current_config.get(key)

                if existing_value is None:
                    logging.warning(f"[ConfigSync] Key '{key}' not found on charger {charge_point_id}, skipping.")
                    continue

                if str(existing_value).strip() == desired_value:
                    logging.info(f"[ConfigSync] '{key}' already correct on {charge_point_id}, skipping.")
                    continue

                logging.info(f"[ConfigSync] Updating '{key}' from '{existing_value}' → '{desired_value}' on {charge_point_id}")

                response = await self.send_request(
                    charge_point_id=charge_point_id,
                    request_method="change_configuration",
                    key=key,
                    value=desired_value
                )

                if hasattr(response, "status") and response.status == "Accepted":
                    logging.info(f"[ConfigSync] Applied {key} = {desired_value} on {charge_point_id}")
                else:
                    logging.warning(
                        f"[ConfigSync] Failed to apply {key} on {charge_point_id}: {getattr(response, 'status', response)}"
                    )
            
            logging.info(f"[ConfigSync] Finished for {charge_point_id}. Final config:")
            for k, v in desired_config.items():
                logging.info(f"  - {k} = {current_config.get(k, 'MISSING')} → intended: {v}")

        except Exception as e:
            logging.error(f"[ConfigSync] Exception during config sync for {charge_point_id}: {e}")


    async def get_charger_data(self):
        """
        Fetch the entire charger data from Valkey cache or API if the cache is not available or expired.
        """
        # Try to get cached charger data from Valkey
        cached_data = valkey_client.get(CHARGER_DATA_KEY)

        if cached_data:
            logging.info("Using cached charger data from Valkey.")
            # Deserialize the JSON string back into a Python object (list)
            return json.loads(cached_data.decode("utf-8"))

        logging.info(
            "Cache not found or expired, fetching fresh charger data from API."
        )
        # Fetch fresh charger data from API
        charger_data = await self.fetch_charger_data_from_api()

        # Serialize the charger data (list) into a JSON string before storing it in Valkey
        charger_data_json = json.dumps(charger_data)

        # Store the data in Valkey with expiration time of 2 hours (7200 seconds)
        valkey_client.setex(CHARGER_DATA_KEY, CACHE_EXPIRY, charger_data_json)

        return charger_data

    async def fetch_charger_data_from_api(self):
        """
        Make an API request to get the charger data from the source.
        """
        first_api_url = config("APICHARGERDATA")
        apiauthkey = config("APIAUTHKEY")
        timeout = 10

        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(first_api_url, headers={"apiauthkey": apiauthkey})

        if response.status_code != 200:
            logging.error("Error fetching charger data from API")
            raise HTTPException(
                status_code=500, detail="Error fetching charger data from API"
            )

        charger_data = response.json().get("data", [])
        return charger_data

    # async def check_inactivity_and_update_status(self, charge_point_id: str):
    #     """
    #     Check the last_message_time for the specified charger. If it has been inactive for more than 2 minutes
    #     and is still marked as online, mark it as offline, remove it from active connections,
    #     and close any stale WebSocket connections between the charger and the backend.
    #     """
    #     # Get the charger from the central system's charge_points dictionary
    #     charge_point = self.charge_points.get(charge_point_id)

    #     if not charge_point:
    #         raise HTTPException(
    #             status_code=404, detail=f"Charger {charge_point_id} not found"
    #         )

    #     # Get the current time and the time of the last message
    #     current_time = self.currdatetime()
    #     last_message_time = charge_point.last_message_time

    #     # Calculate the difference in time (in seconds)
    #     time_difference = (datetime.now(timezone.utc) - last_message_time).total_seconds()

    #     # Check if more than 2 minutes (120 seconds) have passed since the last message
    #     if time_difference > 120 and charge_point.online:
    #         # If the charger is marked as online but has been inactive for more than 2 minutes, update its status to offline
    #         charge_point.online = False
    #         charge_point.state["status"] = "Offline"
    #         logging.info(
    #             f"Charger {charge_point_id} has been inactive for more than 2 minutes. Marking it as offline."
    #         )

    #         # Notify the frontend about the charger going offline with updated status
    #         await self.notify_frontend(charge_point_id, online=False)

    #         # Remove the charger from active connections in Valkey
    #         if f"active_connections:{charge_point_id}" in valkey_client:
    #             valkey_client.delete(f"active_connections:{charge_point_id}")
    #             logging.info(
    #                 f"Removed {charge_point_id} from Valkey active connections."
    #             )

    #         # Remove the charger from self.active_connections
    #         # if charge_point_id in self.active_connections:
    #         #     del self.active_connections[charge_point_id]
    #         #     logging.info(
    #         #         f"Removed {charge_point_id} from local active connections."
    #         #     )

    #         # Close the WebSocket connection between the charger and backend if it exists
    #         ws_adapter = (
    #             charge_point.websocket
    #         )  # Assuming ChargePoint has a WebSocket adapter

    #         if ws_adapter:
    #             try:
    #                 # Close the WebSocket connection to the charger
    #                 await ws_adapter.close()
    #                 logging.info(
    #                     f"Closed WebSocket connection for charger {charge_point_id}."
    #                 )
    #             except Exception as e:
    #                 logging.error(f"Error closing WebSocket for {charge_point_id}: {e}")
    #                 if charge_point_id in self.active_connections:
    #                     del self.active_connections[charge_point_id]
    #                     logging.info(
    #                         f"Removed {charge_point_id} from local active connections."
    #                     )

    INACTIVITY_LIMIT = 120  # seconds
    WATCHDOG_INTERVAL = 120     # seconds
    MAX_FAILURE_RETRIES = 3


    async def check_inactivity_and_update_status(self, charge_point_id: str):
        cp = self.charge_points.get(charge_point_id)
        if not cp:
            logging.warning(f"[Watchdog] Charger {charge_point_id} not found.")
            return

        if cp.last_message_time is None:
            return  # never heard from it yet

        now   = datetime.now(timezone.utc)
        delta = (now - cp.last_message_time).total_seconds()

        if delta < self.INACTIVITY_LIMIT or not cp.online:
            return  # still fine or already offline

        # ---- mark offline ----
        cp.online = False
        cp.state["status"] = "Offline"
        print(f"[Watchdog] {charge_point_id} inactive {int(delta)}s → OFFLINE")

        await self.notify_frontend(charge_point_id, online=False)

        # Valkey cleanup (delete is idempotent)
        try:
            valkey_client.delete(f"active_connections:{charge_point_id}")
        except Exception as e:
            logging.error(f"[Watchdog] Valkey delete failed for {charge_point_id}: {e}")

        # Local WS cleanup
        ws = self.active_connections.pop(charge_point_id, None)
        if ws:
            try:
                await ws.close()
                logging.info(f"[Watchdog] Closed WS for {charge_point_id}")
            except Exception as e:
                logging.error(f"[Watchdog] WS close failed for {charge_point_id}: {e}")

    async def periodic_inactivity_watchdog(self):
        """
        Checks for inactive chargers every 2 minutes.
        Retries up to 3x with exponential backoff.
        """
        failure_counts = {}

        while True:
            try:
                charger_data = await self.get_charger_data()
                known_chargers = {item["uid"] for item in charger_data}
                tasks = []

                for charger_id in known_chargers:
                    async def check(charger_id=charger_id):
                        try:
                            await self.check_inactivity_and_update_status(charger_id)
                            failure_counts.pop(charger_id, None)
                        except Exception as e:
                            count = failure_counts.get(charger_id, 0)
                            if count < self.MAX_FAILURE_RETRIES:
                                delay = 2 ** count
                                logging.warning(f"[Watchdog] {charger_id} check failed: {e}. Retrying in {delay}s...")
                                failure_counts[charger_id] = count + 1
                                await asyncio.sleep(delay)
                            else:
                                logging.error(f"[Watchdog] {charger_id} failed {self.MAX_FAILURE_RETRIES} times. Skipping this round.")
                                failure_counts[charger_id] = 0

                    tasks.append(asyncio.create_task(check()))

                await asyncio.gather(*tasks)

                # 🧹 Remove keys for deleted chargers
                failure_counts = {cid: c for cid, c in failure_counts.items() if cid in known_chargers}

            except Exception as e:
                logging.critical(f"[Watchdog] 💀 Global crash in watchdog: {e}")

            await asyncio.sleep(self.WATCHDOG_INTERVAL)


    async def verify_charger_id(self, charge_point_id: str) -> bool:
        """
        Verify if the charger ID exists in the system by checking cached data first, then the API if necessary.
        If verification fails 3 times, force an update of the cached data.
        """
        # Get the charger data (either from cache or API)
        charger_data = await self.get_charger_data()

        # Check if the charger exists in the cached data
        charger = next(
            (item for item in charger_data if item["uid"] == charge_point_id),
            None,
        )

        if not charger:
            # Track verification failures
            if charge_point_id not in self.verification_failures:
                self.verification_failures[charge_point_id] = 0
            self.verification_failures[charge_point_id] += 1

            logging.error(
                f"Charger with ID {charge_point_id} not found in the system. Verification failed."
            )

            # If verification fails 3 times, force cache update
            if self.verification_failures[charge_point_id] >= 1:
                logging.info(
                    f"Verification failed 3 times for {charge_point_id}, forcing cache update."
                )
                charger_data = await self.fetch_charger_data_from_api()
                valkey_client.setex(
                    CHARGER_DATA_KEY, CACHE_EXPIRY, json.dumps(charger_data)
                )
                self.verification_failures[charge_point_id] = (
                    0  # Reset the failure count
                )

            return False

        return True

    async def send_request(self, charge_point_id, request_method, *args, **kwargs):
        charge_point = self.charge_points.get(charge_point_id)
        if not charge_point:
            logging.error(f"Charge point {charge_point_id} not found.")
            return {"error": "Charge point not found"}

        method = getattr(charge_point, request_method, None)
        if method is None:
            logging.error(
                f"Request method {request_method} not found on ChargePoint {charge_point_id}."
            )
            return {"error": f"Request method {request_method} not found"}

        try:
            async with self.send_request_semaphore:
                response = await method(*args, **kwargs)
            logging.info(
                f"Sent {request_method} to charge point {charge_point_id} with response: {response}"
            )
            return response

        except Exception as e:
            logging.error(
                f"Error sending {request_method} to charge point {charge_point_id}: {e}"
            )
            return {"error": str(e)}

    async def fetch_latest_charger_to_cms_message(self, charge_point_id, message_type):
        """
        Fetch the latest message from the charger-to-CMS table for the given charge point and message type.
        Return the whole entry as JSON, including any dynamic fields.
        """
        try:
            # Use raw SQL to fetch all fields from the table dynamically
            query = """
                SELECT * FROM Charger_to_CMS
                WHERE charger_id = %s AND message_type = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """
            # Execute the query with the parameters
            cursor = db.execute_sql(query, (charge_point_id, message_type))

            # Fetch the first row
            row = cursor.fetchone()

            # If no row is found, return an error message
            if not row:
                return {"error": "No message received from charger."}

            # Get column names dynamically
            column_names = [desc[0] for desc in cursor.description]

            # Convert the row to a dictionary using column names
            message_data = dict(zip(column_names, row))

            # If there is a payload field and it's JSON, deserialize it
            if "payload" in message_data and message_data["payload"]:
                try:
                    message_data["payload"] = json.loads(message_data["payload"])
                except json.JSONDecodeError:
                    # If it's not valid JSON, leave it as-is
                    pass

            # Return the whole entry as a JSON-compatible dictionary
            return message_data

        except Exception as e:
            logging.error(
                f"Error fetching latest message from charger-to-CMS table: {e}"
            )
            return {"error": str(e)}


# Instantiate the central system
central_system = CentralSystem()


# WebSocket endpoint that supports charger_id with slashes
# WebSocket route for connections with both charger_id and serialnumber
@app.websocket("/{charger_id}/{serialnumber}")
async def websocket_with_serialnumber(
    websocket: WebSocket, charger_id: str, serialnumber: str
):
    logging.info(
        f"Charger {charger_id} with serial number {serialnumber} is connecting."
    )
    await central_system.handle_charge_point(websocket, charger_id)


# WebSocket route for connections with only charger_id
@app.websocket("/{charger_id}")
async def websocket_without_serialnumber(websocket: WebSocket, charger_id: str):
    logging.info(f"Charger {charger_id} is connecting without serial number.")
    await central_system.handle_charge_point(websocket, charger_id)


# WebSocket route for frontend connections
@app.websocket("/frontend/ws/{uid}")
async def frontend_websocket(websocket: WebSocket, uid: str):
    try:
        await central_system.handle_frontend_websocket(websocket, uid)
    except WebSocketDisconnect:
        logging.info(f"Frontend WebSocket for {uid} disconnected.")
    except Exception as e:
        logging.error(f"Error during WebSocket connection for {uid}: {e}")
        await websocket.close(code=1011, reason=f"Error: {e}")


# FastAPI request models
class ChangeAvailabilityRequest(BaseModel):
    uid: str
    connector_id: int
    type: str


class StartTransactionRequest(BaseModel):
    uid: str
    id_tag: str
    connector_id: int


class StopTransactionRequest(BaseModel):
    uid: str
    transaction_id: int


class ChangeConfigurationRequest(BaseModel):
    uid: str
    key: str
    value: str


class UnlockConnectorRequest(BaseModel):
    uid: str
    connector_id: int


class GetDiagnosticsRequest(BaseModel):
    uid: str
    location: str
    start_time: Optional[str]
    stop_time: Optional[str]
    retries: Optional[int]
    retry_interval: Optional[int]


class UpdateFirmwareRequest(BaseModel):
    uid: str
    location: str
    retrieve_date: str
    retries: Optional[int]
    retry_interval: Optional[int]


class ResetRequest(BaseModel):
    uid: str
    type: str


class TriggerMessageRequest(BaseModel):
    uid: str
    requested_message: str


class ReserveNowRequest(BaseModel):
    uid: str
    connector_id: int
    expiry_date: str
    id_tag: str
    reservation_id: int


class CancelReservationRequest(BaseModel):
    uid: str
    reservation_id: int


class GetConfigurationRequest(BaseModel):
    uid: str


class StatusRequest(BaseModel):
    uid: str


class ChargerToCMSQueryRequest(BaseModel):
    uid: Optional[str] = None  # Optional UID for filtering by charge_point_id
    filters: Optional[Dict[str, str]] = (
        None  # Optional dictionary of column name and value pairs for filtering
    )
    limit: Optional[str] = None  # Optional limit parameter in the format '1-100'
    start_time: Optional[datetime] = None  # Optional start time for filtering
    end_time: Optional[datetime] = None  # Optional end time for filtering


class CMSToChargerQueryRequest(BaseModel):
    uid: Optional[str] = None  # Optional UID for filtering by charge_point_id
    filters: Optional[Dict[str, str]] = (
        None  # Optional dictionary of column name and value pairs for filtering
    )
    limit: Optional[str] = None  # Optional limit parameter in the format '1-100'
    start_time: Optional[datetime] = None  # Optional start time for filtering
    end_time: Optional[datetime] = None  # Optional end time for filtering


class ChargerAnalyticsRequest(BaseModel):
    start_time: Optional[datetime] = (
        None  # Optional start time for the analytics period
    )
    end_time: Optional[datetime] = None  # Optional end time for the analytics period
    charger_id: Optional[str] = None  # Optional filter by charger_id
    include_charger_ids: Optional[bool] = (
        False  # Whether to include the list of unique charger IDs
    )
    user_id: Optional[str] = None


# REST API endpoints


@app.get("/api/hello")
async def hello():
    """
    A simple endpoint to check if the API is running.
    """
    return {"message": "Helloo, this is the OCPP HAL API. It is running fine."}


# Handle OPTIONS for /api/change_availability
@app.options("/api/change_availability")
async def options_change_availability():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/change_availability")
async def change_availability(request: ChangeAvailabilityRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="change_availability",
        connector_id=request.connector_id,
        type=request.type,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])

    return {"status": response.status}


# Handle OPTIONS for /api/start_transaction
@app.options("/api/start_transaction")
async def options_start_transaction():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/start_transaction")
async def start_transaction(request: StartTransactionRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="remote_start_transaction",
        id_tag=request.id_tag,
        connector_id=request.connector_id,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])

    return {"status": response.status}


# Handle OPTIONS for /api/stop_transaction
@app.options("/api/stop_transaction")
async def options_stop_transaction():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


# @app.post("/api/stop_transaction")
# async def stop_transaction(request: StopTransactionRequest):
#     charge_point_id = request.uid

#     response = await central_system.send_request(
#         charge_point_id=charge_point_id,
#         request_method="remote_stop_transaction",
#         transaction_id=request.transaction_id,
#     )
#     if isinstance(response, dict) and "error" in response:
#         raise HTTPException(status_code=404, detail=response["error"])
#     return {"status": response.status}


@app.post("/api/stop_transaction")
async def stop_transaction(request: Request, response_obj: Response):
    data = await request.json()

    # Extract required fields only
    uid = data.get("uid")
    txn_id_str = data.get("transaction_id")

    if not uid or not txn_id_str:
        raise HTTPException(status_code=400, detail="Missing uid or transaction_id")

    # Parse and sanitize transaction_id
    try:
        transaction_id = int(str(txn_id_str).strip())
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid transaction_id format")

    # Proceed as before
    response = await central_system.send_request(
        charge_point_id=uid,
        request_method="remote_stop_transaction",
        transaction_id=transaction_id,
    )

    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])


    return {"status": response.status}


# Handle OPTIONS for /api/change_configuration
@app.options("/api/change_configuration")
async def options_change_configuration():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/change_configuration")
async def change_configuration(request: ChangeConfigurationRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="change_configuration",
        key=request.key,
        value=request.value,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])

    return {"status": response.status}


# Handle OPTIONS for /api/clear_cache
@app.options("/api/clear_cache")
async def options_clear_cache():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/clear_cache")
async def clear_cache(request: GetConfigurationRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id, request_method="clear_cache"
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])

    return {"status": response.status}


# Handle OPTIONS for /api/unlock_connector
@app.options("/api/unlock_connector")
async def options_unlock_connector():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/unlock_connector")
async def unlock_connector(request: UnlockConnectorRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="unlock_connector",
        connector_id=request.connector_id,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])

    return {"status": response.status}


# Handle OPTIONS for /api/get_diagnostics
@app.options("/api/get_diagnostics")
async def options_get_diagnostics():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/get_diagnostics")
async def get_diagnostics(request: GetDiagnosticsRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="get_diagnostics",
        location=request.location,
        start_time=request.start_time,
        stop_time=request.stop_time,
        retries=request.retries,
        retry_interval=request.retry_interval,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])

    return {"status": response.status}


# Handle OPTIONS for /api/update_firmware
@app.options("/api/update_firmware")
async def options_update_firmware():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/update_firmware")
async def update_firmware(request: UpdateFirmwareRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="update_firmware",
        location=request.location,
        retrieve_date=request.retrieve_date,
        retries=request.retries,
        retry_interval=request.retry_interval,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])

    return {"status": response.status}


# Handle OPTIONS for /api/reset
@app.options("/api/reset")
async def options_reset():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/reset")
async def reset(request: ResetRequest, response_obj: Response):
    # charger_serialnum = await central_system.getChargerSerialNum(request.uid)

    # Form the complete charge_point_id
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="reset",
        type=request.type,
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])

    return {"status": response.status}


# Handle OPTIONS for /api/get_configuration
@app.options("/api/get_configuration")
async def options_get_configuration():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/get_configuration")
async def get_configuration(request: GetConfigurationRequest, response_obj: Response):
    charge_point_id = request.uid

    response = await central_system.send_request(
        charge_point_id=charge_point_id, request_method="get_configuration"
    )
    if isinstance(response, dict) and "error" in response:
        raise HTTPException(status_code=404, detail=response["error"])
    return response  # Return the configuration response as JSON


# Handle OPTIONS for /api/status
@app.options("/api/status")
async def options_status():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


# @app.post("/api/status")
# async def get_charge_point_status(request: StatusRequest, response_obj: Response):

#     # Fetch the latest charger data from cache or API
#     charger_data = await central_system.get_charger_data()

#     # Create a set of active charger IDs from the charger data (API/cache)
#     active_chargers = {f"{item['uid']}" for item in charger_data}

#     # Handle specific chargers or all_online case
#     if request.uid not in ["all", "all_online"]:
#         # Form the complete charge_point_id
#         charge_point_id = request.uid

#         # Ensure the requested charger is still present in the API/cache
#         if charge_point_id not in active_chargers:
#             raise HTTPException(
#                 status_code=404, detail="Charger not found in the system"
#             )

#     else:
#         charge_point_id = request.uid

#     # Handle "all_online" case
#     if charge_point_id == "all_online":
#         all_online_statuses = {}
#         for cp_id, charge_point in central_system.charge_points.items():
#             if (
#                 charge_point.online and cp_id in active_chargers
#             ):  # Only show chargers still present in the API/cache
#                 online_status = (
#                     "Online (with error)" if charge_point.has_error else "Online"
#                 )
#                 connectors = charge_point.state["connectors"]
#                 connectors_status = {}

#                 connectors_status[conn_id] = {
#                     "status": conn_state["status"],
#                     "latest_meter_value": conn_state.get("last_meter_value"),
#                     "latest_transaction_consumption_kwh": conn_state.get(
#                         "last_transaction_consumption_kwh", 0
#                     ),
#                     "error_code": conn_state.get("error_code", "NoError"),
#                     "latest_transaction_id": conn_state.get("transaction_id"),

#                 }

#                 all_online_statuses[cp_id] = {
#                     "status": charge_point.state["status"],
#                     "connectors": connectors_status,
#                     "online": online_status,
#                     "latest_message_received_time": charge_point.last_message_time.isoformat(),
#                 }
#         return all_online_statuses

#     # Handle "all" case - return all chargers (online and offline) present in the API/cache
#     if charge_point_id == "all":
#         all_statuses = {}

#         # Iterate through the charger data (from API/cache)
#         for charger in charger_data:
#             # Get the charger ID from the API/cache
#             charger_id = charger[
#                 "uid"
#             ]  # Use the 'uid' field from the charger data to set charger_id
#             charge_point = central_system.charge_points.get(charger_id)
#             online_status = "Offline"  # Default to offline unless active

#             connectors_status = {}

#             # If the charge point is active, use its real-time status and data
#             if charge_point:
#                 online_status = (
#                     "Online (with error)"
#                     if charge_point.online and charge_point.has_error
#                     else "Online"
#                 )
#                 connectors = charge_point.state["connectors"]

                







                    
               


#                 connectors_status[conn_id] = {
#                     "status": conn_state["status"],
#                     "last_meter_value": conn_state.get("last_meter_value"),
#                     "last_transaction_consumption_kwh": conn_state.get(
#                         "last_transaction_consumption_kwh", 0
#                     ),
#                     "error_code": conn_state.get("error_code", "NoError"),
#                     "transaction_id": conn_state.get("transaction_id"),

#                 }

#             # If the charger is offline, use its last known data from charge_points
#             elif charger_id in central_system.charge_points:
#                 charge_point = central_system.charge_points[charger_id]
#                 online_status = "Offline"
#                 connectors = charge_point.state["connectors"]

#                 for conn_id, conn_state in connectors.items():
#                     connectors_status[conn_id] = {
#                         "status": conn_state["status"],
#                         "last_meter_value": conn_state.get("last_meter_value"),
#                         "last_transaction_consumption_kwh": conn_state.get(
#                             "last_transaction_consumption_kwh", 0
#                         ),
#                         "error_code": conn_state.get("error_code", "NoError"),
#                         "transaction_id": conn_state.get("transaction_id"),
#                     }

#             all_statuses[charger_id] = {
#                 "status": (charge_point.state["status"] if charge_point else "Offline"),
#                 "connectors": connectors_status,
#                 "online": online_status,
#                 "last_message_received_time": (
#                     charge_point.last_message_time.isoformat() if charge_point else None
#                 ),
#             }

#         return all_statuses

#     # Handle specific charge point status request (for specific chargers)
#     if charge_point_id:
#         charge_point = central_system.charge_points.get(charge_point_id)
#         if not charge_point:
#             raise HTTPException(status_code=404, detail="Charge point not found")

#         online_status = (
#             "Online (with error)"
#             if charge_point.online and charge_point.has_error
#             else "Online"
#             if charge_point.online
#             else "Offline"
#         )
#         connectors = charge_point.state["connectors"]
#         connectors_status = {}










#         connectors_status[conn_id] = {
#             "status": conn_state["status"],
#             "latest_meter_value": conn_state.get("last_meter_value"),
#             "latest_transaction_consumption_kwh": conn_state.get(
#                 "last_transaction_consumption_kwh", 0
#             ),
#             "error_code": conn_state.get("error_code", "NoError"),
#             "latest_transaction_id": conn_state.get("transaction_id"),

#         }

#         return {
#             "charger_id": charge_point_id,
#             "status": charge_point.state["status"],
#             "connectors": connectors_status,
#             "online": online_status,
#             "latest_message_received_time": charge_point.last_message_time.isoformat(),
#         }

#     # Handle specific charge point status request (this block will handle only specific charge points, not "all")
#     if charge_point_id and charge_point_id != "all":
#         charge_point = central_system.charge_points.get(charge_point_id)
#         if not charge_point:
#             raise HTTPException(status_code=404, detail="Charge point not found")

#         online_status = (
#             "Online (with error)"
#             if charge_point.online and charge_point.has_error
#             else "Online"
#             if charge_point.online
#             else "Offline"
#         )
#         connectors = charge_point.state["connectors"]
#         connectors_status = {}

        










#         connectors_status[conn_id] = {
#             "status": conn_state["status"],
#             "latest_meter_value": conn_state.get("last_meter_value"),
#             "latest_transaction_consumption_kwh": conn_state.get(
#                 "last_transaction_consumption_kwh", 0
#             ),
#             "error_code": conn_state.get("error_code", "NoError"),
#             "latest_transaction_id": conn_state.get("transaction_id"),

#         }

#         return {
#             "charger_id": charge_point_id,
#             "status": charge_point.state["status"],
#             "connectors": connectors_status,
#             "online": online_status,
#             "latest_message_received_time": charge_point.last_message_time.isoformat(),
#         }

@app.post("/api/status")
async def get_charge_point_status(request: StatusRequest, response_obj: Response):

    try:

        # Fetch the latest charger data from cache or API
        charger_data = await central_system.get_charger_data()

        # Create a set of active charger IDs from the charger data (API/cache)
        active_chargers = {f"{item['uid']}" for item in charger_data}

        uid = request.uid

        # Handle specific chargers: enforce presence in API/cache
        if uid not in ["all", "all_online"]:
            if uid not in active_chargers:
                raise HTTPException(status_code=404, detail="Charger not found in the system")

        # ---- all_online: only show chargers that are online AND still present in API/cache ----
        if uid == "all_online":
            all_online_statuses = {}

            for cp_id, charge_point in central_system.charge_points.items():
                if cp_id not in active_chargers:
                    continue
                if not getattr(charge_point, "online", False):
                    continue

                online_status = "Online (with error)" if charge_point.has_error else "Online"

                connectors = charge_point.state["connectors"]
                connectors_status = {}

                # ✅ FIX: loop so conn_id/conn_state exist
                for conn_id, conn_state in connectors.items():
                    connectors_status[conn_id] = {
                        "status": conn_state["status"],
                        "latest_meter_value": conn_state.get("last_meter_value"),
                        "latest_transaction_consumption_kwh": conn_state.get(
                            "last_transaction_consumption_kwh", 0
                        ),
                        "error_code": conn_state.get("error_code", "NoError"),
                        "latest_transaction_id": conn_state.get("transaction_id"),
                    }

                all_online_statuses[cp_id] = {
                    "status": charge_point.state["status"],
                    "connectors": connectors_status,
                    "online": online_status,
                    "latest_message_received_time": charge_point.last_message_time.isoformat(),
                }

            return all_online_statuses

        # ---- all: return all chargers (online/offline) present in API/cache ----
        if uid == "all":
            all_statuses = {}

            for charger in charger_data:
                charger_id = charger["uid"]
                charge_point = central_system.charge_points.get(charger_id)

                online_status = "Offline"  # default
                connectors_status = {}

                if charge_point:
                    online_status = (
                        "Online (with error)"
                        if charge_point.online and charge_point.has_error
                        else "Online"
                    )

                    connectors = charge_point.state["connectors"]

                    # ✅ FIX: loop so conn_id/conn_state exist
                    for conn_id, conn_state in connectors.items():
                        connectors_status[conn_id] = {
                            "status": conn_state["status"],
                            "last_meter_value": conn_state.get("last_meter_value"),
                            "last_transaction_consumption_kwh": conn_state.get(
                                "last_transaction_consumption_kwh", 0
                            ),
                            "error_code": conn_state.get("error_code", "NoError"),
                            "transaction_id": conn_state.get("transaction_id"),
                        }

                # (This branch in your old code is basically redundant, but keeping behavior-safe)
                elif charger_id in central_system.charge_points:
                    charge_point = central_system.charge_points[charger_id]
                    online_status = "Offline"
                    connectors = charge_point.state["connectors"]

                    for conn_id, conn_state in connectors.items():
                        connectors_status[conn_id] = {
                            "status": conn_state["status"],
                            "last_meter_value": conn_state.get("last_meter_value"),
                            "last_transaction_consumption_kwh": conn_state.get(
                                "last_transaction_consumption_kwh", 0
                            ),
                            "error_code": conn_state.get("error_code", "NoError"),
                            "transaction_id": conn_state.get("transaction_id"),
                        }

                all_statuses[charger_id] = {
                    "status": (charge_point.state["status"] if charge_point else "Offline"),
                    "connectors": connectors_status,
                    "online": online_status,
                    "last_message_received_time": (
                        charge_point.last_message_time.isoformat() if charge_point else None
                    ),
                }

            return all_statuses

        # ---- specific charger ----
        charge_point_id = uid
        charge_point = central_system.charge_points.get(charge_point_id)
        # if not charge_point:
        #     raise HTTPException(status_code=404, detail="Charge point not found")

        if not charge_point:
            return {
                "charger_id": charge_point_id,
                "status": "Offline",
                "connectors": {},
                "online": "Offline",
                "latest_message_received_time": None,
            }

        online_status = (
            "Online (with error)"
            if charge_point.online and charge_point.has_error
            else "Online"
            if charge_point.online
            else "Offline"
        )

        connectors = charge_point.state["connectors"]
        connectors_status = {}

        # ✅ FIX: loop so conn_id/conn_state exist
        for conn_id, conn_state in connectors.items():
            connectors_status[conn_id] = {
                "status": conn_state["status"],
                "latest_meter_value": conn_state.get("last_meter_value"),
                "latest_transaction_consumption_kwh": conn_state.get(
                    "last_transaction_consumption_kwh", 0
                ),
                "error_code": conn_state.get("error_code", "NoError"),
                "latest_transaction_id": conn_state.get("transaction_id"),
            }

        return {
            "charger_id": charge_point_id,
            "status": charge_point.state["status"],
            "connectors": connectors_status,
            "online": online_status,
            "latest_message_received_time": charge_point.last_message_time.isoformat(),
        }
    
    except HTTPException as e:
    # Preserve original HTTP exceptions (404, 401, etc.)
        raise e
    # except Exception as e:
    #     print(e)
    #     raise HTTPException(status_code=500, detail=str(e)) from e

# Handle OPTIONS for /api/trigger_message
@app.options("/api/trigger_message")
async def options_trigger_message():
    return JSONResponse(
        content={},
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, x-api-key, Access-Control-Allow-Origin",
        },
    )


@app.post("/api/trigger_message")
async def trigger_message(request: TriggerMessageRequest, response_obj: Response):
    charge_point_id = request.uid
    requested_message = (
        request.requested_message
    )  # Assuming you want this as the message type

    # Send the trigger message
    response = await central_system.send_request(
        charge_point_id=charge_point_id,
        request_method="trigger_message",
        requested_message=requested_message,
    )

    # Check the type of `response` to ensure it's the expected type
    if hasattr(response, "status"):
        status = response.status
    else:
        raise HTTPException(status_code=500, detail="Failed to trigger message")

    await asyncio.sleep(6)

    # After sending, fetch the latest message from the database
    latest_message = await central_system.fetch_latest_charger_to_cms_message(
        charge_point_id, requested_message
    )



    return {"status": status, "latest_message": latest_message}


def format_duration(seconds):
    """Convert seconds into a human-readable format: years, days, hours, minutes, seconds."""
    years, remainder = divmod(seconds, 31536000)  # 365 * 24 * 60 * 60
    days, remainder = divmod(remainder, 86400)  # 24 * 60 * 60
    hours, remainder = divmod(remainder, 3600)  # 60 * 60
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if years > 0:
        parts.append(f"{int(years)} years")
    if days > 0:
        parts.append(f"{int(days)} days")
    if hours > 0:
        parts.append(f"{int(hours)} hours")
    if minutes > 0:
        parts.append(f"{int(minutes)} minutes")
    if seconds > 0:
        parts.append(f"{int(seconds)} seconds")

    return ", ".join(parts) if parts else "0 seconds"


# @app.post("/api/charger_analytics")
# async def charger_analytics(request: ChargerAnalyticsRequest, response_obj: Response):
#     # Query to get the earliest and latest timestamps from your data tables
#     min_max_time_query = ChargerToCMS.select(
#         fn.MIN(ChargerToCMS.timestamp).alias("min_time"),
#         fn.MAX(ChargerToCMS.timestamp).alias("max_time"),
#     ).first()
#     min_time = min_max_time_query.min_time
#     max_time = min_max_time_query.max_time

#     # Use the earliest and latest timestamps if not provided in the request
#     start_time = request.start_time or (
#         min_time
#         if min_time
#         else datetime.min.replace(tzinfo=timezone.utc, hour=0, minute=0, second=0, microsecond=0)
#     )
#     end_time = request.end_time or (
#         max_time
#         if max_time
#         else datetime.max.replace(tzinfo=timezone.utc, hour=23, minute=59, second=59, microsecond=0)
#     )

#     # Initialize variables to store the results
#     analytics = {}

#     # Initialize variables for cumulative logic
#     total_cumulative_uptime_seconds = 0
#     total_cumulative_transactions = 0
#     total_cumulative_electricity_used = 0
#     total_cumulative_time_occupied_seconds = 0

#     charger_ids = []

#     # Step 1: Handle case when charger_id == "cumulative" (fetch all chargers for user)
#     if request.charger_id == "cumulative":
#         user_id = request.user_id  # Assuming user_id is passed in the body

#         # API call to fetch the chargers for the specific user
#         api_url = config("APIUSERCHARGERDATA")  # API URL from the config
#         apiauthkey = config("APIAUTHKEY")  # API Auth key from config

#         # Post request to fetch user charger data
#         response = requests.post(
#             api_url,
#             headers={"apiauthkey": apiauthkey},
#             json={"get_user_id": user_id},
#             timeout=10,
#         )

#         if response.status_code != 200:
#             return {"error": "Failed to fetch user charger data from external API"}

#         user_charger_data = response.json().get("user_chargerunit_details", [])
#         charger_ids = [charger["uid"] for charger in user_charger_data]

#         # If no chargers found for the user, return null values
#         if not charger_ids:
#             return {
#                 "total_uptime": "0 seconds",
#                 "total_transactions": 0,
#                 "total_electricity_used_kwh": 0,
#                 "message": "No chargers registered for this user.",
#             }
#     else:
#         # When a specific charger_id is provided, use that charger
#         charger_ids = [request.charger_id]  # For a single charger

#     # Step 2: Calculate the uptime for each charger in the list
#     for charger_id in charger_ids:
#         uptime_query = (
#             ChargerToCMS.select(ChargerToCMS.charger_id, ChargerToCMS.timestamp)
#             .where(
#                 (ChargerToCMS.timestamp.between(start_time, end_time))
#                 & (ChargerToCMS.charger_id == charger_id)
#             )
#             .order_by(ChargerToCMS.charger_id, ChargerToCMS.timestamp)
#         )

#         uptime_results = list(uptime_query)

#         # If no data (messages) is found, set the uptime to zero for this charger and continue to the next
#         if not uptime_results:
#             analytics[charger_id] = {
#                 "total_uptime": "0 seconds",
#                 "uptime_percentage": 0,
#                 "total_transactions": 0,
#                 "total_electricity_used_kwh": 0,
#                 "occupancy_rate_percentage": 0,
#                 "average_session_duration": "0 seconds",
#                 "peak_usage_times": "No data available",
#             }
#             continue  # Skip further processing for this charger

#         # Initialize for each charger, ensuring no stale data is carried over
#         charger_uptime_data = {
#             "total_uptime_seconds": 0,
#             "total_possible_uptime_seconds": (end_time - start_time).total_seconds(),
#             "total_time_occupied_seconds": 0,  # Initialize to 0
#             "session_durations": [],
#             "peak_usage_times": [0] * 24,  # 24-hour time slots
#         }

#         previous_timestamp = None
#         first_message = None

#         for row in uptime_results:
#             charger_id, timestamp = row.charger_id, row.timestamp

#             if not first_message:
#                 first_message = timestamp

#             if previous_timestamp:
#                 time_difference = (timestamp - previous_timestamp).total_seconds()

#                 if time_difference <= 30:
#                     # If the time difference is 30 seconds or less, accumulate the uptime
#                     charger_uptime_data["total_uptime_seconds"] += time_difference
#                 else:
#                     # Charger was offline for more than 30 seconds, skip adding to uptime
#                     logging.info(
#                         f"Charger {charger_id} was offline for {time_difference - 30} seconds."
#                     )

#             # Update the previous timestamp for the next iteration
#             previous_timestamp = timestamp

#         # Step 3: Calculate total number of transactions, electricity used, and session-related metrics
#         transaction_summary_query = (
#             Transactions.select(
#                 fn.COUNT(Transactions.id).alias("total_transactions"),
#                 fn.SUM(Transactions.total_consumption).alias("total_electricity_used"),
#                 fn.SUM(
#                     fn.TIMESTAMPDIFF(
#                         SQL("SECOND"),
#                         Transactions.start_time,
#                         Transactions.stop_time,
#                     )
#                 ).alias("total_time_occupied"),
#             )
#             .where(
#                 (Transactions.start_time.between(start_time, end_time))
#                 & (Transactions.charger_id == charger_id)
#             )
#             .first()
#         )

#         total_transactions = transaction_summary_query.total_transactions
#         total_electricity_used = transaction_summary_query.total_electricity_used or 0.0
#         total_time_occupied = (
#             transaction_summary_query.total_time_occupied or 0
#         )  # Ensure total_time_occupied is not None

#         # Add the current charger's metrics to the cumulative values (only if "cumulative" is requested)
#         if request.charger_id == "cumulative":
#             total_cumulative_uptime_seconds += charger_uptime_data[
#                 "total_uptime_seconds"
#             ]
#             total_cumulative_transactions += total_transactions or 0
#             total_cumulative_electricity_used += total_electricity_used
#             total_cumulative_time_occupied_seconds += total_time_occupied or 0

#         # Calculate session durations and peak usage times for each charger
#         session_query = Transactions.select(
#             Transactions.start_time,
#             fn.TIMESTAMPDIFF(
#                 SQL("SECOND"), Transactions.start_time, Transactions.stop_time
#             ).alias("session_duration"),
#         ).where(
#             (Transactions.start_time.between(start_time, end_time))
#             & (Transactions.charger_id == charger_id)
#         )

#         sessions = list(session_query)

#         for session in sessions:
#             start_time = session.start_time
#             session_duration = session.session_duration
#             charger_uptime_data["session_durations"].append(session_duration)
#             hour_of_day = start_time.hour
#             charger_uptime_data["peak_usage_times"][hour_of_day] += 1

#         # Calculate peak usage hours
#         max_usage_count = max(charger_uptime_data["peak_usage_times"])
#         peak_usage_hours = [
#             f"{hour}:00 - {hour + 1}:00"
#             for hour, count in enumerate(charger_uptime_data["peak_usage_times"])
#             if count == max_usage_count
#         ]

#         if max_usage_count == 0:
#             peak_usage_hours = [
#                 "No peak usage times - charger was not used during this period."
#             ]

#         # Compile the results for this charger
#         uptime_seconds = charger_uptime_data["total_uptime_seconds"]
#         uptime_percentage = (
#             round(
#                 (uptime_seconds / charger_uptime_data["total_possible_uptime_seconds"])
#                 * 100,
#                 3,
#             )
#             if uptime_seconds > 0
#             else 0
#         )
#         average_session_duration_seconds = (
#             sum(charger_uptime_data["session_durations"]) / total_transactions
#             if total_transactions > 0
#             else 0
#         )
#         occupancy_rate = (
#             round(
#                 (
#                     total_time_occupied
#                     / charger_uptime_data["total_possible_uptime_seconds"]
#                 )
#                 * 100,
#                 3,
#             )
#             if total_time_occupied > 0
#             else 0
#         )

#         analytics_data = {
#             "charger_id": charger_id,
#             "timestamp": CentralSystem.currdatetime(),
#             "total_uptime": format_duration(uptime_seconds),
#             "uptime_percentage": uptime_percentage,
#             "total_transactions": total_transactions,
#             "total_electricity_used_kwh": total_electricity_used,
#             "occupancy_rate_percentage": occupancy_rate,
#             "average_session_duration": format_duration(
#                 average_session_duration_seconds
#             ),
#             "peak_usage_times": ", ".join(
#                 peak_usage_hours
#             ),  # Store as a comma-separated string
#         }

#         # Save analytics data to the database
#         Analytics.create(**analytics_data)

#         # Add the analytics data to the response
#         analytics[charger_id] = analytics_data

#     # Step 4: Return cumulative analytics if 'cumulative' is requested
#     if request.charger_id == "cumulative":
#         return {
#             "total_uptime": format_duration(total_cumulative_uptime_seconds),
#             "total_transactions": total_cumulative_transactions,
#             "total_electricity_used_kwh": total_cumulative_electricity_used,
#             "total_time_occupied": format_duration(
#                 total_cumulative_time_occupied_seconds
#             ),
#         }

#     # Step 5: Return individual charger analytics

#     return {"analytics": analytics}


@app.post("/api/check_charger_inactivity")
async def check_charger_inactivity(request: StatusRequest, response_obj: Response):
    """
    API endpoint that allows the frontend to check if a charger has been inactive for more than 2 minutes.
    If the charger has been inactive and is still marked as online, it will update the status to offline.
    """
    charge_point_id = request.uid

    # Call the method to check inactivity and update the status if necessary
    result = await central_system.check_inactivity_and_update_status(charge_point_id)


    return result

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=config("F_SERVER_HOST", default="127.0.0.1"),
        port=int(config("F_SERVER_PORT", default=8050)),
        proxy_headers=True,
        reload=False,  # set True in dev
    )
