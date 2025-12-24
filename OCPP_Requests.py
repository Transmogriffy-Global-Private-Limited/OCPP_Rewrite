import logging
from datetime import datetime, timezone
import uuid
import asyncio
from ocpp.routing import on
from ocpp.v16 import ChargePoint as CP
from ocpp.v16 import call, call_result
import random
from peewee import DoesNotExist, fn, IntegrityError
from models import Transaction
from utils.methods import send_charging_session_transaction_data_to_applicaton_layer
from send_transaction_to_be import add_to_queue
from start_txn_hook_queue import add_to_start_hook_queue, max_energy_limits


class ChargePoint(CP):
    def __init__(self, id, websocket):
        super().__init__(id, websocket)
        self.charger_id = id  # Store Charger_ID
        self.websocket = websocket  # Store WebSocket instance
        self.state = {"status": "Inactive", "connectors": {}}
        self.online = False  # Track if the charger is online
        self.has_error = False  # Track if the charger has an error
        self.last_message_time = (
            self.currdatetime()
        )  # Timestamp of the last received message

    def currdatetime(self):
        return datetime.now(timezone.utc).isoformat()

    def utc_now(self) -> datetime:
        """Always return *aware* UTC datetime."""
        return datetime.now(timezone.utc)

    def delta_wh(self, prev: float, curr: float, rollover: int = 4_294_967_295) -> float:
        """
        Energy delta (Wh) tolerant of counter wrap.
        Works whether the value rolled zero, once, or never.
        """
        if prev is None:
            return 0
        ret = (curr-prev)%(rollover+1)
        return float(ret)


    def update_connector_state(
        self,
        connector_id,
        status=None,
        meter_value=None,
        error_code=None,
        transaction_id=None,
    ):
        if connector_id not in self.state["connectors"]:
            self.state["connectors"][connector_id] = {
                "status": "Unknown",
                "last_meter_value": None,
                "last_transaction_consumption_kwh": 0.0,  # Track last transaction consumption per connector
                "error_code": "NoError",
                "transaction_id": None,
                "last_meter_received": None,
                "watchdog_task": None,
            }
        if status:
            self.state["connectors"][connector_id]["status"] = status
        if meter_value is not None:
            if (
                self.state["connectors"][connector_id]["transaction_id"]
                != transaction_id
            ):
                initial_meter_value = self.state["connectors"][connector_id][
                    "last_meter_value"
                ]
                if initial_meter_value and meter_value:
                    consumption_kwh = (
                        self.delta_wh(curr = float(meter_value), prev= float(initial_meter_value))
                    ) / 1000.0  # Convert Wh to kWh
                    self.state["connectors"][connector_id][
                        "last_transaction_consumption_kwh"
                    ] = consumption_kwh
                    logging.info(
                        f"Connector {connector_id} last transaction consumed {consumption_kwh:.3f} kWh."
                    )
            self.state["connectors"][connector_id]["last_meter_value"] = meter_value
            self.state["connectors"][connector_id]["last_meter_received"] = self.currdatetime()
        if error_code:
            self.state["connectors"][connector_id]["error_code"] = error_code
        if transaction_id is not None:
            self.state["connectors"][connector_id]["transaction_id"] = transaction_id

        # Check for errors
        if error_code and error_code != "NoError":
            self.has_error = True
        else:
            self.has_error = False

    def mark_as_online(self, has_error=False):
        self.online = True
        self.last_message_time = datetime.now(timezone.utc)
        self.has_error = has_error

    async def send(self, message):
        response = await super().send(message)
        self.mark_as_online()  # Update the last message time on receiving a response
        return response

    async def start(self):
        await super().start()
        self.mark_as_online()

    def update_transaction_id(self, connector_id, transaction_id):
        if transaction_id is not None:
            self.update_connector_state(connector_id, transaction_id=transaction_id)

    async def handle_boot_recovery(self):
        """
        Called right after BootNotification ACK.
        Logic per connector:
        • If a transaction_id is recorded:
            – If charger reports status 'Available'  ➜  this is a GHOST session.
                · Push its uuiddb to the normal add_to_queue() so CMS can close it cleanly.
            – In every case, launch RemoteStop + Unlock retry loop to free the latch.
        • If no transaction_id               ➜  just fire an UnlockConnector.
        """
        for cid, state in self.state["connectors"].items():
            tx_id   = state.get("transaction_id")
            status  = state.get("status", "Unknown")

            if tx_id:
                # ── Ghost-TX detector ───────────────────────────────────────────
                if status == "Available":
                    try:
                        tx_row = Transaction.get(
                            (Transaction.charger_id == self.charger_id) &
                            (Transaction.transaction_id == int(tx_id))
                        )
                        logging.warning(
                            f"[{self.charger_id}] 👻 Ghost TX {tx_id} on "
                            f"connector {cid} — forwarding to CMS queue."
                        )
                        # Tell BE to treat it as 'ended'
                        add_to_queue(tx_row.uuiddb)

                    except Transaction.DoesNotExist:
                        logging.error(
                            f"[{self.charger_id}] No DB row for ghost TX {tx_id}"
                        )
                # ── Always try to clean latch afterward ────────────────────────
                asyncio.create_task(self._remote_stop_and_unlock_loop(cid, tx_id))

            else:
                # No TX ⇒ simple unlock to ensure plug is free
                asyncio.create_task(self.unlock_connector(cid))

    async def _remote_stop_and_unlock_loop(
            self,
            connector_id: int,
            transaction_id: int,
            max_total_time: int = 300
        ):
        """Retry RemoteStop + Unlock until success or timeout."""
        delay = 2
        deadline = self.utc_now() + max_total_time

        while self.utc_now() < deadline:
            try:
                stop_resp = await self.remote_stop_transaction(transaction_id)
                if getattr(stop_resp, "status", None) == "Accepted":
                    unlock_resp = await self.unlock_connector(connector_id)
                    if getattr(unlock_resp, "status", None) == "Unlocked":
                        # purge TX from RAM
                        self.update_connector_state(connector_id, transaction_id=None, status="Available")
                        self.state["connectors"][connector_id]["transaction_id"] = None
                        logging.info(f"[{self.charger_id}] 🔓 Connector {connector_id} recovered after reboot.")
                        return
            except Exception as e:
                logging.error(f"[{self.charger_id}] Recovery loop error: {e}")

            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)

        logging.error(f"[{self.charger_id}] ⏰ Failed to recover connector {connector_id} within {max_total_time}s.")

    # Inbound messages from chargers to CMS

    @on("Authorize")
    async def on_authorize(self, **kwargs):
        self.mark_as_online()
        logging.debug(f"Received Authorize request with kwargs: {kwargs}")
        id_tag = kwargs.get("id_tag")

        # Trust yourself, king 👑
        status = "Accepted"

        
        response = call_result.Authorize(id_tag_info={"status": status})
        return response

    @on("BootNotification")
    async def on_boot_notification(self, **kwargs):
        try:
            logging.debug(f"Received BootNotification with kwargs: {kwargs}")
            self.mark_as_online()
            
            response = call_result.BootNotification(
                current_time=self.currdatetime(), interval=900, status="Accepted"
            )
            return response
        finally:
            asyncio.create_task(self.handle_boot_recovery())

    @on("Heartbeat")
    async def on_heartbeat(self, **kwargs):
        logging.debug(f"Received Heartbeat with kwargs: {kwargs}")
        self.mark_as_online()
        
        response = call_result.Heartbeat(current_time=self.currdatetime())
                
        return response

    def generate_unique_transaction_id(self, max_attempts: int = 100) -> int:
        """
        Grab a 32-bit random int, immediately INSERT it to reserve ownership.
        If another CP raced us, the UNIQUE constraint raises IntegrityError
        and we retry with a new number.
        """
        for _ in range(max_attempts):
            tid = random.randint(1, 2_147_483_647)

            try:
                # Write first, ask questions later → race-proof
                Transaction.create(
                    charger_id=self.charger_id,
                    transaction_id=tid,
                    id_tag="__RESERVED__",               # temp placeholder
                    start_time=datetime.now(timezone.utc)
                )
                return tid                               # we own it 🎉

            except IntegrityError:                       # duplicate hit
                continue

        raise RuntimeError("RNG exhausted after 100 attempts – improbable, but fatal.")


    @on("StartTransaction")
    async def on_start_transaction(self, **kwargs):
        self.mark_as_online()
        logging.debug(f"Received StartTransaction with kwargs: {kwargs}")
        
        connector_id = kwargs.get("connector_id")
        id_tag = kwargs.get("id_tag")
        meter_start = kwargs.get("meter_start")

        transaction_id = self.generate_unique_transaction_id()
        kwargs["transaction_id"] = transaction_id  # inject into everything downstream

        print(f"Transaction ID assigned: {transaction_id}")

        # Store the original request
        
        # Create DB transaction record
        rows = (
            Transaction
            .update(
                connector_id = connector_id,
                meter_start  = meter_start,
                start_time   = datetime.now(timezone.utc),
                id_tag       = id_tag,
            )
            .where(
                (Transaction.charger_id     == self.charger_id) &
                (Transaction.transaction_id == transaction_id)
            )
            .execute()
        )

        if not rows:
            raise RuntimeError(f"TX {transaction_id} vanished before update")

        transaction_record = Transaction.get(
        (Transaction.charger_id     == self.charger_id) &
        (Transaction.transaction_id == transaction_id)
    )

        self.update_transaction_id(connector_id, transaction_id)
        self.state["connectors"][connector_id]["transaction_record_id"] = transaction_record.id
        self.state["status"] = "Active"

        self.update_connector_state(
            connector_id,
            status="Charging",
            meter_value=meter_start,
            transaction_id=transaction_id,
        )

        # Return to charger (must be int!)
        response = call_result.StartTransaction(
            transaction_id=transaction_id,
            id_tag_info={"status": "Accepted"}
        )

        add_to_start_hook_queue({
        "transactionid": int(transaction_id),
        "userid": id_tag,
        "chargerid": self.charger_id,
        "connectorid": str(connector_id)
        })

        # Log ACK to CMS DB
        
        return response


    @on("StopTransaction")
    async def on_stop_transaction(self, **kwargs):
        self.mark_as_online()
        logging.debug(f"Received StopTransaction with kwargs: {kwargs}")
        self.mark_as_online()

        connector_id = kwargs.get("connector_id")
        transaction_id = kwargs.get("transaction_id")
        meter_stop = kwargs.get("meter_stop")

        # 🔍 Fallback: resolve connector_id from state using transaction_id
        if connector_id is None:
            for cid, state in self.state["connectors"].items():
                if state.get("transaction_id") == transaction_id:
                    connector_id = cid
                    break

        if connector_id is None:
            logging.error(
                f"[{self.charger_id}] 🔥 Failed to resolve connector_id for transaction_id={transaction_id}"
            )
            return call_result.StopTransaction(id_tag_info={"status": "Rejected"})

        # ✅ Retrieve transaction record ID
        transaction_record_id = self.state["connectors"][connector_id].get("transaction_record_id")

        if transaction_record_id:
            try:
                transaction_record = Transaction.get_by_id(transaction_record_id)
                transaction_record.meter_stop = meter_stop
                transaction_record.stop_time = self.utc_now()
                if meter_stop is not None:
                    transaction_record.total_consumption = (
                        self.delta_wh(curr=float(meter_stop), prev = float(transaction_record.meter_start)) / 1000.0
                    )
                transaction_record.save()

                await send_charging_session_transaction_data_to_applicaton_layer.post_transaction_to_app(
                    transaction_record
                )
                add_to_queue(transaction_record.uuiddb)
                if transaction_id in max_energy_limits:
                    del max_energy_limits[transaction_id]
                    logging.debug(f"[🧹] Cleared max_kWh limit for TX {transaction_id}")

            except Exception as e:
                logging.error(f"[{self.charger_id}] 💀 Error updating transaction record: {e}")

        # 📦 Store raw message
        
        # 🔄 State update
        self.state["status"] = "Inactive"
        self.update_connector_state(
            connector_id,
            status="Available",
            meter_value=meter_stop,
            transaction_id=None,
        )

        self.state["connectors"][connector_id]["transaction_id"] = None
        self.state["connectors"][connector_id].pop("transaction_record_id", None)

        wd = self.state["connectors"][connector_id].pop("watchdog_task", None)
        if wd and not wd.done():
            wd.cancel()

        # ✅ Acknowledge to charger
        response = call_result.StopTransaction(id_tag_info={"status": "Accepted"})
        asyncio.create_task(self.unlock_connector(connector_id))

        return response

    @on("MeterValues")
    async def on_meter_values(self, **kwargs):
        self.mark_as_online()
        logging.debug(f"Received MeterValues with kwargs: {kwargs}")
        connector_id = kwargs.get("connector_id")
        transaction_id = kwargs.get("transaction_id")
        print(f"Transaction ID whose Meter Values I recieved: {transaction_id}")
        meter_values = kwargs.get("meter_value", [])
        
        if meter_values:
            last_entry = meter_values[-1]
            sampled_values = last_entry.get("sampled_value", [])
            print (f"Meter Values recieved: {sampled_values}")
            if sampled_values:
                sv0 = sampled_values[0]
                try:
                    # raw numeric value
                    raw_value = float(sv0.get("value"))

                    # Normalise energy units: if charger reports kWh, convert to Wh
                    unit = (sv0.get("unit")            # official OCPP 1.6J field
                            or sv0.get("unitOfMeasure")  # some vendors use this
                            or "").lower()

                    if unit in {"kwh", "kilowatthour"}:
                        raw_value *= 1000  # kWh ➜ Wh, because backend expects Wh
                        print("Converted kWh → Wh for internal consistency")

                    last_meter_value = raw_value

                except (TypeError, ValueError) as e:
                    logging.error(f"Failed to parse meter value: {sv0.get('value')} | {e}")
                    last_meter_value = None
            else:
                last_meter_value = None

            if last_meter_value is not None:
                self.update_connector_state(
                    connector_id,
                    meter_value=last_meter_value,
                    transaction_id=transaction_id,
                )
                try:
                    tx_row = Transaction.get(
                        (Transaction.charger_id == self.charger_id) &
                        (Transaction.transaction_id == int(transaction_id))
                    )

                    tx_row.meter_stop = last_meter_value           # latest raw Wh
                    tx_row.total_consumption = (
                        self.delta_wh(curr=float(last_meter_value), prev=float(tx_row.meter_start)) / 1000.0
                    )                                              # kWh so far
                    # We intentionally leave stop_time NULL; session isn’t closed yet.
                    tx_row.save()

                except Transaction.DoesNotExist:
                    logging.error(
                        f"[{self.charger_id}] Could not find Transaction row "
                        f"for live TX {transaction_id} on connector {connector_id}"
                    )

            try:
                limit_kwh = max_energy_limits.get(int(transaction_id))
                if limit_kwh is None:
                    print(f"🚫 No energy limit set for TX {transaction_id}")
               
                if limit_kwh is not None:
                    tx = Transaction.get(
                        (Transaction.charger_id == self.charger_id) &
                        (Transaction.transaction_id == int(transaction_id))
                    )

                    print(f"Found Transaction: {tx}")

                    if tx.meter_start is not None:
                        consumed_kwh = self.delta_wh(curr=float(last_meter_value), prev=float(tx_row.meter_start)) / 1000.0

                        print(
                            f"[Limit Check] TX {transaction_id} — Consumed: {consumed_kwh:.3f} / {limit_kwh:.3f} kWh"
                        )

                        if consumed_kwh >= (limit_kwh - 0.05):  # small buffer
                            print(
                                f"[⚡️ CUTOFF] TX {transaction_id} exceeded limit — triggering RemoteStop"
                            )
                            async def _remote_stop(tx_id: int):
                                try:
                                    resp = await self.call(call.RemoteStopTransaction(transaction_id=tx_id))
                                    logging.info("[🟢 RemoteStop ACK] TX %s: %s", tx_id, resp)
                                except Exception as e:
                                    logging.error("Autocutoff failed for TX %s: %s", tx_id, e)

                            # schedule and *do not await*
                            asyncio.create_task(_remote_stop(transaction_id))
            except Exception as e:
                logging.error(f"Limit enforcement failed for TX {transaction_id}: {e}")

        response = call_result.MeterValues()
        return response


    @on("StatusNotification")
    async def on_status_notification(self, **kwargs):
        logging.debug(f"Received StatusNotification with kwargs: {kwargs}")
        error_code = kwargs.get("error_code", "NoError")
        self.mark_as_online(has_error=(error_code != 'NoError'))
        connector_id = kwargs.get("connector_id")
        status = kwargs.get("status")
        transaction_id = kwargs.get("transaction_id")
        
        self.update_connector_state(
            connector_id,
            status=status,
            error_code=error_code,
            transaction_id=transaction_id,
        )

        response = call_result.StatusNotification()
        return response

    @on("DiagnosticsStatusNotification")
    async def on_diagnostics_status_notification(self, **kwargs):
        logging.debug(f"Received DiagnosticsStatusNotification with kwargs: {kwargs}")
        self.mark_as_online()
        
        response = call_result.DiagnosticsStatusNotification()
        return response

    @on("FirmwareStatusNotification")
    async def on_firmware_status_notification(self, **kwargs):
        logging.debug(f"Received FirmwareStatusNotification with kwargs: {kwargs}")
        self.mark_as_online()
        
        response = call_result.FirmwareStatusNotification()
        return response

    # Outbound messages from CMS to Charger
    async def remote_start_transaction(self, id_tag, connector_id):
        logging.debug(
            f"Sending RemoteStartTransaction: id_tag {id_tag}, connector_id {connector_id}"
        )
        request = call.RemoteStartTransaction(
            id_tag=id_tag,
            connector_id=connector_id,
        )

        response = await self.call(request)
        self.mark_as_online()  # Mark as online when a response is received
        logging.debug(f"RemoteStartTransaction response: {response}")

        transaction_id = (
            response.transaction_id if hasattr(response, "transaction_id") else None
        )
        self.update_transaction_id(connector_id, transaction_id)

        return response

    async def remote_stop_transaction(self, transaction_id):
        logging.debug(f"Sending RemoteStopTransaction: transaction_id {transaction_id}")
        request = call.RemoteStopTransaction(transaction_id=transaction_id)

        response = await self.call(request)
        self.mark_as_online()  # Mark as online when a response is received
        logging.debug(f"RemoteStopTransaction response: {response}")

        connector_id = next(
            (
                k
                for k, v in self.state["connectors"].items()
                if v["transaction_id"] == transaction_id
            ),
            None,
        )
        if connector_id:
            self.update_connector_state(connector_id, transaction_id=None)

        return response

    async def change_availability(self, connector_id, type):
        logging.debug(
            f"Sending ChangeAvailability: connector_id {connector_id}, type {type}"
        )
        request = call.ChangeAvailability(
            connector_id=connector_id,
            type=type,
        )

        response = await self.call(request)
        self.mark_as_online()  # Mark as online when a response is received
        logging.debug(f"ChangeAvailability response: {response}")

        return response

    async def change_configuration(self, key, value):
        logging.debug(f"Sending ChangeConfiguration: key {key}, value {value}")
        request = call.ChangeConfiguration(
            key=key,
            value=value,
        )

        response = await self.call(request)
        self.mark_as_online()  # Mark as online when a response is received
        logging.debug(f"ChangeConfiguration response: {response}")

        return response

    async def get_configuration(self):
        logging.debug("Sending GetConfiguration:")
        request = call.GetConfiguration()

        try:
            response = await self.call(request)
            self.mark_as_online()  # Mark as online when a response is received
            logging.debug(f"GetConfiguration response: {response}")

            # Log the types and values of the configuration keys in the response
            if hasattr(response, "configuration_key"):
                logging.debug(f"Configuration keys: {response.configuration_key}")
                for config in response.configuration_key:
                    logging.debug(
                        f"Key: {config.get('key')} Type: {type(config.get('key'))}"
                    )
                    logging.debug(
                        f"Value: {config.get('value')} Type: {type(config.get('value'))}"
                    )
                    logging.debug(
                        f"Readonly: {config.get('readonly')} Type: {type(config.get('readonly'))}"
                    )

                    return response
        except Exception as e:
            logging.error(f"Error in get_configuration: {e}")
            raise

    async def clear_cache(self):
        logging.debug("Sending ClearCache")
        request = call.ClearCache()

        response = await self.call(request)
        self.mark_as_online()  # Mark as online when a response is received
        logging.debug(f"ClearCache response: {response}")

        return response

    async def unlock_connector(self, connector_id):
        logging.debug(f"Sending UnlockConnector: connector_id {connector_id}")
        request = call.UnlockConnector(
            connector_id=connector_id,
        )

        response = await self.call(request)
        self.mark_as_online()  # Mark as online when a response is received
        logging.debug(f"UnlockConnector response: {response}")

        return response

    async def get_diagnostics(
        self,
        location,
        start_time=None,
        stop_time=None,
        retries=None,
        retry_interval=None,
    ):
        logging.debug(
            f"Sending GetDiagnostics: location {location}, start_time={start_time}, stop_time={stop_time}, retries={retries}, retry_interval={retry_interval}"
        )
        request = call.GetDiagnostics(
            location=location,
            start_time=start_time,
            stop_time=stop_time,
            retries=retries,
            retry_interval=retry_interval,
        )

        response = await self.call(request)
        self.mark_as_online()  # Mark as online when a response is received
        logging.debug(f"GetDiagnostics response: {response}")

        return response

    async def update_firmware(
        self, location, retrieve_date, retries=None, retry_interval=None
    ):
        logging.debug(
            f"Sending UpdateFirmware: location {location}, retrieve_date {retrieve_date}, retries={retries}, retry_interval={retry_interval}"
        )
        request = call.UpdateFirmware(
            location=location,
            retrieve_date=retrieve_date,
            retries=retries,
            retry_interval=retry_interval,
        )

        response = await self.call(request)
        self.mark_as_online()  # Mark as online when a response is received
        logging.debug(f"UpdateFirmware response: {response}")

        return response

    async def reset(self, type):
        logging.debug(f"Sending Reset: type {type}")
        request = call.Reset(
            type=type,
        )

        response = await self.call(request)
        self.mark_as_online()  # Mark as online when a response is received
        logging.debug(f"Reset response: {response}")

        return response

    async def trigger_message(self, requested_message):
        request = call.TriggerMessage(requested_message=requested_message)
        response = await self.call(request)
        return response

    async def reserve_now(self, connector_id, expiry_date, id_tag, reservation_id):
        request = call.ReserveNow(
            connector_id=connector_id,
            expiry_date=expiry_date,
            id_tag=id_tag,
            reservation_id=reservation_id,
        )
        response = await self.call(request)
        return response

    async def cancel_reservation(self, reservation_id):
        request = call.CancelReservation(reservation_id=reservation_id)
        response = await self.call(request)
        return response
