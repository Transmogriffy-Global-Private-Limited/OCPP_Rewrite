from pathlib import Path
import re
import shutil
import datetime

ROOT = Path.cwd()
OCPP = ROOT / "OCPP_Requests.py"
MAIN = ROOT / "main.py"

for p in (OCPP, MAIN):
    if not p.exists():
        raise SystemExit(f"Missing {p}. Run this from the OCPP_Rewrite project root.")

ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
for p in (OCPP, MAIN):
    shutil.copy2(p, p.with_suffix(p.suffix + f".bak_{ts}"))

def replace_once(text, old, new, label):
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"Expected exactly 1 match for {label}, found {count}. Aborting before write.")
    return text.replace(old, new, 1)

# -------------------------
# Patch OCPP_Requests.py
# -------------------------
s = OCPP.read_text()
orig = s

s = replace_once(
    s,
    "from datetime import datetime, timezone",
    "from datetime import datetime, timezone, timedelta",
    "datetime import",
)

s = replace_once(
    s,
    """        self.last_message_time = (
            self.currdatetime()
        )  # Timestamp of the last received message""",
    """        self.last_message_time = self.utc_now()  # Timestamp of the last received message""",
    "last_message_time init",
)

s = replace_once(
    s,
    """    def mark_as_online(self, has_error=False):
        self.online = True
        self.last_message_time = datetime.now(timezone.utc)
        self.has_error = has_error

    async def send(self, message):
        response = await super().send(message)
          # Update the last message time on receiving a response
        return response

    async def start(self):
        await super().start()
        
""",
    """    def touch(self):
        self.online = True
        self.last_message_time = datetime.now(timezone.utc)

    def mark_as_online(self, has_error=None):
        self.touch()
        if has_error is not None:
            self.has_error = has_error

    async def send(self, message):
        response = await super().send(message)
        self.touch()
        return response

    async def start(self):
        await super().start()
        self.touch()

""",
    "touch/mark/send/start block",
)

s = replace_once(
    s,
    "        deadline = self.utc_now() + max_total_time",
    "        deadline = self.utc_now() + timedelta(seconds=max_total_time)",
    "boot recovery deadline",
)

handler_names = [
    "on_authorize",
    "on_boot_notification",
    "on_heartbeat",
    "on_start_transaction",
    "on_stop_transaction",
    "on_meter_values",
    "on_status_notification",
    "on_diagnostics_status_notification",
    "on_firmware_status_notification",
]

lines = s.splitlines()
out = []
for i, line in enumerate(lines):
    out.append(line)
    if re.match(r"    async def (" + "|".join(handler_names) + r")\(self, \*\*kwargs\):", line):
        j = i + 1
        while j < len(lines) and lines[j].strip() == "":
            j += 1
        if j >= len(lines) or "self.touch()" not in lines[j]:
            out.append("        self.touch()")

s = "\n".join(out) + ("\n" if s.endswith("\n") else "")

s = replace_once(
    s,
    """        error_code = kwargs.get("error_code", "NoError")
        self.mark_as_online(has_error=(error_code != 'NoError'))
        connector_id = kwargs.get("connector_id")""",
    """        error_code = kwargs.get("error_code", "NoError")
        self.has_error = error_code != "NoError"
        connector_id = kwargs.get("connector_id")""",
    "StatusNotification error handling",
)

old_gen = """    def generate_unique_transaction_id(self, max_attempts: int = 100) -> int:
        \"\"\"
        Grab a 32-bit random int, immediately INSERT it to reserve ownership.
        If another CP raced us, the UNIQUE constraint raises IntegrityError
        and we retry with a new number.
        \"\"\"
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
"""

new_gen = """    def create_transaction_record(
        self,
        connector_id,
        meter_start,
        id_tag,
        is_single_session: bool = False,
        max_attempts: int = 100,
    ) -> Transaction:
        \"\"\"Create the transaction row atomically with all required fields.\"\"\"
        connector_id = int(connector_id)
        meter_start = float(meter_start)

        for _ in range(max_attempts):
            tid = random.randint(1, 2_147_483_647)
            try:
                return Transaction.create(
                    charger_id=self.charger_id,
                    connector_id=connector_id,
                    meter_start=meter_start,
                    start_time=datetime.now(timezone.utc),
                    id_tag=id_tag,
                    transaction_id=tid,
                    is_single_session=is_single_session,
                )
            except IntegrityError:
                continue

        raise RuntimeError("Could not allocate a unique transaction id after 100 attempts.")
"""

s = replace_once(s, old_gen, new_gen, "transaction id reservation/create block")

old_start = """        transaction_id = self.generate_unique_transaction_id()
        kwargs["transaction_id"] = transaction_id

        print(f"Transaction ID assigned: {transaction_id}")

        rows = (
            Transaction
            .update(
                connector_id=connector_id,
                meter_start=meter_start,
                start_time=datetime.now(timezone.utc),
                id_tag=id_tag,
                is_single_session=is_single_session,
            )
            .where(
                (Transaction.charger_id == self.charger_id) &
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
"""

new_start = """        transaction_record = self.create_transaction_record(
            connector_id=connector_id,
            meter_start=meter_start,
            id_tag=id_tag,
            is_single_session=is_single_session,
        )
        transaction_id = transaction_record.transaction_id
        kwargs["transaction_id"] = transaction_id

        print(f"Transaction ID assigned: {transaction_id}")
"""

s = replace_once(s, old_start, new_start, "StartTransaction DB create flow")

# Touch after every successful outbound self.call(...).
s = re.sub(
    r"^(?P<indent>\s+)(?P<var>\w+) = await self\.call\((?P<args>[^\n]+)\)\n(?!\s+self\.touch\(\))",
    r"\g<indent>\g<var> = await self.call(\g<args>)\n\g<indent>self.touch()\n",
    s,
    flags=re.MULTILINE,
)

# update_connector_state ignores transaction_id=None, so clear explicitly after RemoteStop.
s = replace_once(
    s,
    """        if connector_id:
            self.update_connector_state(connector_id, transaction_id=None)

        return response
""",
    """        if connector_id:
            self.state["connectors"][connector_id]["transaction_id"] = None
            self.state["connectors"][connector_id].pop("transaction_record_id", None)

        return response
""",
    "RemoteStop state clear",
)

# Guard MeterValues without transaction_id.
s = replace_once(
    s,
    """        connector_id = kwargs.get("connector_id")
        transaction_id = int(kwargs.get("transaction_id"))
        print(f"Transaction ID whose Meter Values I recieved: {transaction_id}")""",
    """        connector_id = kwargs.get("connector_id")
        raw_transaction_id = kwargs.get("transaction_id")
        if raw_transaction_id is None:
            logging.warning(f"MeterValues without transaction_id from {self.charger_id}; acknowledging without session update")
            return call_result.MeterValues()
        transaction_id = int(raw_transaction_id)
        print(f"Transaction ID whose Meter Values I recieved: {transaction_id}")""",
    "MeterValues transaction_id guard",
)

# Keep connector meter delta updated during same live transaction too.
s = replace_once(
    s,
    """            if (
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
            self.state["connectors"][connector_id]["last_meter_value"] = meter_value""",
    """            initial_meter_value = self.state["connectors"][connector_id]["last_meter_value"]
            if initial_meter_value is not None and meter_value is not None:
                consumption_kwh = (
                    self.delta_wh(curr=float(meter_value), prev=float(initial_meter_value))
                ) / 1000.0  # Convert Wh to kWh
                self.state["connectors"][connector_id][
                    "last_transaction_consumption_kwh"
                ] = consumption_kwh
                logging.info(
                    f"Connector {connector_id} latest meter delta {consumption_kwh:.3f} kWh."
                )
            self.state["connectors"][connector_id]["last_meter_value"] = meter_value""",
    "live meter delta update",
)

if s == orig:
    raise SystemExit("No changes made to OCPP_Requests.py; aborting.")

OCPP.write_text(s)

# -------------------------
# Patch main.py
# -------------------------
m = MAIN.read_text()
orig_m = m

m = replace_once(
    m,
    """                if charge_point:
                    online_status = (
                        "Online (with error)"
                        if charge_point.online and charge_point.has_error
                        else "Online"
                    )
""",
    """                if charge_point:
                    online_status = (
                        "Online (with error)"
                        if charge_point.online and charge_point.has_error
                        else "Online"
                        if charge_point.online
                        else "Offline"
                    )
""",
    "/api/status all online ternary",
)

m = replace_once(
    m,
    """        now   = datetime.now(timezone.utc)
        delta = (now - cp.last_message_time).total_seconds()

        if delta < self.INACTIVITY_LIMIT or not cp.online:""",
    """        now = datetime.now(timezone.utc)
        last_message_time = cp.last_message_time
        if isinstance(last_message_time, str):
            last_message_time = datetime.fromisoformat(last_message_time.replace("Z", "+00:00"))
        delta = (now - last_message_time).total_seconds()

        if delta < self.INACTIVITY_LIMIT or not cp.online:""",
    "inactivity datetime/string guard",
)

if m == orig_m:
    raise SystemExit("No changes made to main.py; aborting.")

MAIN.write_text(m)

# Syntax-only check. This does not import modules, connect DB, or start app.
for p in (OCPP, MAIN):
    compile(p.read_text(), str(p), "exec")

print("✅ Patched OCPP_Requests.py and main.py")
print(f"Backups created:")
print(f"  OCPP_Requests.py.bak_{ts}")
print(f"  main.py.bak_{ts}")
