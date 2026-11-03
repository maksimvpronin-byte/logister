"""Live LDAP/RBAC acceptance. --setup adds the documented demo mappings.

Uses the running local stack. Creates tagged test messages in payments/billing;
temporarily edits test LDAP membership and restores it in finally blocks.
"""
import argparse
import base64
import http.cookiejar
import json
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
ENV = dict(line.split("=", 1) for line in (ROOT / "deploy/ldap/.env").read_text().splitlines() if line and not line.startswith("#"))
BASE = "http://127.0.0.1:8080"


class Client:
    def __init__(self):
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def call(self, path, body=None, method=None, expected=200, basic=None, origin=None):
        headers = {"Content-Type": "application/json"}
        if basic:
            headers["Authorization"] = "Basic " + base64.b64encode((basic + ":" + ENV["LDAP_TEST_PASSWORD"]).encode()).decode()
        if origin:
            headers["Origin"] = origin
        req = urllib.request.Request(BASE + path, data=None if body is None else json.dumps(body).encode(), method=method, headers=headers)
        try:
            response = self.opener.open(req, timeout=35)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read()
            data = json.loads(raw) if raw else None
            assert response.status == expected, (path, response.status, data)
            return data

    def login(self, name, source="ldap", expected=200):
        return self.call("/api/auth/login", {"username": name, "password": ENV["AUTH_LOCAL_PASSWORD"] if source == "local" else ENV["LDAP_TEST_PASSWORD"], "source": source}, expected=expected)


def ldap_modify(data):
    subprocess.run(["docker", "compose", "-f", str(ROOT / "deploy/ldap/docker-compose.yml"), "exec", "-T", "ldap", "sh", "-c", 'exec ldapmodify -x -H ldap://127.0.0.1 -D "cn=admin,dc=logister,dc=test" -w "$LDAP_ADMIN_PASSWORD"'], input=data, text=True, check=True, capture_output=True)


def setup(admin):
    streams = admin.call("/api/streams")["streams"]
    for name in ("payments", "billing"):
        if not any(s["name"] == name for s in streams):
            admin.call("/api/streams", {"name": name, "title": name.title(), "description": "LDAP role model test stream", "routing": {"enabled": False, "priority": 100, "match": "all", "rules": []}, "storage": {"max_age_hours": 24, "max_size_mb": 1024, "max_documents": 1000000, "retention_days": 0}}, expected=201)
    streams = {s["name"]: s["id"] for s in admin.call("/api/streams")["streams"]}
    groups = {e["name"]: e["id"] for e in admin.call("/api/auth/directory")["entries"] if e["kind"] == "group"}
    catalog = admin.call("/api/auth/grants")
    for name, role, stream in [("LOGISTER-payments-Operators", "operator", "payments"), ("LOGISTER-billing-Operators", "operator", "billing"), ("LOGISTER-Ingest", "service", None), ("LOGISTER-Admins", "administrator", None)]:
        grant = {"principal_kind": "group", "principal_id": groups[name], "role": role, "scope": "stream" if stream else "system"}
        if stream:
            grant["stream_id"] = streams[stream]
        if grant not in catalog["grants"]:
            catalog["grants"].append(grant)
    admin.call("/api/auth/grants", catalog, method="PUT")
    print("PASS: demo streams and UUID-based group mappings configured")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--setup", action="store_true")
    parser.add_argument("--outage", action="store_true", help="briefly stop/restart the local test LDAP")
    args = parser.parse_args()
    anonymous = Client()
    assert anonymous.call("/api/auth/status")["enabled"]
    for path in ["/api/streams", "/api/search", "/api/inputs", "/api/auth/grants", "/api/auth/directory"]:
        anonymous.call(path, expected=401)
    anonymous.login("disabled", expected=401)
    anonymous.call("/api/auth/login", {"username": "operator", "password": "", "source": "ldap"}, expected=401)
    anonymous.call("/api/auth/login", {"username": "*)(uid=*)", "password": "wrong", "source": "ldap"}, expected=401)
    print("PASS: anonymous access, disabled user, empty password and LDAP injection rejected")
    admin = Client()
    admin.login("local-admin", "local")
    if args.setup:
        setup(admin)
    clients = {}
    for name in ["operator", "billing", "service", "administrator", "multi", "no-role"]:
        c = Client()
        c.login(name)
        clients[name] = c
    for name, expected in {"operator": {"payments"}, "billing": {"billing"}, "multi": {"payments", "billing"}, "service": set(), "no-role": set()}.items():
        assert {s["name"] for s in clients[name].call("/api/streams")["streams"]} == expected
        clients[name].call("/api/auth/grants", expected=403)
        clients[name].call("/api/inputs", expected=403)
        clients[name].call("/api/streams", {"name": "forbidden"}, expected=403)
    clients["administrator"].call("/api/auth/grants")
    print("PASS: role union, no-role account, service-only access and administrative boundaries")

    marker = "ldap-auth-" + uuid.uuid4().hex
    for stream in ["payments", "billing"]:
        anonymous.call("/api/streams/" + stream + "/ingest", [{"id": marker + stream + str(i), "message": marker, "stream": stream} for i in range(3)], expected=202, basic="service")
    clients["operator"].call("/api/ingest/json", {"message": marker}, expected=403)
    clients["service"].call("/api/search", expected=403)
    clients["no-role"].call("/api/search", expected=403)
    query = "/api/search?" + urllib.parse.urlencode({"q": '"' + marker + '"', "relative": "1h", "stream": "all", "size": 1})
    deadline = time.monotonic() + 45
    while True:
        page = clients["operator"].call(query)
        if page["total"] == 3:
            break
        if page.get("session_id"):
            clients["operator"].call("/api/search/session", {"session_id": page["session_id"]}, method="DELETE", expected=204)
        assert time.monotonic() < deadline, page
        time.sleep(.5)
    assert {m["stream"] for m in page["messages"]} == {"payments"}
    cursor_path = "/api/search?" + urllib.parse.urlencode({"cursor": page["next_cursor"]})
    clients["billing"].call(cursor_path, expected=410)
    other_stream = "/api/search?" + urllib.parse.urlencode({"q": marker, "stream": "billing"})
    clients["operator"].call(other_stream, expected=403)
    injection = "/api/search?" + urllib.parse.urlencode({"q": 'stream:billing OR "' + marker + '"', "stream": "all", "relative": "1h", "size": 200})
    injected = clients["operator"].call(injection)
    assert all(m["stream"] == "payments" for m in injected["messages"])
    if injected.get("session_id"):
        clients["operator"].call("/api/search/session", {"session_id": injected["session_id"]}, method="DELETE", expected=204)
    print("PASS: authenticated ingestion, stream isolation, query bypass and cross-user cursor rejected")

    group = "cn=LOGISTER-payments-Operators,ou=groups,dc=logister,dc=test"
    user = "uid=operator,ou=people,dc=logister,dc=test"
    try:
        ldap_modify(f"dn: {group}\nchangetype: modify\ndelete: member\nmember: {user}\n")
        assert clients["operator"].call("/api/streams")["streams"] == []
        clients["operator"].call(cursor_path, expected=403)
    finally:
        ldap_modify(f"dn: {group}\nchangetype: modify\nadd: member\nmember: {user}\n")
    assert len(clients["operator"].call("/api/streams")["streams"]) == 1
    print("PASS: removing LDAP membership revokes access on the same session; restored afterward")

    renamed = "cn=LOGISTER-payments-Renamed,ou=groups,dc=logister,dc=test"
    try:
        ldap_modify(f"dn: {group}\nchangetype: modrdn\nnewrdn: cn=LOGISTER-payments-Renamed\ndeleteoldrdn: 1\n")
        assert len(clients["operator"].call("/api/streams")["streams"]) == 1
    finally:
        ldap_modify(f"dn: {renamed}\nchangetype: modrdn\nnewrdn: cn=LOGISTER-payments-Operators\ndeleteoldrdn: 1\n")
    print("PASS: group rename preserves UUID-based role mappings; original name restored")

    catalog = admin.call("/api/auth/grants")
    changed = {**catalog, "grants": [g for g in catalog["grants"] if g["principal_id"] != "local-admin"]}
    admin.call("/api/auth/grants", changed, method="PUT", expected=400)
    admin.call("/api/auth/grants", catalog, method="PUT", origin="https://untrusted.example", expected=403)
    admin.call("/api/auth/grants", catalog, method="PUT")
    admin.call("/api/auth/grants", catalog, method="PUT", expected=409)
    if page.get("session_id"):
        clients["operator"].call("/api/search/session", {"session_id": page["session_id"]}, method="DELETE", expected=204)
    clients["operator"].call("/api/auth/logout", method="POST", expected=204)
    clients["operator"].call("/api/streams", expected=401)
    print("PASS: local admin preserved, CSRF blocked, stale mapping update rejected, logout revokes session")
    if args.outage:
        compose = ["docker", "compose", "-f", str(ROOT / "deploy/ldap/docker-compose.yml")]
        try:
            subprocess.run(compose + ["stop", "ldap"], check=True, capture_output=True)
            clients["billing"].call("/api/streams", expected=503)
            Client().login("operator", expected=503)
            admin.call("/api/auth/grants")
            emergency = Client()
            emergency.login("local-admin", "local")
            emergency.call("/api/auth/grants")
        finally:
            subprocess.run(compose + ["up", "-d", "--wait"], check=True, capture_output=True)
        assert len(clients["billing"].call("/api/streams")["streams"]) == 1
        print("PASS: LDAP outage fails closed; local emergency login works; LDAP restored")


if __name__ == "__main__":
    main()
