"""Live admin-only LDAP settings checks; preserves current users and grants.

Temporarily changes only a trailing slash on the existing LDAP server URL,
verifies persistence through an API restart with --restart, then restores it.
"""
import argparse
import copy
import runpy
from pathlib import Path
import subprocess
import time
import urllib.error

ROOT = Path(__file__).resolve().parents[2]
helpers = runpy.run_path(str(ROOT / "deploy/tests/ldap_auth.py"))
Client, ENV = helpers["Client"], helpers["ENV"]


def draft(settings):
    return {key: settings[key] for key in ("url", "bind_dn", "users_dn", "groups_dn", "revision")}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--restart", action="store_true")
    args = parser.parse_args()
    anonymous = Client()
    for method, path in [("GET", "/api/auth/ldap"), ("PUT", "/api/auth/ldap"), ("POST", "/api/auth/ldap/test")]:
        anonymous.call(path, None if method == "GET" else {}, method=method, expected=401)
    original_operator_streams = []
    for username in ["operator", "service", "no-role"]:
        user = Client()
        user.login(username)
        if username == "operator":
            original_operator_streams = [s["name"] for s in user.call("/api/streams")["streams"]]
        for method, path in [("GET", "/api/auth/ldap"), ("PUT", "/api/auth/ldap"), ("POST", "/api/auth/ldap/test")]:
            user.call(path, None if method == "GET" else {}, method=method, expected=403)
        user.call("/api/auth/logout", method="POST", expected=204)
    admin = Client()
    admin.login("local-admin", "local")
    saved = admin.call("/api/auth/ldap")
    assert saved["password_set"] and "bind_password" not in saved and "password_cipher" not in saved
    grants = admin.call("/api/auth/grants")
    directory_admin = Client()
    directory_admin.login("administrator")
    assert directory_admin.call("/api/auth/ldap") == saved
    print("PASS: LDAP settings and tests require administrator; no password/ciphertext returned")
    result = admin.call("/api/auth/ldap/test", draft(saved))
    assert result == {"users_found": True, "groups_found": True}, result
    bad = dict(draft(saved), bind_password="intentionally-invalid")
    admin.call("/api/auth/ldap/test", bad, expected=422)
    bad = dict(draft(saved), users_dn="ou=missing-base,dc=logister,dc=test")
    admin.call("/api/auth/ldap", bad, method="PUT", expected=422)
    bad = dict(draft(saved), url="ldap://127.0.0.1:1")
    admin.call("/api/auth/ldap", bad, method="PUT", expected=422)
    assert admin.call("/api/auth/ldap") == saved
    print("PASS: draft connectivity/schema check; failed checks leave saved settings unchanged")
    changed = False
    try:
        alternate = copy.deepcopy(draft(saved))
        alternate["url"] = saved["url"].rstrip("/") + "/"
        if alternate["url"] == saved["url"]:
            alternate["url"] = saved["url"].rstrip("/")
        updated = admin.call("/api/auth/ldap", alternate, method="PUT")
        changed = True
        assert updated["url"] == alternate["url"] and updated["revision"] == saved["revision"] + 1
        admin.call("/api/auth/ldap", alternate, method="PUT", expected=409)
        operator = Client()
        operator.login("operator")
        assert [s["name"] for s in operator.call("/api/streams")["streams"]] == original_operator_streams
        operator.call("/api/auth/logout", method="POST", expected=204)
        print("PASS: saved settings apply to sign-in immediately; blank password retained; stale save rejected")
        if args.restart:
            compose = ["docker", "compose", "--env-file", str(ROOT / "deploy/ldap/.env"), "-f", str(ROOT / "docker-compose.yml"), "-f", str(ROOT / "deploy/docker-compose.auth.yml")]
            subprocess.run(compose + ["restart", "api"], check=True, capture_output=True)
            until = time.monotonic() + 30
            while True:
                try:
                    persisted = admin.call("/api/auth/ldap")
                    break
                except (urllib.error.URLError, ConnectionError):
                    assert time.monotonic() < until
                    time.sleep(.3)
            assert persisted == updated, (persisted, updated)
            print("PASS: settings and browser session survive restart; environment does not overwrite settings")
    finally:
        if changed:
            current = admin.call("/api/auth/ldap")
            restore = dict(draft(saved), revision=current["revision"])
            admin.call("/api/auth/ldap", restore, method="PUT")
    assert admin.call("/api/auth/grants") == grants
    current = admin.call("/api/auth/ldap")
    assert draft({**current, "revision": saved["revision"]}) == draft(saved)
    admin.call("/api/auth/logout", method="POST", expected=204)
    directory_admin.call("/api/auth/logout", method="POST", expected=204)
    print("PASS: original connection restored; role mappings preserved")


if __name__ == "__main__":
    main()
