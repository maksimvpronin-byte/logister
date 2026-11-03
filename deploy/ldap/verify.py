#!/usr/bin/env python3
"""Integration checks against the local, seeded LDAP lab."""
from pathlib import Path
import subprocess

COMPOSE = ["docker", "compose", "-f", str(Path(__file__).with_name("docker-compose.yml"))]
BASE = "dc=logister,dc=test"
ADMIN = f"cn=admin,{BASE}"
READER = f"uid=ldap-reader,ou=services,{BASE}"


def ldap(tool, args, *, dn=READER, secret="LDAP_READER_PASSWORD", data=None, code=0):
    result = subprocess.run(
        COMPOSE + ["exec", "-T", "ldap", "sh", "-c",
                   f'exec "$@" -w "${secret}"', "sh", tool,
                   "-x", "-H", "ldap://127.0.0.1", "-D", dn] + args,
        input=data, text=True, capture_output=True,
    )
    assert result.returncode == code, f"{tool}: expected {code}, got {result.returncode}: {result.stderr}"
    return result.stdout


def search(filter_, *attrs):
    return ldap("ldapsearch", ["-LLL", "-o", "ldif-wrap=no", "-b", BASE, filter_, *attrs])


for uid in ["operator", "billing", "service", "administrator", "multi", "no-role"]:
    ldap("ldapwhoami", [], dn=f"uid={uid},ou=people,{BASE}", secret="LDAP_TEST_PASSWORD")
ldap("ldapwhoami", [], dn=f"uid=disabled,ou=people,{BASE}", secret="LDAP_TEST_PASSWORD", code=49)
ldap("ldapwhoami", [], dn=f"uid=operator,ou=people,{BASE}", secret="LDAP_ADMIN_PASSWORD", code=49)
ldap("ldapwhoami", [], dn=f"uid=operator,ou=people,{BASE}", secret="LDAP_NONEXISTENT_PASSWORD", code=53)
print("PASS: six user binds succeed; disabled user, wrong and empty passwords are rejected")

for uid, count in {"operator": 1, "billing": 1, "service": 1, "administrator": 1, "multi": 3, "no-role": 0}.items():
    record = search(f"(uid={uid})", "uid", "entryUUID", "memberOf", "userPassword")
    assert record.count("memberOf: ") == count, record
    assert "entryUUID: " in record and "userPassword: " not in record, record
assert search("(objectClass=groupOfNames)", "cn").count("\ncn: ") == 4
print("PASS: four groups, expected memberships, stable UUIDs, password hashes hidden")

group = f"cn=LOGISTER-payments-Operators,ou=groups,{BASE}"
user = f"uid=no-role,ou=people,{BASE}"
change = f"dn: {group}\nchangetype: modify\nadd: member\nmember: {user}\n"
ldap("ldapmodify", [], data=change, code=50)
try:
    ldap("ldapmodify", [], dn=ADMIN, secret="LDAP_ADMIN_PASSWORD", data=change)
    assert f"memberOf: {group}" in search("(uid=no-role)", "memberOf")
finally:
    ldap("ldapmodify", [], dn=ADMIN, secret="LDAP_ADMIN_PASSWORD", data=change.replace("add: member", "delete: member"))
assert "memberOf: " not in search("(uid=no-role)", "memberOf")
print("PASS: reader cannot edit groups; memberOf updates on grant and revocation; seed restored")
