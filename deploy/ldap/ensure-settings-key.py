"""Create the installation's LDAP settings encryption key once, outside Git/DB."""
from pathlib import Path
import secrets

path = Path(__file__).with_name(".env")
lines = path.read_text().splitlines()
keys = [line.split("=", 1)[1] for line in lines if line.startswith("AUTH_SETTINGS_KEY=")]
if not any(keys):
    lines = [line for line in lines if not line.startswith("AUTH_SETTINGS_KEY=")]
    lines.append("AUTH_SETTINGS_KEY=" + secrets.token_hex(32))
    path.write_text("\n".join(lines) + "\n")
path.chmod(0o600)
