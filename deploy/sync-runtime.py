from pathlib import Path

src = Path(__file__).resolve().parent
dst = Path.home() / "logister-runtime"

files = {
    "loggen/generate.sh": "loggen/generate.sh",
    "fluent-bit/fluent-bit.conf": "fluent-bit/fluent-bit.conf",
    "fluent-bit/parsers.conf": "fluent-bit/parsers.conf",
    "docker-compose.pipeline.yml": "docker-compose.pipeline.yml",
}

for rel_src, rel_dst in files.items():
    data = (src / rel_src).read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    if rel_src.endswith("docker-compose.pipeline.yml"):
        text = data.decode().replace("./runtime/", "./")
        data = text.encode()
    out = dst / rel_dst
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(data)
    if out.suffix == ".sh":
        out.chmod(0o755)
    has_cr = b"\r" in data
    print(f"wrote {out} cr={has_cr}")
