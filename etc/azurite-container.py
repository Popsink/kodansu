"""Create a container in Azurite, the one setup step Azure has no `mc` for (#420).

`just s3-up` creates minio's bucket with `mc mb`, which ships inside the minio
image. Azurite ships no client, and the two documented ways to create a
container both cost more than this does:

- `az storage container create --connection-string <azurite>` needs the Azure
  CLI, and `mcr.microsoft.com/azure-cli` is ~500 MB to pull on a job whose whole
  point is being cheap enough to run on every PR;
- `object_store` has no create-container API at all, so the broker cannot do it
  on the way past.

So: one signed `PUT /{account}/{container}?restype=container`. The account and
key are Azurite's well-known development pair, published by Microsoft and
hard-coded in every emulator client — they are not a secret and are of no use
against a real account.

Two things the signature got wrong first time, kept here as comments because
both are silent 403s:

- `Content-Type` is signed, and `urllib` adds
  `application/x-www-form-urlencoded` of its own accord whenever a body is
  passed. Hence `Request(..., method="PUT")` with no `data`.
- `Content-Length` is signed as the **empty string** when it is zero
  (x-ms-version 2015-02-21 and later), not as `"0"`, even though the header
  itself is sent as `0`.

Azurite will tell you which, if you ask: `--debug /dev/stdout` makes it log the
string-to-sign it expected.
"""

import base64, datetime, hashlib, hmac, os, sys, urllib.error, urllib.request

ACCOUNT = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
HOST = os.environ.get("AZURITE_BLOB_STORAGE_URL", "http://127.0.0.1:10000")
CONTAINER = sys.argv[1] if len(sys.argv) > 1 else "tansu"
VERSION = sys.argv[2] if len(sys.argv) > 2 else "2023-11-03"

path = f"/{ACCOUNT}/{CONTAINER}"
query = "restype:container"
now = datetime.datetime.now(datetime.timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

canon_headers = f"x-ms-date:{now}\nx-ms-version:{VERSION}\n"
canon_resource = f"/{ACCOUNT}{path}\n{query}"
to_sign = "\n".join(["PUT", "", "", "", "", "", "", "", "", "", "", ""]) + "\n" + canon_headers + canon_resource
signature = base64.b64encode(
    hmac.new(base64.b64decode(KEY), to_sign.encode("utf-8"), hashlib.sha256).digest()
).decode()

request = urllib.request.Request(f"{HOST}{path}?restype=container", method="PUT")
request.add_header("x-ms-date", now)
request.add_header("x-ms-version", VERSION)
request.add_header("Content-Length", "0")
request.add_header("Authorization", f"SharedKey {ACCOUNT}:{signature}")

try:
    with urllib.request.urlopen(request) as response:
        print(f"created {CONTAINER} ({response.status}) at x-ms-version {VERSION}")
except urllib.error.HTTPError as err:
    body = err.read().decode("utf-8", "replace")[:300]
    if err.code == 409:
        print(f"{CONTAINER} already exists ({err.code}) at x-ms-version {VERSION}")
    else:
        sys.exit(f"container create failed {err.code} at x-ms-version {VERSION}: {body}")
