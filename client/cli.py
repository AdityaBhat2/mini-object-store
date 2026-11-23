#!/usr/bin/env python3
"""CLI client for the Mini Distributed Cloud Object Store.

Usage examples:
  python client/cli.py upload photo.png --meta '{"owner":"alice"}'
  python client/cli.py list --limit 10
  python client/cli.py metadata <object_id>
  python client/cli.py patch <object_id> '{"project":"demo"}'
  python client/cli.py download <object_id> -o out.png
  python client/cli.py delete <object_id>
  python client/cli.py status
"""
import argparse
import json
import mimetypes
import os
import re
import sys

import httpx


def fail(resp: httpx.Response):
    try:
        body = resp.json()
        msg = f"{body.get('error')}: {body.get('detail')}"
    except Exception:
        msg = resp.text[:300]
    print(f"HTTP {resp.status_code} - {msg}", file=sys.stderr)
    sys.exit(1)


def show(obj):
    print(json.dumps(obj, indent=2))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", default=os.getenv("OBJSTORE_URL", "http://localhost:8000"), help="controller base URL")
    sub = p.add_subparsers(dest="cmd", required=True)

    u = sub.add_parser("upload", help="upload a file (POST /api/v1/objects)")
    u.add_argument("path")
    u.add_argument("--meta", help="custom metadata as a JSON object string")
    u.add_argument("--object-id", help="optional explicit object id (409 if it already exists)")

    d = sub.add_parser("download", help="download an object (GET /api/v1/objects/{id})")
    d.add_argument("object_id")
    d.add_argument("-o", "--output", help="output path (default: original file name)")

    m = sub.add_parser("metadata", help="show object metadata")
    m.add_argument("object_id")

    pa = sub.add_parser("patch", help="add/update custom metadata (null value deletes a key)")
    pa.add_argument("object_id")
    pa.add_argument("json_body")

    de = sub.add_parser("delete", help="delete an object")
    de.add_argument("object_id")

    l = sub.add_parser("list", help="list objects")
    l.add_argument("--limit", type=int, default=100)
    l.add_argument("--offset", type=int, default=0)

    sub.add_parser("status", help="cluster / worker / replication status")

    a = p.parse_args()
    base = a.url.rstrip("/")
    with httpx.Client(timeout=httpx.Timeout(120.0, connect=5.0)) as c:
        try:
            if a.cmd == "upload":
                ctype = mimetypes.guess_type(a.path)[0] or "application/octet-stream"
                data = {}
                if a.meta:
                    data["metadata"] = a.meta
                if a.object_id:
                    data["object_id"] = a.object_id
                with open(a.path, "rb") as f:
                    r = c.post(f"{base}/api/v1/objects", data=data,
                               files={"file": (os.path.basename(a.path), f, ctype)})
                if r.status_code != 201:
                    fail(r)
                show(r.json())
            elif a.cmd == "download":
                r = c.get(f"{base}/api/v1/objects/{a.object_id}")
                if r.status_code != 200:
                    fail(r)
                name = a.output
                if not name:
                    mt = re.search(r'filename="([^"]+)"', r.headers.get("content-disposition", ""))
                    name = mt.group(1) if mt else a.object_id
                with open(name, "wb") as f:
                    f.write(r.content)
                print(f"saved {len(r.content)} bytes to {name} ({r.headers.get('content-type')})")
            elif a.cmd == "metadata":
                r = c.get(f"{base}/api/v1/objects/{a.object_id}/metadata")
                r.status_code == 200 or fail(r)
                show(r.json())
            elif a.cmd == "patch":
                try:
                    body = json.loads(a.json_body)
                except json.JSONDecodeError as exc:
                    sys.exit(f"invalid JSON: {exc}")
                r = c.patch(f"{base}/api/v1/objects/{a.object_id}/metadata", json=body)
                r.status_code == 200 or fail(r)
                show(r.json())
            elif a.cmd == "delete":
                r = c.delete(f"{base}/api/v1/objects/{a.object_id}")
                r.status_code == 204 or fail(r)
                print(f"deleted {a.object_id}")
            elif a.cmd == "list":
                r = c.get(f"{base}/api/v1/objects", params={"limit": a.limit, "offset": a.offset})
                r.status_code == 200 or fail(r)
                show(r.json())
            elif a.cmd == "status":
                r = c.get(f"{base}/api/v1/cluster/status")
                r.status_code == 200 or fail(r)
                show(r.json())
        except httpx.ConnectError:
            sys.exit(f"cannot connect to controller at {base} - is the cluster running?")


if __name__ == "__main__":
    main()
