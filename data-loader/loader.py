#!/usr/bin/env python3
import os, json, argparse, time
from typing import Optional, List, Dict, Tuple
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


class TrinoClient:
    def __init__(self, base_url: str, user: str):
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.session = requests.Session()

    def _headers(self, catalog: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, str]:
        h = {
            "X-Trino-User": self.user,
            "X-Trino-Source": "k8s-loader",
        }
        if catalog:
            h["X-Trino-Catalog"] = catalog
        if schema:
            h["X-Trino-Schema"] = schema
        return h

    def execute(self, sql: str, catalog: Optional[str] = None, schema: Optional[str] = None, timeout: int = 60) -> Dict:
        headers = self._headers(catalog, schema)
        r = self.session.post(f"{self.base_url}/v1/statement", data=sql.encode("utf-8"), headers=headers, timeout=timeout)
        r.raise_for_status()
        j = r.json()
        next_uri = j.get("nextUri")
        last = j
        while next_uri:
            r = self.session.get(next_uri, headers=headers, timeout=timeout)
            r.raise_for_status()
            last = r.json()
            next_uri = last.get("nextUri")
        if "error" in last:
            raise RuntimeError(f"Trino query failed: {last['error'].get('message')}\nSQL:\n{sql}")
        return last

    def query_iter(self, sql: str, catalog: Optional[str] = None, schema: Optional[str] = None, timeout: int = 60):
        headers = self._headers(catalog, schema)
        r = self.session.post(f"{self.base_url}/v1/statement", data=sql.encode("utf-8"), headers=headers, timeout=timeout)
        r.raise_for_status()
        j = r.json()

        cols = j.get("columns")
        if cols and j.get("data"):
            yield {"columns": cols, "data": j["data"]}

        next_uri = j.get("nextUri")
        while next_uri:
            r = self.session.get(next_uri, headers=headers, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            if "error" in j:
                raise RuntimeError(f"Trino query failed: {j['error'].get('message')}\nSQL:\n{sql}")
            if j.get("columns"):
                cols = j["columns"]
            if cols and j.get("data"):
                yield {"columns": cols, "data": j["data"]}
            next_uri = j.get("nextUri")

    def query_one(self, sql: str, catalog: Optional[str] = None, schema: Optional[str] = None):
        for batch in self.query_iter(sql, catalog=catalog, schema=schema):
            if batch["data"]:
                return batch["data"][0]
        return None

class ESClient:
    def __init__(self, base_url: str, username: Optional[str], password: Optional[str], insecure: bool):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        if username and password:
            self.session.auth = (username, password)
        self.verify = not insecure
        if insecure:
            try:
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except Exception:
                pass

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{path}"
        kwargs.setdefault("verify", self.verify)
        r = self.session.request(method, url, **kwargs)
        return r
    
    def delete_index_if_exists(self, index: str):
        r = self.request("DELETE", f"/{index}")
        if r.status_code in (200, 404):
            return
        r.raise_for_status()

    def index_exists(self, index: str) -> bool:
        r = self.request("HEAD", f"/{index}")
        if r.status_code == 200:
            return True
        if r.status_code == 404:
            return False
        r.raise_for_status()
        return False

    def wait_for_index(self, index: str, timeout_s: int = 120):
        t0 = time.time()
        while time.time() - t0 < timeout_s:
            if self.index_exists(index):
                return
            time.sleep(1)
        raise RuntimeError(f"Timed out waiting for index {index} to exist")

    def ensure_index(self, index: str, shards: int = 1, replicas: int = 0):
        r = self.request("GET", f"/{index}")
        if r.status_code == 200:
            return
        if r.status_code != 404:
            r.raise_for_status()
        payload = {
            "settings": {
                "number_of_shards": shards,
                "number_of_replicas": replicas,
                "refresh_interval": "-1"
            }
        }
        r = self.request("PUT", f"/{index}", json=payload)
        r.raise_for_status()

    def set_refresh_interval(self, index: str, interval: str):
        r = self.request("PUT", f"/{index}/_settings", json={"index": {"refresh_interval": interval}})
        r.raise_for_status()

    def refresh(self, index: str):
        r = self.request("POST", f"/{index}/_refresh")
        r.raise_for_status()

    def bulk_index(self, index: str, docs: List[Dict], id_field: Optional[str] = None):
        lines = []
        for d in docs:
            action = {"index": {"_index": index}}
            if id_field and id_field in d and d[id_field] is not None:
                action["index"]["_id"] = str(d[id_field])
            lines.append(json.dumps(action))
            lines.append(json.dumps(d, default=str))
        body = "\n".join(lines) + "\n"

        r = self.request("POST", "/_bulk", data=body, headers={"Content-Type": "application/x-ndjson"})
        r.raise_for_status()
        resp = r.json()
        if resp.get("errors"):
            items = resp.get("items", [])[:5]
            raise RuntimeError("ES bulk had errors. First items: " + json.dumps(items, indent=2)[:2000])

def ctas_single(trino_url: str, trino_user: str,
                target_catalog: str, target_schema: str,
                table: str, sf_schema: str) -> Tuple[str, str]:
    """
    Run CTAS for one table. Returns (catalog, table) on success.
    Uses a fresh TrinoClient per call for thread-safety simplicity.
    """
    trino = TrinoClient(trino_url, trino_user)

    trino.execute(f"DROP TABLE IF EXISTS {target_catalog}.{target_schema}.{table}")
    trino.execute(f"""
        CREATE TABLE {target_catalog}.{target_schema}.{table} AS
        SELECT * FROM tpcds.{sf_schema}.{table}
    """)
    return (target_catalog, table)

def ctas_parallel(trino_url: str, trino_user: str,
                  target_catalog: str, target_schema: str,
                  tables: List[str], sf_schema: str,
                  parallelism: int = 4):
    """
    Load multiple tables concurrently via CTAS with bounded parallelism.
    """
    if not tables:
        return

    print(f"[CTAS] catalog={target_catalog} schema={target_schema} tables={len(tables)} parallelism={parallelism}")

    t0 = time.time()
    ok = 0
    with ThreadPoolExecutor(max_workers=parallelism) as ex:
        futs = []
        for t in tables:
            futs.append(ex.submit(ctas_single, trino_url, trino_user, target_catalog, target_schema, t, sf_schema))

        for f in as_completed(futs):
            try:
                cat, tab = f.result()
                ok += 1
                print(f"[CTAS] OK {cat}.{target_schema}.{tab} ({ok}/{len(tables)})")
            except Exception as e:
                # Fail fast: raise after printing context
                print(f"[CTAS] FAILED: {e}")
                raise

    print(f"[CTAS] DONE {target_catalog}.{target_schema} ok={ok}/{len(tables)} elapsed={round(time.time()-t0,2)}s")


def load_es_bucket(trino: TrinoClient, es: ESClient, index: str, sf_schema: str, table: str,
                   key_col: str, buckets: int, bucket_id: int, batch_rows: int):
    src = f"tpcds.{sf_schema}.{table}"

    sample = list(trino.query_iter(f"SELECT * FROM {src} LIMIT 1", catalog="tpcds", schema=sf_schema))
    if not sample:
        print(f"[ES] {src}: no data")
        return
    columns = [c["name"] for c in sample[0]["columns"]]

    cnt = trino.query_one(f"""
        SELECT count(*) FROM {src}
        WHERE mod(
          bitwise_and(from_big_endian_64(xxhash64(to_utf8(cast({key_col} as varchar)))), 9223372036854775807),
          {buckets}
        ) = {bucket_id}

    """, catalog="tpcds", schema=sf_schema)
    total = int(cnt[0]) if cnt else 0
    print(f"[ES] {src}: bucket {bucket_id}/{buckets} rows={total}")

    sql = f"""
        SELECT {", ".join(columns)}
        FROM {src}
        WHERE mod(
          bitwise_and(from_big_endian_64(xxhash64(to_utf8(cast({key_col} as varchar)))), 9223372036854775807),
          {buckets}
        ) = {bucket_id}
    """

    docs: List[Dict] = []
    seen = 0
    t0 = time.time()
    for batch in trino.query_iter(sql, catalog="tpcds", schema=sf_schema):
        for row in batch["data"]:
            docs.append({columns[i]: row[i] for i in range(len(columns))})
            if len(docs) >= batch_rows:
                es.bulk_index(index, docs, id_field=None)
                seen += len(docs)
                docs = []
                if seen and (seen % (batch_rows * 5) == 0):
                    print(f"[ES] {index}: indexed ~{seen} elapsed={round(time.time()-t0,1)}s")
    if docs:
        es.bulk_index(index, docs, id_field=None)
        seen += len(docs)

    print(f"[ES] {index}: done indexed={seen} elapsed={round(time.time()-t0,1)}s")


def drop_table(trino_url: str, trino_user: str, target_catalog: str, target_schema: str, table: str):
    trino = TrinoClient(trino_url, trino_user)
    trino.execute(f"DROP TABLE IF EXISTS {target_catalog}.{target_schema}.{table}")


def drop_tables_parallel(trino_url: str, trino_user: str, target_catalog: str, target_schema: str,
                        tables: List[str], parallelism: int = 4):
    if not tables:
        return
    print(f"[DROP] catalog={target_catalog} schema={target_schema} tables={len(tables)} parallelism={parallelism}", flush=True)
    with ThreadPoolExecutor(max_workers=parallelism) as ex:
        futs = [ex.submit(drop_table, trino_url, trino_user, target_catalog, target_schema, t) for t in tables]
        for f in as_completed(futs):
            f.result()
    print(f"[DROP] DONE {target_catalog}.{target_schema}", flush=True)


def drop_es_index_if_leader(es: ESClient, index: str, drop: bool, bucket_id: int):
    """
    In parallel loads, only bucket 0 should delete the index.
    Others will wait until the index exists.
    """
    if not drop:
        return

    if bucket_id == 0:
        print(f"[ES] drop enabled: deleting index {index}", flush=True)
        es.delete_index_if_exists(index)
    else:
        # Let bucket 0 delete/recreate; avoid thundering herd.
        print(f"[ES] drop enabled: bucket {bucket_id} waiting for index recreate", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=["leader_ctas", "es_load_bucket"])
    ap.add_argument("--trino", required=True)
    ap.add_argument("--trino-user", default="bench")
    ap.add_argument("--sf", type=int, default=10)

    ap.add_argument("--pg-catalog", default="pg")
    ap.add_argument("--mongo-catalog", default="mongo")
    ap.add_argument("--target-schema", default="tpcds")
    ap.add_argument("--ctas-parallelism", type=int, default=4, help="Concurrent CTAS per catalog (pg and mongo)")
    ap.add_argument("--drop", action="store_true", help="Drop existing target tables/indices before loading")
    ap.add_argument("--es", default=None)
    ap.add_argument("--es-index", default=None)
    ap.add_argument("--es-user", default=None)
    ap.add_argument("--es-pass", default=None)
    ap.add_argument("--es-insecure", action="store_true")

    ap.add_argument("--table", default=None)
    ap.add_argument("--key-col", default="ws_order_number")
    ap.add_argument("--buckets", type=int, default=8)
    ap.add_argument("--bucket-id", type=int, default=0)
    ap.add_argument("--batch-rows", type=int, default=20000)
    args = ap.parse_args()

    sf_schema = f"sf{args.sf}"
    target_schema = args.target_schema.replace("sf10", f"sf{args.sf}")

    if args.mode == "leader_ctas":
        dims = [
            "date_dim","time_dim",
            "customer","customer_address","customer_demographics",
            "household_demographics","income_band",
            "item","promotion",
            "store","warehouse","ship_mode",
            "reason","call_center","web_site","web_page","catalog_page"
        ]
        pg_facts = ["store_sales","store_returns"]
        mongo_facts = ["catalog_sales","catalog_returns"]

        all_pg = dims + pg_facts
        all_mongo = mongo_facts

        if args.drop:
            print("[LEADER] Dropping Postgres tables...", flush=True)
            drop_tables_parallel(
                trino_url=args.trino,
                trino_user=args.trino_user,
                target_catalog=args.pg_catalog,
                target_schema=target_schema,
                tables=all_pg,
                parallelism=args.ctas_parallelism,
            )
            print("[LEADER] Dropping Mongo tables...", flush=True)
            drop_tables_parallel(
                trino_url=args.trino,
                trino_user=args.trino_user,
                target_catalog=args.mongo_catalog,
                target_schema=target_schema,
                tables=all_mongo,
                parallelism=args.ctas_parallelism,
            )

        print("[LEADER] Create Schema if not exists into Postgres")
        TrinoClient(args.trino, args.trino_user).execute(f"CREATE SCHEMA IF NOT EXISTS {args.pg_catalog}.{target_schema}")
        TrinoClient(args.trino, args.trino_user).execute(f"CREATE SCHEMA IF NOT EXISTS {args.mongo_catalog}.{target_schema}")

        print("[LEADER] CTAS into Postgres (parallel)...")
        ctas_parallel(
            trino_url=args.trino,
            trino_user=args.trino_user,
            target_catalog=args.pg_catalog,
            target_schema=target_schema,
            tables=dims + pg_facts,
            sf_schema=sf_schema,
            parallelism=args.ctas_parallelism,
        )

        print("[LEADER] CTAS into Mongo (parallel)...")
        ctas_parallel(
            trino_url=args.trino,
            trino_user=args.trino_user,
            target_catalog=args.mongo_catalog,
            target_schema=target_schema,
            tables=mongo_facts,
            sf_schema=sf_schema,
            parallelism=args.ctas_parallelism,
        )

        print("[LEADER] Done.")
        return

    # ES bucket load
    if not args.es or not args.es_index or not args.table:
        raise SystemExit("es_load_bucket requires --es, --es-index, --table")

    trino = TrinoClient(args.trino, args.trino_user)

    es = ESClient(args.es, args.es_user, args.es_pass, args.es_insecure)
    if args.drop and args.bucket_id == 0:
        r = es.request("DELETE", f"/{args.es_index}")
        if r.status_code not in (200, 404):
            r.raise_for_status()

    # Ensure bucket_id is int
    bucket_id = int(args.bucket_id)

    drop_es_index_if_leader(es, args.es_index, args.drop, bucket_id)

    # Create/recreate index
    # (If bucket 0 deleted it, this will recreate it with refresh disabled, replicas=0, etc.)
    es.ensure_index(args.es_index, shards=1, replicas=0)

    # Non-leader buckets should wait until index exists (covers race if bucket 0 is still creating it)
    if args.drop and bucket_id != 0:
        es.wait_for_index(args.es_index, timeout_s=120)

    load_es_bucket(
        trino=trino,
        es=es,
        index=args.es_index,
        sf_schema=sf_schema,
        table=args.table,
        key_col=args.key_col,
        buckets=args.buckets,
        bucket_id=args.bucket_id,
        batch_rows=args.batch_rows,
    )

    es.set_refresh_interval(args.es_index, "1s")
    es.refresh(args.es_index)

if __name__ == "__main__":
    main()
