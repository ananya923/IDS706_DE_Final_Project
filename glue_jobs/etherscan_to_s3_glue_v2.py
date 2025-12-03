import os
import sys
import json
from datetime import datetime, timezone

import boto3
import requests
from awsglue.utils import getResolvedOptions


def get_config():
    """
    Read config from Glue job parameters or environment variables.
    Supports multiple API keys and multiple addresses (comma-separated).
    """
    arg_names = ["RAW_BUCKET_NAME", "ETHERSCAN_API_KEYS", "ETH_ADDRESSES", "CHAIN_ID"]
    glue_args = {}

    try:
        glue_args = getResolvedOptions(sys.argv, arg_names)
        print("Running inside Glue job, using Glue arguments.")
    except Exception:
        print("Not running inside Glue, falling back to environment variables.")

    bucket = glue_args.get("RAW_BUCKET_NAME") or os.getenv(
        "RAW_BUCKET_NAME", "de-27-team11-new"
    )

    api_keys_str = glue_args.get("ETHERSCAN_API_KEYS") or os.getenv(
        "ETHERSCAN_API_KEYS"
    )
    if not api_keys_str:
        raise RuntimeError(
            "ETHERSCAN_API_KEYS is not set. Provide at least one API key."
        )
    api_keys = [k.strip() for k in api_keys_str.split(",") if k.strip()]
    if not api_keys:
        raise RuntimeError("No valid API keys found in ETHERSCAN_API_KEYS.")

    addresses_str = glue_args.get("ETH_ADDRESSES") or os.getenv(
        "ETH_ADDRESSES",
        "0x742d35Cc6634C0532925a3b844Bc454e4438f44e",
    )
    addresses = [a.strip() for a in addresses_str.split(",") if a.strip()]
    if not addresses:
        raise RuntimeError("No ETH addresses provided in ETH_ADDRESSES.")

    chain_id_str = glue_args.get("CHAIN_ID") or os.getenv("CHAIN_ID", "1")
    try:
        chain_id = int(chain_id_str)
    except ValueError:
        raise RuntimeError(f"Invalid CHAIN_ID value: {chain_id_str}")

    return bucket, api_keys, addresses, chain_id


def pick_api_key(api_keys, index):
    """Pick an API key using round-robin based on the index."""
    return api_keys[index % len(api_keys)]


def fetch_transactions(address: str, api_key: str, chain_id: int = 1) -> dict:
    """Call Etherscan API and return full JSON payload."""
    url = (
        "https://api.etherscan.io/v2/api"
        f"?chainid={chain_id}&module=account&action=txlist"
        f"&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={api_key}"
    )
    print(f"Requesting txlist for {address} with API key ending {api_key[-4:]}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data


def build_s3_key(address: str, prefix: str = "raw/ethereum/txlist") -> str:
    """Build partitioned S3 key including the address."""
    addr_clean = address.lower()
    now = datetime.now(timezone.utc)
    return (
        f"{prefix}/address={addr_clean}/{now.year:04d}/{now.month:02d}/{now.day:02d}/"
        f"eth_txlist_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
    )


def upload_json_to_s3(data: dict, bucket: str, key: str):
    """Upload JSON data to S3 using boto3."""
    s3 = boto3.client("s3")
    body = json.dumps(data).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    print(f"Uploaded to s3://{bucket}/{key}")


def main():
    bucket, api_keys, addresses, chain_id = get_config()

    print(f"Using {len(api_keys)} API key(s) for {len(addresses)} address(es).")

    for i, addr in enumerate(addresses):
        api_key = pick_api_key(api_keys, i)
        print(f"=== [{i+1}/{len(addresses)}] Address: {addr} ===")
        payload = fetch_transactions(addr, api_key, chain_id=chain_id)
        num_txs = len(payload.get("result", []))
        print(f"Fetched {num_txs} txs from Etherscan for {addr}")

        s3_key = build_s3_key(addr)
        upload_json_to_s3(payload, bucket, s3_key)


if __name__ == "__main__":
    main()
