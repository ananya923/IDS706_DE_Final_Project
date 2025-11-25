import os
import json
from datetime import datetime

import boto3
import requests


ETHERSCAN_API_KEY = os.getenv(
    "92PHT62WJFSRXFPMJ84JD51GK2NHCI9KJ3", "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
)
ETH_ADDRESS = os.getenv(
    "92PHT62WJFSRXFPMJ84JD51GK2NHCI9KJ3",
    "0x742d35Cc6634C0532925a3b844Bc454e4438f44e",
)
BUCKET = os.getenv("RAW_BUCKET_NAME", "de-27-team11")
CHAIN_ID = 1  # mainnet


def fetch_transactions(address: str, api_key: str, chain_id: int = CHAIN_ID) -> dict:
    """Call Etherscan API and return full JSON payload."""
    url = (
        "https://api.etherscan.io/v2/api"
        f"?chainid={chain_id}&module=account&action=txlist"
        f"&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={api_key}"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data


def build_s3_key(prefix: str = "raw/ethereum/txlist") -> str:
    """Build partitioned S3 key like raw/ethereum/txlist/YYYY/MM/DD/file.json"""
    today = datetime.utcnow()
    return (
        f"{prefix}/{today.year:04d}/{today.month:02d}/{today.day:02d}/"
        f"eth_txlist_{today.strftime('%Y-%m-%dT%H-%M-%S')}.json"
    )


def upload_json_to_s3(data: dict, bucket: str, key: str):
    """Upload JSON data to S3 using boto3 (Glue job role will handle credentials)."""
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
    if ETHERSCAN_API_KEY == "YOUR_ETHERSCAN_API_KEY":
        raise RuntimeError("Please set ETHERSCAN_API_KEY for the Glue job / env.")

    print(f"Fetching transactions for address: {ETH_ADDRESS}")
    payload = fetch_transactions(ETH_ADDRESS, ETHERSCAN_API_KEY)
    num_txs = len(payload.get("result", []))
    print(f"Fetched {num_txs} transactions from Etherscan")

    s3_key = build_s3_key()
    upload_json_to_s3(payload, BUCKET, s3_key)


if __name__ == "__main__":
    main()
