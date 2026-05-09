"""
zerodha_auth.py — Auto-generates a KiteConnect access token using
user ID + password + TOTP. Caches the token for the current trading day
to avoid repeated logins.

Required env vars:
    ZERODHA_API_KEY
    ZERODHA_API_SECRET
    ZERODHA_USER_ID
    ZERODHA_PASSWORD
    ZERODHA_TOTP_SECRET   (base32 TOTP secret from Zerodha's 2FA setup)
"""

import json
import os
import re
from datetime import date
from pathlib import Path

import pyotp
import requests
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()

_TOKEN_CACHE = Path(__file__).resolve().parent.parent / "data" / ".zerodha_token.json"


def _load_cached_token() -> str | None:
    """Return today's cached access token, or None if missing/stale."""
    if not _TOKEN_CACHE.exists():
        return None
    try:
        data = json.loads(_TOKEN_CACHE.read_text())
        if data.get("date") == str(date.today()):
            return data["access_token"]
    except Exception:
        pass
    return None


def _save_token(access_token: str) -> None:
    _TOKEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
    _TOKEN_CACHE.write_text(json.dumps({"date": str(date.today()), "access_token": access_token}))


def _generate_token() -> str:
    """
    Log in to Zerodha programmatically using the web login flow and TOTP,
    then exchange the request_token for an access_token via KiteConnect.
    """
    api_key    = os.environ["ZERODHA_API_KEY"]
    api_secret = os.environ["ZERODHA_API_SECRET"]
    user_id    = os.environ["ZERODHA_USER_ID"]
    password   = os.environ["ZERODHA_PASSWORD"]
    totp_secret = os.environ["ZERODHA_TOTP_SECRET"]

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    # Step 1: POST credentials to Zerodha login API
    login_resp = session.post(
        "https://kite.zerodha.com/api/login",
        data={"user_id": user_id, "password": password},
    )
    login_resp.raise_for_status()
    login_data = login_resp.json()
    if login_data.get("status") != "success":
        raise RuntimeError(f"Zerodha login failed: {login_data.get('message', login_data)}")

    request_id = login_data["data"]["request_id"]

    # Step 2: Submit TOTP for 2FA
    totp_value = pyotp.TOTP(totp_secret).now()
    twofa_resp = session.post(
        "https://kite.zerodha.com/api/twofa",
        data={
            "user_id": user_id,
            "request_id": request_id,
            "twofa_value": totp_value,
            "twofa_type": "totp",
            "skip_session": "",
        },
    )
    twofa_resp.raise_for_status()
    twofa_data = twofa_resp.json()
    if twofa_data.get("status") != "success":
        raise RuntimeError(f"Zerodha 2FA failed: {twofa_data.get('message', twofa_data)}")

    # Step 3: Fetch the KiteConnect login page to get request_token from redirect
    kite = KiteConnect(api_key=api_key)
    login_url = kite.login_url()
    redirect_resp = session.get(login_url, allow_redirects=True)

    # Extract request_token from the final redirect URL
    request_token = None
    for resp in list(redirect_resp.history) + [redirect_resp]:
        match = re.search(r"request_token=([^&\s]+)", resp.url)
        if match:
            request_token = match.group(1)
            break

    if not request_token:
        # Fallback: parse from response text
        match = re.search(r"request_token=([^&\s\"']+)", redirect_resp.text)
        if match:
            request_token = match.group(1)

    if not request_token:
        raise RuntimeError(
            "Could not extract request_token from Zerodha redirect. "
            "Check API key and ensure the app is configured at developers.kite.trade."
        )

    # Step 4: Exchange request_token for access_token
    sess_data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = sess_data["access_token"]
    return access_token


def get_kite_client() -> KiteConnect:
    """
    Return an authenticated KiteConnect client.
    Uses cached access token if available for today; otherwise logs in fresh.
    """
    api_key = os.environ["ZERODHA_API_KEY"]
    access_token = _load_cached_token()

    if not access_token:
        print("Generating new Zerodha access token...")
        access_token = _generate_token()
        _save_token(access_token)
        print("Access token generated and cached.")
    else:
        print("Using cached Zerodha access token.")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


if __name__ == "__main__":
    # Quick test: print profile to confirm auth works
    kite = get_kite_client()
    profile = kite.profile()
    print(f"Logged in as: {profile['user_name']} ({profile['user_id']})")
