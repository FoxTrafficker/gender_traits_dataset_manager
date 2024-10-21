import time
import json
import re
import requests
import configparser
from argparse import ArgumentParser
from base64 import urlsafe_b64encode
from hashlib import sha256
from pprint import pprint
from secrets import token_urlsafe
from sys import exit
from urllib.parse import urlencode
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

config = configparser.ConfigParser()
config.read("config/config.ini")


def oauth_pkce(transform):
    """Proof Key for Code Exchange by OAuth Public Clients (RFC7636)."""

    code_verifier = token_urlsafe(32)
    code_challenge = transform(code_verifier.encode("ascii"))

    return code_verifier, code_challenge


def print_auth_token_response(response):
    data = response.json()

    try:
        access_token = data["access_token"]
        refresh_token = data["refresh_token"]
    except KeyError:
        print("error:")
        pprint(data)
        exit(1)

    print("access_token:", access_token)
    print("refresh_token:", refresh_token)
    print("expires_in:", data.get("expires_in", 0))


def login():
    caps = DesiredCapabilities.CHROME.copy()
    caps["goog:loggingPrefs"] = {"performance": "ALL"}  # enable performance logs

    driver = webdriver.Chrome("./chromedriver", desired_capabilities=caps)

    code_verifier, code_challenge = oauth_pkce(lambda x: urlsafe_b64encode(sha256(data).digest()).rstrip(b"=").decode("ascii"))
    login_params = {
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "client": "pixiv-android",
    }
    print("[INFO] Gen code_verifier:", code_verifier)

    driver.get(f"{config['PixivAPI']['LOGIN_URL']}?{urlencode(login_params)}")

    while True:
        # wait for login
        if driver.current_url[:40] == "https://accounts.pixiv.net/post-redirect":
            break
        time.sleep(1)

    # filter code url from performance logs
    code = None
    for row in driver.get_log('performance'):
        data = json.loads(row.get("message", {}))
        message = data.get("message", {})
        if message.get("method") == "Network.requestWillBeSent":
            url = message.get("params", {}).get("documentURL")
            if url[:8] == "pixiv://":
                code = re.search(r'code=([^&]*)', url).groups()[0]
                break

    driver.close()

    print("[INFO] Get code:", code)

    response = requests.post(
        config['PixivAPI']['AUTH_TOKEN_URL'],
        data={
            "client_id": config['PixivAPI']['CLIENT_ID'],
            "client_secret": config['PixivAPI']['CLIENT_SECRET'],
            "code": code,
            "code_verifier": code_verifier,
            "grant_type": "authorization_code",
            "include_policy": "true",
            "redirect_uri": config['PixivAPI']['REDIRECT_URI'],
        },
        headers={
            "user-agent": config['PixivAPI']['USER_AGENT'],
            "app-os-version": "14.6",
            "app-os": "ios",
        },
    )

    print_auth_token_response(response)


def refresh(refresh_token):
    response = requests.post(
        config['PixivAPI']['AUTH_TOKEN_URL'],
        data={
            "client_id": config['PixivAPI']['CLIENT_ID'],
            "client_secret": config['PixivAPI']['CLIENT_SECRET'],
            "grant_type": "refresh_token",
            "include_policy": "true",
            "refresh_token": refresh_token,
        },
        headers={
            "user-agent": config['PixivAPI']['USER_AGENT'],
            "app-os-version": "14.6",
            "app-os": "ios",
        },
    )
    print_auth_token_response(response)
