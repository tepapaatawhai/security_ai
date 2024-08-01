import json
import streamlit as st
from streamlit.web.server.websocket_headers import _get_websocket_headers
import jwt
import base64
import requests



import base64

def get_username_from_header():

    region = 'ap-southeast-2'
    expected_alb_arn = 'arn:aws:elasticloadbalancing:ap-southeast-2:381491951558:loadbalancer/app/prod-F-servi-yX4QRgrovPjP/b6e6e1abd0bf1e5c'

    encoded_jwt = st.context.headers['X-Amzn-Oidc-Data']
    jwt_headers = encoded_jwt.split('.')[0]
    decoded_jwt_headers = base64.b64decode(jwt_headers)
    decoded_jwt_headers = decoded_jwt_headers.decode("utf-8")
    decoded_json = json.loads(decoded_jwt_headers)
    received_alb_arn = decoded_json['signer']

    assert expected_alb_arn == received_alb_arn, "Invalid Signer"

    # Step 2: Get the key id from JWT headers (the kid field)
    kid = decoded_json['kid']

    # Step 3: Get the public key from regional endpoint
    url = 'https://public-keys.auth.elb.' + region + '.amazonaws.com/' + kid
    req = requests.get(url)
    pub_key = req.text

    # Step 4: Get the payload
    payload = jwt.decode(encoded_jwt, pub_key, algorithms=['ES256'])
        
    return payload['given_name']
    

