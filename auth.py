#!/usr/bin/env python3

import os, requests, json, jwt, urllib3
from log import print_to_log

def auth_with_dc (debug=False):
	urllib3.disable_warnings()
	
	dc_u = os.environ["DC_USER"]
	dc_pw = os.environ["DC_PW"]
	dc_client_id = os.environ["DC_CLIENT_ID"]
	dc_client_secret = os.environ["DC_CLIENT_SECRET"]
	
	dc_token = ""
	dc_token_url = "https://login.glbrc.org/adfs/oauth2/token"
	dc_grant_data = {'grant_type': 'password','username': dc_u, 'password': dc_pw, 'client_id':dc_client_id}
	dc_access_token_response = requests.post(dc_token_url, data=dc_grant_data, verify=False, allow_redirects=False, auth=(dc_client_id, dc_client_secret))
	dc_tokens = json.loads(dc_access_token_response.text)
	if "id_token" in dc_tokens:
		dc_token = dc_tokens["id_token"]
		dc_token_kid = str(jwt.get_unverified_header(dc_token)['kid'])
		dc_token_aud = str(jwt.decode(dc_token, verify=False)['aud'])
		dc_keys_url = "https://login.glbrc.org/adfs/discovery/keys"
		dc_keys_url_response = requests.get(dc_keys_url, verify=False, allow_redirects=False)
		dc_keys_url_response_json = json.loads(dc_keys_url_response.text)
		dc_public_keys = {}
		for jwk in dc_keys_url_response_json["keys"]:
			dc_kid = str(jwk["kid"])
			dc_public_keys[dc_kid] = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))
		dc_key = dc_public_keys[dc_token_kid]
		if debug == True:
			print(dc_key)
			print(dc_token)
			print(dc_token_kid)
			print(dc_kid)
		payload = None
		try:
			payload = jwt.decode(dc_token, verify=True, key = dc_key, algorithms=["RS256"], audience=dc_token_aud)
		except jwt.InvalidTokenError:
			print_to_log("The obtained Data Catalog token is invalid!","fatal")
			return ""
		if debug == True:
			print_to_log(json.dumps(payload))
			
		return dc_token
		
	else:
		print_to_log("No id_token obtained from Data Catalog!","fatal")
		return ""