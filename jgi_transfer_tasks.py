#!/usr/bin/env python3
# 
# Name: jgi_transfer_tasks.py
# Author: Jacek Kominek <jkominek@wisc.edu>
# Description: Stage JGI data and sync it to GLBRC servers

import os, sys, argparse, pexpect, json, re, copy
import requests, urllib3, urllib.request, urllib.parse, urllib.error
import cx_Oracle
from collections import defaultdict
from shutil import copyfile, move
from auth import auth_with_dc
from log import print_to_log, date_now, time_now

def exit_gracefully():
	if os.path.exists("/tmp/jgi_transfer_tasks.pid") == True:
		child_exit = pexpect.spawn("rm",["/tmp/jgi_transfer_tasks.pid"])
		child_exit.read()
	else:
		print_to_log("Lockfile removed before a run could complete!", "warn")
	exit()

def get_sample_details(sample_barcode):
	dc_get_sample_details_url = base_dc_url+"/api/v2/datafiles/sample_details"
	try:
		response = requests.get(url=dc_get_sample_details_url, params={"sample_barcode": sample_barcode}, headers=dc_api_call_headers, verify=False)
	except TimeoutError:
		return("Connection timed out!")
	else:
		return(response)

def get_experiment_details(sample_barcodes, experiment_id):
	dc_get_experiment_details_url = base_dc_url+"/api/v2/datafiles/experiment_details"
	barcodes = ",".join(sample_barcodes)
	try:
		if sample_barcodes == []:
			response = requests.get(url=dc_get_experiment_details_url, params={"experiment_id": experiment_id}, headers=dc_api_call_headers, verify=False)
		else:
			dc_api_call_headers2 = copy.deepcopy(dc_api_call_headers)
			dc_api_call_headers2["Content-Type"] = "application/json"
			response = requests.get(url=dc_get_experiment_details_url, data="{ \"sample_barcodes\" : \""+barcodes+"\" }", headers=dc_api_call_headers2, verify=False)
			if args.debug == True:
				print("\nENCODING")
				print(response.encoding)
				print("\nCONTENT")
				print(response.content)
				print("\nREQUEST HEADERS")
				print(response.request.headers)
				print("\nHEADERS")
				print(response.headers)
				print("\nTEXT")
				print(response.text)
				print("\nSTATUS")
				print(response.status_code)
				print("\nURL")
				print(response.url)
				print("\nJSON")
				print(response.json())
				sys.stdout.flush()

	except TimeoutError:
		return("Connection timed out!")
	else:
		return(response)
		
def post_file(filename, sample_barcode, experiment_id):
	dc_post_file_url = base_dc_url+"/api/v2/datafiles"
	path_filename = filename.split("/")[-1]
	path_dir = str("/".join(filename.split("/")[:-1]))+"/"
	stub_file = "/tmp/"+path_filename
	child_post = pexpect.spawn("touch",[stub_file])
	child_post.read()
	with open(stub_file, 'r') as sf:
		try:
			if experiment_id == "":
				response = requests.post(url=dc_post_file_url, files={"file": sf}, params={"sample_barcode": sample_barcode, "description": path_filename, "custom_subpath": path_dir}, headers=dc_api_call_headers, verify=False)
			else:
				response = requests.post(url=dc_post_file_url, files={"file": sf}, params={"experiment_id" : experiment_id, "description": path_filename, "custom_subpath": path_dir}, headers=dc_api_call_headers, verify=False)
			if args.debug == True:
				print("\nENCODING")
				print(response.encoding)
				print("\nCONTENT")
				print(response.content)
				print("\nREQUEST HEADERS")
				print(response.request.headers)
				print("\nHEADERS")
				print(response.headers)
				print("\nTEXT")
				print(response.text)
				print("\nSTATUS")
				print(response.status_code)
				print("\nURL")
				print(response.url)
				print("\nJSON")
				print(response.json())
				sys.stdout.flush()
			child_rm = pexpect.spawn("rm", [stub_file])
			child_rm.read()
		except TimeoutError:
			return("Connection timed out!")
		else:
			return(response)

def stage (force_fd_id):
	fd_ids = []
	sync_status = ""
	sync_status_old = "New"
	if args.intervention == True:
		sync_status_old = "Intervention"
		
	if force_fd_id != "-1":
		fd_ids.append(force_fd_id)
	else:
		cur.execute("SELECT fd_id FROM sync_requests WHERE status = :1 AND num_samples >= 1", (sync_status_old,))
		rows = cur.fetchall()
		for row in rows:
			fd_id = row[0]
			fd_ids.append(fd_id)
		cur.execute("SELECT fd_id FROM sync_requests WHERE status = :1 AND num_samples is null", (sync_status_old,))
		rows = cur.fetchall()
		for row in rows:
			fd_id = row[0]
			print_to_log("No samples in the database for FD_ID "+fd_id+". Intervention required.", "error", no_email=args.no_mail)
			sync_status = "Intervention"
			cur.execute("UPDATE sync_requests SET status = :1, sync_timestamp = :2, updated_at = :3 WHERE fd_id = :4 AND status = :5", (sync_status, time_now(), time_now(), fd_id, sync_status_old))
			con.commit()
	if len(fd_ids) >= 1:
		for fd_id in fd_ids:
			print_to_log(fd_id+" "+"Sync requested.")
					
			#Get portal_id for the fd_id
			p1 = {"parameterName":"jgiProjectId", "parameterValue":fd_id}
			p1 = "&".join("%s=%s" % (k,v) for k,v in list(p1.items()))
			try:
				r1 = s.get("https://genome.jgi.doe.gov/portal/ext-api/genome-admin/getPortalIdByParameter", params=p1, cookies=s.cookies, allow_redirects=True, stream=False)
			except requests.exceptions.Timeout:
				print_to_log("JGI Genome Portal getting PortalID request timed out!","fatal", no_email=args.no_mail)
				exit_gracefully()
			except requests.exceptions.SSLError:
				print_to_log("JGI Genome Portal SSL error!","fatal", no_email=args.no_mail)
				exit_gracefully()
			portal_id = r1.text.strip()
			
			print_to_log(fd_id+" Portal ID acquired. "+portal_id)
			#Report the portal_id to database
				
			if force_fd_id == "-1" or args.force_db == True:
				cur.execute("UPDATE sync_requests SET portal_id = :1 WHERE fd_id = :2 AND status = :3", (portal_id, fd_id, sync_status_old))
				con.commit()
			
			#Request JGI to stage the portal_id data through GLOBUS
			p2 = {"portal":portal_id,"globusName":globus_u,"sendMail": False}
			p2 = "&".join("%s=%s" % (k,v) for k,v in list(p2.items()))
			try:
				r2 = s.post("https://genome.jgi.doe.gov/portal/ext-api/downloads/globus/request", timeout=10, data=p2, cookies=s.cookies, allow_redirects=True, stream=False)
			except requests.exceptions.Timeout:
				print_to_log("JGI Genome Portal staging request timed out!", "fatal", no_email=args.no_mail)
				exit_gracefully()
			except requests.exceptions.SSLError:
				print_to_log("JGI Genome Portal SSL error!","fatal", no_email=args.no_mail)
				exit_gracefully()
			jgi_stage_url = r2.text.strip()
			
			if "Exception while getting ID for Globus user" in jgi_stage_url:
				print_to_log("JGI Error: Exception while getting ID for Globus user.", "fatal", no_email=args.no_mail)
				exit_gracefully()
			if "This service is temporarily unavailable. Please try again later" in jgi_stage_url:
				print_to_log("JGI staging service is temporarily unavailable.", "fatal", no_email=args.no_mail)
				exit_gracefully()
			print_to_log(fd_id+" Data staging requested from JGI. "+jgi_stage_url)
			sync_status = "Staging"
			if force_fd_id == "-1" or args.force_db == True:
				cur.execute("UPDATE sync_requests SET status = :1, sync_timestamp = :2, updated_at = :3, jgi_stage_url = :4 WHERE fd_id = :5 AND status = :6", (sync_status, time_now(), time_now(), jgi_stage_url, fd_id, sync_status_old))
				con.commit()
			elif force_fd_id != "-1":
				return jgi_stage_url
	else:
		print_to_log("No FD_IDs currently with the \"New\" status to process.")
		
def xfer (force_fd_id, force_jgi_stage_url):
	jgi_stage_urls = []
	fd_ids = []
	sync_status = ""
	sync_status_old = "Staging"
	if args.intervention == True:
		sync_status_old = "Intervention"
		
	if force_jgi_stage_url != "" and force_fd_id != "-1":
		jgi_stage_urls.append(force_jgi_stage_url)
		fd_ids.append(force_fd_id)
	elif force_jgi_stage_url == "":
		cur.execute("SELECT fd_id, jgi_stage_url FROM sync_requests WHERE status = :1", (sync_status_old,))
		rows = cur.fetchall()
		for row in rows:
			fd_id = row[0]
			fd_ids.append(fd_id)
			jgi_stage_url = row[1]
			jgi_stage_urls.append(jgi_stage_url)
	child0 = pexpect.spawn(globus_bin, ["task","list","--format","json","--limit","100","--filter-status","ACTIVE"], encoding='utf-8')
	child0_out = child0.read()
	if args.debug == True:
		print(child0_out)
	task0_json = json.loads(child0_out)
	task_limit = 100 - int(len(task0_json["DATA"]))
	if args.debug == True:
		print(str(len(fd_ids)))
	if len(jgi_stage_urls) >= 1 and len(fd_ids) >= 1:
		index = 1	
		for fd_id, jgi_stage_url in zip(fd_ids, jgi_stage_urls):
			if index >= task_limit:
				print_to_log("Globus concurrent transfer task limit reached. Better luck next cycle.", "error", no_email=args.no_mail)
				break
			if "http" in jgi_stage_url:
				try:
					r1 = s.get(jgi_stage_url, timeout=10, cookies=s.cookies, allow_redirects=True, stream=False)
				except requests.exceptions.Timeout:
					print_to_log("JGI Genome Portal staging request URL access timed out!", "fatal", no_email=args.no_mail)
					exit_gracefully()
				except requests.exceptions.SSLError:
					print_to_log("JGI Genome Portal SSL error!","fatal", no_email=args.no_mail)
					exit_gracefully()
				if "Download request completed." in r1.text and "No data are available for download." not in r1.text:
					globus_stage_url = r1.text[r1.text.find("https:"):]
					params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(globus_stage_url).query))
					globus_stage_endpoint = params["origin_id"]
					globus_stage_path = params["origin_path"]
					
					print_to_log(fd_id+" Data staging successful.")
					if force_jgi_stage_url == "" or args.force_db == True:
						cur.execute("UPDATE sync_requests SET globus_stage_url = :1, globus_stage_endpoint = :2, globus_stage_path = :3 WHERE fd_id = :4 AND status = :5", (globus_stage_url, globus_stage_endpoint, globus_stage_path, fd_id, sync_status_old))
						con.commit()
						
					glbrc_destination_path = globus_stage_path

					#Login tokens for GLOBUS are valid for 6 months but should get refreshed every time the Globus CLI is used
					#Login tokens must be acquired with a browser, if we do not want to go through the API (we don't)
					#so if we're not logged in, there is no sense in proceeding, hence the forced exit.
					child0 = pexpect.spawn(globus_bin, ["whoami"], encoding='utf-8')
					
					login_status = str([child0.read()][0])
					if args.debug == True:
						print(login_status)
					if "Please try logging in again" in login_status:
						print_to_log("Globus error: Not signed into Globus. Log in first and then restart.", "fatal", no_email=args.no_mail)
						exit_gracefully()
						
					#Check if the GLBRC endpoint is activated, reactivate if it is not. Use pexpect because it allows passing silent passwords 
					child1 = pexpect.spawn(globus_bin, ["endpoint","is-activated","--format","json",glbrc_destination_endpoint], encoding='utf-8')
					child1_out = child1.read()
					task1_json = json.loads(child1_out.replace("Exit: \r\n",""))
					if task1_json["activated"] == False:
						print_to_log(fd_id+" GLBRC Endpoint not active, attempting reactivation.", "warn")
						child2 = pexpect.spawn(globus_bin, ["endpoint","activate","--format","json","--myproxy",glbrc_destination_endpoint,"--myproxy-lifetime","168"], encoding='utf-8')
						child2.expect("Myproxy username:")
						child2.sendline(globus_myproxy_u+"\n")
						child2.expect("Myproxy password:")
						child2.sendline(globus_myproxy_pw+"\n")
						child2_out = child2.read()
						task2_json = json.loads(child2_out.replace("Exit: \r\n",""))
						if task2_json["code"] != "Activated.MyProxyCredential":
							print_to_log("Globus error: Endpoint reactivation failed, cannot transfer data.", "fatal", no_email=args.no_mail)
							exit_gracefully()
						elif task2_json["code"] == "Activated.MyProxyCredential":
							print_to_log(fd_id+" Endpoint succesfully reactivated, proceeding with transfer.")
					else:
						print_to_log(fd_id+" GLBRC endpoint active, proceeding with transfer.")
					
					#Launch the transfer via GLOBUS
					transfer_label = "GLBRC JGI Data Sync "+time_now()
					transfer_label = transfer_label.replace(":","_").replace(".","_").replace("-","_").replace(" ","_")
					transfer_params = ["transfer","--preserve-mtime","--deadline",date_now(add_days=7),"--format","json",globus_stage_endpoint+":"+globus_stage_path,glbrc_destination_endpoint+":"+tmp_path+"/"+glbrc_destination_path.split("/")[3]+"/","--recursive", "--label",transfer_label,"--notify","off"]
					child3 = pexpect.spawn(globus_bin, transfer_params, encoding='utf-8')
					child3_out = child3.read()
					task3_json = json.loads(child3_out.replace("Exit: \r\n",""))
					if task3_json["code"] != "Accepted":
						print_to_log(fd_id+" Transfer request failed. Transfer params:\n"+transfer_params+"\n"+child3_out, "fatal", no_email=args.no_mail)
						exit_gracefully()
					elif task3_json["code"] == "Accepted":
						globus_transfer_task_id = task3_json["task_id"]
						print_to_log(fd_id+" Transfer succesfully submitted. Transfer ID: "+globus_transfer_task_id)
						sync_status = "Downloading"
						index += 1
						if force_jgi_stage_url == "" or args.force_db == True:
							cur.execute("UPDATE sync_requests SET status = :1, sync_timestamp = :2, updated_at = :3, globus_transfer_task_id = :4, globus_transfer_task_label = :5 WHERE fd_id = :6 AND status = :7", (sync_status, time_now(), time_now(), globus_transfer_task_id, transfer_label, fd_id, sync_status_old))
							con.commit()
						elif force_jgi_stage_url != "" or args.force_db == True:
							return globus_transfer_task_id			
				elif "Download request completed." in r1.text and "No data are available for download." in r1.text:
					print_to_log(fd_id+" No data available for download")
				elif "Download request is being processed." in r1.text:
					print_to_log(fd_id+" Staging request in progress")
				elif "Download request has been submitted." in r1.text:
					print_to_log(fd_id+" Staging request has been submitted.")
				elif "Download request failed." in r1.text:
					print_to_log(fd_id+" Staging request failed. Reverting status to \"New\" to retry next cycle. Error:\n"+r1.text, "error", no_email=args.no_mail)
					sync_status = "New"
					cur.execute("UPDATE sync_requests SET status = :1, sync_timestamp = :2, updated_at = :3 WHERE fd_id = :4 AND status = :5", (sync_status, time_now(), time_now(), fd_id, sync_status_old))
					con.commit()
	else:
		print_to_log("No FD_IDs currently with the \"Staging\" status to process.")
		
def post (force_fd_id, force_sample_id, force_experiment_id, force_globus_transfer_task_id):
	globus_transfer_task_ids = []
	fd_ids = []
	sync_status = ""
	sync_status_old = "Downloading"
	if args.intervention == True:
		sync_status_old = "Intervention"
		
	if force_fd_id != "-1":
		fd_ids.append(force_fd_id)
	if force_globus_transfer_task_id != "":
		globus_transfer_task_ids.append(force_globus_transfer_task_id)
	if force_fd_id == "-1" and force_globus_transfer_task_id == "":
		cur.execute("SELECT fd_id, globus_transfer_task_id FROM sync_requests WHERE status = :1", (sync_status_old,))
		rows = cur.fetchall()
		for row in rows:
			fd_id = row[0]
			fd_ids.append(fd_id)
			globus_transfer_task_id = row[1]
			globus_transfer_task_ids.append(globus_transfer_task_id)
	
	if len(fd_ids) >= 1 and len(globus_transfer_task_ids) >= 1:
		for fd_id, globus_transfer_task_id in zip(fd_ids, globus_transfer_task_ids):
			#Check status of the GLOBUS transfers. If downloading successful, go through the list
			#of transferred files and post them into Data Catalog and move to the target location
			child1 = pexpect.spawn(globus_bin, ["task","show","--format","json",globus_transfer_task_id], encoding='utf-8')
			child1_out = child1.read()
			child1_json = json.loads(child1_out)
			if child1_json["status"] == "SUCCEEDED":
				print_to_log(fd_id+" All files successfully downloaded. Posting to Data Catalog.")
				
				globus_task_check = pexpect.spawn(globus_bin, ["task","show","--format","json", globus_transfer_task_id], encoding='utf-8')
				globus_task_check_out = globus_task_check.read()
				globus_task_check_out_json = json.loads(globus_task_check_out)
				if globus_task_check_out_json["history_deleted"] == True:
					print_to_log(fd_id+" Detailed history of the Globus transfer task was deleted, cannot proceed with post, aborting the current FD_ID and changing its status to 'New', for re-staging.", "error", no_email=args.no_mail)
					sync_status = "New"
					if cur != None and con != None:
						cur.execute("UPDATE sync_requests SET status = :1, sync_timestamp = :2, updated_at = :3 WHERE fd_id = :4 AND status = :5", (sync_status, time_now(), time_now(), fd_id, sync_status_old))
						con.commit()
					continue
				
				child2 = pexpect.spawn(globus_bin, ["task","show","--format","json","--successful-transfers", globus_transfer_task_id], encoding='utf-8')
				child2_out = child2.read()
				child2_json_files = json.loads(child2_out)
				child2_json_files_posted = defaultdict(bool)
				child2_json_files_moved = defaultdict(bool)
				child2_json_files_replaced = defaultdict(bool)
				
				sample_id = ""
				experiment_id = ""
				existing_files = []
				existing_files_full = defaultdict(str)
				sids = []
				
				if args.force_sample_ids != []:
					sids = args.force_sample_ids
				elif force_sample_id != "-1" and force_experiment_id != "-1":
					print_to_log(fd_id+" Both sample_id and experiment_id provided, both cannot be specified at the same time, aborting the current FD_ID.", "error", no_email=args.no_mail)
					continue
				elif force_sample_id != "-1" and force_experiment_id == "-1":
					sids.append(force_sample_id)
				elif force_sample_id == "-1" and force_experiment_id != "-1":
					experiment_id = force_experiment_id
				elif force_sample_id == "-1" and force_experiment_id == "-1":
					if cur != None and con != None:
						cur.execute("SELECT fd.fd_id, s.sample_id FROM final_deliverables fd, samples s WHERE s.final_deliverable_id = fd.id AND fd.fd_id = :1", (fd_id,))
						rows = cur.fetchall()
						for row in rows:
							sid = row[1]
							sids.append(str(sid))
						if sids == []:
							print_to_log(fd_id+" No samples associated with current FD_ID in the database. Aborting the current FD_ID.", "error", no_email=args.no_mail)
							continue
	
				if args.debug == True:
					print(str(" ".join(["\nsids"]+sids+["\n"])))
					print(str(" ".join(["\nexperiment_id",experiment_id,"\n"])))
					
				if experiment_id == "" and sids == []:								
					print_to_log(fd_id+" No sample IDs or experiment_id to process. Aborting the current FD_ID.", "error", no_email=args.no_mail)
					continue	
				elif experiment_id != "" and sids == []:
					experiment_details = get_experiment_details(sample_barcodes=sids, experiment_id=experiment_id)
					if experiment_details.status_code != 200:
						print_to_log(fd_id+" Experiment details request for ID "+experiment_id+" failed or data absent from Data Catalog, aborting the current FD_ID. Intervention required. \n"+experiment_details.text, "error", no_email=args.no_mail)
						sync_status = "Intervention"
						if cur != None and con != None:
							cur.execute("UPDATE sync_requests SET status = :1, sync_timestamp = :2, updated_at = :3 WHERE fd_id = :4 AND status = :5", (sync_status, time_now(), time_now(), fd_id, sync_status_old))
							con.commit()
						continue
					else:	
						experiment_details_json = experiment_details.json()
						if args.debug == True:
							print(str(" ".join(["\nExperiment details",json.dumps(experiment_details_json),"\n"])))
						if len(experiment_details_json) >= 1:
							if len(experiment_details_json[0]["files"]) >= 1 and len(experiment_details_json[0]["files"]["subpaths"]) >= 1 and len(experiment_details_json[0]["files"]["fullpaths"]) >= 1:
								existing_files = experiment_details_json[0]["files"]["subpaths"]
								existing_files_full = experiment_details_json[0]["files"]["fullpaths"]
						else:
							print_to_log(fd_id+" Experiment details request for ID "+experiment_id+" failed or data absent from Data Catalog, aborting the current FD_ID. Intervention required. \n"+experiment_details.text, "error", no_email=args.no_mail)
							sync_status = "Intervention"
							if cur != None and con != None:
								cur.execute("UPDATE sync_requests SET status = :1, sync_timestamp = :2, updated_at = :3 WHERE fd_id = :4 AND status = :5", (sync_status, time_now(), time_now(), fd_id, sync_status_old))
								con.commit()
							continue
				elif experiment_id == "" and len(sids) == 1:
					sample_id = str(sids[0])
					sample_details = get_sample_details(sample_id)
					if sample_details.status_code != 200:
						print_to_log(fd_id+" Sample details request for ID "+sample_id+" failed or data absent from Data Catalog, aborting the current FD_ID. Intervention required.\n"+sample_details.text, "error", no_email=args.no_mail)
						sync_status = "Intervention"
						if cur != None and con != None:
							cur.execute("UPDATE sync_requests SET status = :1, sync_timestamp = :2, updated_at = :3 WHERE fd_id = :4 AND status = :5", (sync_status, time_now(), time_now(), fd_id, sync_status_old))
							con.commit()
						continue
					else:
						sample_details_json = sample_details.json()
						if args.debug == True:
							print(str(" ".join(["\n",json.dumps(sample_details_json),"\n"])))
						if "errors" in sample_details_json:
							if "sample" in sample_details_json["errors"] and sample_details_json["errors"]["sample"] == ["could not be found"]:
								print_to_log(fd_id+" Sample "+sample_id+" details could not be found, aborting the current FD_ID. "+sample_details.text, "error", no_email=args.no_mail)
							else:
								print_to_log(fd_id+" Sample "+sample_id+" request error, aborting the current FD_ID. "+sample_details.text, "error", no_email=args.no_mail)
							sync_status = "Intervention"
							if cur != None and con != None:
								cur.execute("UPDATE sync_requests SET status = :1, sync_timestamp = :2, updated_at = :3 WHERE fd_id = :4 AND status = :5", (sync_status, time_now(), time_now(), fd_id, sync_status_old))
								con.commit()
							continue
						elif len(sample_details_json["files"]) >= 1 and len(sample_details_json["files"]["subpaths"]) >= 1 and len(sample_details_json["files"]["fullpaths"]) >= 1:
							existing_files = sample_details_json["files"]["subpaths"]					
							existing_files_full = sample_details_json["files"]["fullpaths"]
				elif experiment_id == "" and len(sids) > 1:
					experiment_details = get_experiment_details(sample_barcodes=sids, experiment_id=experiment_id)
					if experiment_details.status_code != 200:
						print_to_log(fd_id+" Experiment details request for sample IDs "+",".join(sids)+" failed or data absent from Data Catalog, aborting the current FD_ID. Intervention required.\n"+experiment_details.text, "error", no_email=args.no_mail)
						sync_status = "Intervention"
						if cur != None and con != None:
							cur.execute("UPDATE sync_requests SET status = :1, sync_timestamp = :2, updated_at = :3 WHERE fd_id = :4 AND status = :5", (sync_status, time_now(), time_now(), fd_id, sync_status_old))
							con.commit()
						continue
					else:
						experiment_details_json = experiment_details.json()
						if args.debug == True:
							print(str(" ".join(["\nExperiment details",json.dumps(experiment_details_json),"\n"])))
						if "errors" in experiment_details_json:
							if "experiment" in experiment_details_json["errors"] and experiment_details_json["errors"]["experiment"] == ["could not be found"]:
								print_to_log(fd_id+" Experiment details for sample IDs "+",".join(sids)+"could not be found, aborting the current FD_ID. ", "error", no_email=args.no_mail)
							else:
								print_to_log(fd_id+" Experiment details request error for sample IDs "+",".join(sids)+" aborting the current FD_ID.\n"+experiment_details.text, "error", no_email=args.no_mail)
							sync_status = "Intervention"
							if cur != None and con != None:
								cur.execute("UPDATE sync_requests SET status = :1, sync_timestamp = :2, updated_at = :3 WHERE fd_id = :4 AND status = :5", (sync_status, time_now(), time_now(), fd_id, sync_status_old))
								con.commit()
							continue
						if "id" in experiment_details_json:
							experiment_id = str(experiment_details_json["id"])
							if len(experiment_details_json["files"]) >= 1 and len(experiment_details_json["files"]["subpaths"]) >= 1 and len(experiment_details_json["files"]["fullpaths"]) >= 1:
								existing_files = experiment_details_json["files"]["subpaths"]
								existing_files_full = experiment_details_json["files"]["fullpaths"]
						else:
							print_to_log(fd_id+" Sample IDs "+",".join(sids)+" associated with more than one experiment in Data Catalog. Aborting the current FD_ID. \n"+experiment_details.text, "error", no_email=args.no_mail)
							sync_status = "Intervention"
							if cur != None and con != None:
								cur.execute("UPDATE sync_requests SET status = :1, sync_timestamp = :2, updated_at = :3 WHERE fd_id = :4 AND status = :5", (sync_status, time_now(), time_now(), fd_id, sync_status_old))
								con.commit()
							continue
				if args.debug == True:
					print(str(" ".join(["\nExisting files"]+existing_files+["\n"])))
				
				if args.dry_post == True or (experiment_id == "" and sample_id == ""):
					continue
				else:
					#Must have either sample_id or experiment_id, if both provided sample_id takes precedence in the post
					if sample_id != "":
						print_to_log(fd_id+" Posting files to sample "+sample_id+".")
					elif experiment_id != "":
						print_to_log(fd_id+" Posting files to experiment "+experiment_id+".")
					for f in child2_json_files["DATA"]:
						if f["DATA_TYPE"] == "successful_transfer":
							already_exists = False
							santizing_regex = re.compile(r"[^A-Za-z0-9._\/\-]")
							local_file_path = f["destination_path"]
							if os.path.exists(local_file_path) == False:
								print_to_log(fd_id+" File does not exist, skipping! "+local_file_path)
								continue
							local_file_path_decoded = urllib.parse.unquote(local_file_path)
							#local_file_path_decoded_and_sanitized = santizing_regex.sub("_",local_file_path_decoded)
							
							path_to_post = local_file_path.replace(tmp_path,"")
							path_to_post_decoded = urllib.parse.unquote(path_to_post)
							path_to_post_decoded_and_sanitized = santizing_regex.sub("_",path_to_post_decoded)
							path_to_post_decoded_and_sanitized_noleadslash = path_to_post_decoded_and_sanitized
							if path_to_post_decoded_and_sanitized[0] == "/":
								path_to_post_decoded_and_sanitized_noleadslash = path_to_post_decoded_and_sanitized[1:]
							
							for i,ef in enumerate(existing_files):
								if ef == path_to_post_decoded_and_sanitized_noleadslash:
									child2_json_files_replaced[local_file_path] = False
									ef_full = existing_files_full[i]
									minio_path = base_minio_path+"/"+ef_full
									print_to_log(fd_id+" File already present in the Data Catalog. Overwriting. "+ef)
									if os.path.exists(minio_path) == False:
										os.makedirs(os.path.dirname(minio_path))
									if args.copy == True:
										copyfile(local_file_path_decoded, minio_path)
									else:
										move(local_file_path_decoded, minio_path)
									if sample_id == "":
										print_to_log(fd_id+" File Moved (experiment_id: "+experiment_id+") FROM "+local_file_path+" TO "+minio_path)
									elif experiment_id == "":
										print_to_log(fd_id+" File Moved (sample_id: "+sample_id+") FROM "+local_file_path+" TO "+minio_path)
									child2_json_files_replaced[local_file_path] = True
									already_exists = True
									break
							if already_exists == False:
								child2_json_files_posted[local_file_path] = False
								child2_json_files_moved[local_file_path] = False
								
								r = None
								r = post_file(path_to_post_decoded_and_sanitized, sample_barcode=sample_id, experiment_id=experiment_id)
								if int(r.status_code) != 200:
									print_to_log(fd_id+" Error posting file to Data Catalog. "+path_to_post_decoded_and_sanitized+"\n"+r.text, "error", no_email=args.no_mail)
									break
								
								if args.no_move == True:
									continue
								r = r.json()
								if r["message"] == "Successfully uploaded datafile":
									minio_path = base_minio_path+"/"+r["path"]
									if sample_id == "":
										print_to_log(fd_id+" File Posted (experiment_id: "+experiment_id+") "+local_file_path)
									elif experiment_id == "":
										print_to_log(fd_id+" File Posted (sample_id: "+sample_id+") "+local_file_path)
									child2_json_files_posted[local_file_path] = True
									size_orig = os.stat(local_file_path_decoded).st_size
									if args.copy == True:
										copyfile(local_file_path_decoded, minio_path)
									else:
										move(local_file_path_decoded, minio_path)
									size_target = os.stat(minio_path).st_size
									if size_orig == size_target:
										if sample_id == "":
											print_to_log(fd_id+" File Moved (experiment_id: "+experiment_id+") FROM "+local_file_path+" TO "+minio_path)
										elif experiment_id == "":
											print_to_log(fd_id+" File Moved (sample_id: "+sample_id+") FROM "+local_file_path+" TO "+minio_path)
										child2_json_files_moved[local_file_path] = True
									else:
										print_to_log(fd_id+" Error moving file to Data Catalog. "+path_to_post, "error", no_email=args.no_mail)
								
					if sum(child2_json_files_posted.values()) == len(child2_json_files_posted) and sum(child2_json_files_moved.values()) == len(child2_json_files_moved):
						print_to_log(fd_id+" "+str(len(child2_json_files_posted))+" new file(s) and "+str(len(child2_json_files_replaced))+" replaced file(s) successfully posted and moved to the Data Catalog.")
						sync_status = "Posted and Moved"
						if force_globus_transfer_task_id == "" or args.force_db == True:
							cur.execute("UPDATE sync_requests SET status = :1, sync_timestamp = :2, updated_at = :3 WHERE fd_id = :4 AND status = :5", (sync_status, time_now(), time_now(), fd_id, sync_status_old))
							con.commit()
					else:
						print_to_log(fd_id+" Error posting or moving some of the files to Data Catalog. Will retry on the next cycle.", "error", no_email=args.no_mail)
			elif child1_json["status"] == "ACTIVE":
				print_to_log(fd_id+" Download still in progress")
			elif child1_json["status"] == "FAILED":
				print_to_log(fd_id+" Globus download task "+globus_transfer_task_id+" failed.", "error", no_email=args.no_mail)
			elif child1_json["status"] == "INACTIVE":
				print_to_log(fd_id+" Globus download task "+globus_transfer_task_id+" inactive.", "warn")
			else:
				print_to_log(fd_id+" Globus download task "+globus_transfer_task_id+" returned unknown status.", "error", no_email=args.no_mail)
	else:
		print_to_log("No FD_IDs currently with the \"Downloading\" status to process.")
		
parser = argparse.ArgumentParser(description="Sync data from JGI")
parser.add_argument("--config", default="config.json", help="")
parser.add_argument("--stage", default=False, action="store_true", help="")
parser.add_argument("--xfer", default=False, action="store_true", help="")
parser.add_argument("--post", default=False, action="store_true", help="")
parser.add_argument("--force_fd_id", default="-1", help="")
parser.add_argument("--force_jgi_stage_url", default="", help="")
parser.add_argument("--force_globus_transfer_task_id", default="", help="")
parser.add_argument("--force_sample_id", default="-1", help="")
parser.add_argument("--force_sample_ids", nargs="*", default=[], help="")
parser.add_argument("--force_experiment_id", default="-1", help="")
parser.add_argument("--force_db", default=False, action="store_true", help="")
parser.add_argument("--intervention", default=False, action="store_true", help="")
parser.add_argument("--debug", default=False, action="store_true", help="")
parser.add_argument("--no_mail", default=False, action="store_true", help="")
parser.add_argument("--no_move", default=False, action="store_true", help="")
parser.add_argument("--no_oracle", default=False, action="store_true", help="")
parser.add_argument("--dry_post", default=False, action="store_true", help="")
parser.add_argument("--copy", default=False, action="store_true", help="")
parser.add_argument("--print_url", default=False, action="store_true", help="")
args = parser.parse_args()
urllib3.disable_warnings()

#Check for a pidfile, to prevent simultaneous runs. 
#If it's there and the PID is currently running - exit
#If it's there and the PID is not running - store a new one and proceed
	
if os.path.exists("/tmp/jgi_transfer_tasks.pid") == True:
	local_pid = 0
	with open("/tmp/jgi_transfer_tasks.pid") as pf:
		for l in pf:
			local_pid = str(l.strip())
	pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
	if local_pid in pids:
		print_to_log("Another jgi_transfer_tasks.py task already running (PID "+str(local_pid)+").", "warn")
		exit()
	else:
		print_to_log("Another jgi_transfer_tasks.py task likely crashed (PID "+local_pid+"). Replacing lockfile with current PID "+str(os.getpid())+" and proceeding with the current run.", "error", no_email=args.no_mail)
		with open("/tmp/jgi_transfer_tasks.pid", "w") as pf:
			pf.write(str(os.getpid()))
else:
	with open("/tmp/jgi_transfer_tasks.pid", "w") as pf:
		pf.write(str(os.getpid()))
	print_to_log("Creating a new lockfile with current PID "+str(os.getpid())+" and proceeding with the current run.")

jgi_u = ""
jgi_pw = ""
globus_u = ""
globus_myproxy_u = ""
globus_myproxy_pw = ""
globus_bin = ""
tmp_path = ""
base_minio_path = ""
base_dc_url = ""

if os.path.exists(args.config) == False:
	print_to_log('The config file doesn\'t exist or no config file was specified using "--config".', "fatal", no_email=args.no_mail)
	exit_gracefully()
#Set up global variables from config
with open(args.config) as json_config_file:
	json_config_data = json.load(json_config_file)	
	globus_bin = json_config_data["globus_bin"]
	glbrc_destination_endpoint = json_config_data["glbrc_destination_endpoint"] 
	tmp_path = json_config_data["tmp_path"]
	base_minio_path = json_config_data["base_minio_path"]
	base_dc_url = json_config_data["base_dc_url"]

jgi_u = os.environ["JGI_USER"]
jgi_pw = os.environ["JGI_PW"]
globus_u = os.environ["GLOBUS_USER"]
globus_myproxy_u = os.environ["GLOBUS_MYPROXY_USER"]
globus_myproxy_pw = os.environ["GLOBUS_MYPROXY_PW"]
oracle_u = os.environ["data_transfer_scripts_USERNAME"]
oracle_pw = os.environ["data_transfer_scripts_PASSWORD"]
oracle_db_host_primary = os.environ["data_transfer_scripts_DB_HOST_PRIMARY"]
oracle_db_host_secondary = os.environ["data_transfer_scripts_DB_HOST_SECONDARY"]
oracle_db_service_name = os.environ["data_transfer_scripts_DB_SERVICE_NAME"]

con = None
cur = None
if args.no_oracle == False:
	dsnStr = "(DESCRIPTION=(FAILOVER=on)(CONNECT_TIMEOUT=5)(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST="+oracle_db_host_primary+")(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST="+oracle_db_host_secondary+")(PORT=1521))(LOAD_BALANCE=no))(CONNECT_DATA=(SERVICE_NAME="+oracle_db_service_name+")))"
	try:
		con = cx_Oracle.connect(oracle_u,oracle_pw,dsnStr)
	except cx_Oracle.DatabaseError:
		print_to_log("Error while connecting to the Oracle database!", "fatal", no_email=args.no_mail)
		exit_gracefully()
	cur = con.cursor()

#Get Data Catalog authentication token
dc_api_call_headers = ""
auth_token = auth_with_dc(args.debug)
if auth_token == "":
	print_to_log("Authentication with Data Catalog failed!","fatal", no_email=args.no_mail)
	exit_gracefully()
else:
	dc_api_call_headers = {"Authorization": "Token " + auth_token}


if args.stage == True or args.xfer == True:
	s = requests.session()
	p0= {"login":jgi_u, "password":jgi_pw}
	try:
		resp = s.post("https://signon.jgi.doe.gov/signon/create", timeout=10, params=p0, allow_redirects=True, stream=False)
	except requests.exceptions.Timeout as e:
		print_to_log("JGI Genome Portal sign-on connection timed out!", "fatal", no_email=args.no_mail)
		print_to_log(str(e))
	except requests.exceptions.SSLError:
		print_to_log("JGI Genome Portal SSL error!","fatal", no_email=args.no_mail)
		exit_gracefully()
	if args.stage == True:
		stage(args.force_fd_id)
	elif args.xfer == True:
		xfer(args.force_fd_id, args.force_jgi_stage_url)

if args.post == True:
	post(args.force_fd_id, args.force_sample_id, args.force_experiment_id, args.force_globus_transfer_task_id)

if cur != None and con != None:
	con.close()
if args.stage == False and args.xfer == False and args.post == False:
	print_to_log('No task specified. Use "--stage", "--xfer" or "--post".', "fatal", no_email=args.no_mail)

exit_gracefully()

