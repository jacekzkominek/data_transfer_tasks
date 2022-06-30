#!/usr/bin/env python3

import datetime, smtplib
from datetime import timedelta

def time_now():
	return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	
def date_now(add_days=0):
	return (datetime.datetime.now()+timedelta(days=add_days)).strftime("%Y-%m-%d")
	
def print_to_log(message="", log_level="info", no_email=False):
	log_level_label = ""
	if log_level == "info":
		log_level_label = "[INFO]"
	elif log_level == "warn":
		log_level_label = "[WARN]"
	elif log_level == "error":
		log_level_label = "[ERROR]"
		message = message+" Notifying the helpdesk.\n"
		if no_email == False:
			send_email_to_helpdesk(message, log_level)
	elif log_level == "fatal":
		log_level_label = "[FATAL]"
		message = message+" Notifying the helpdesk.\n"
		if no_email == False:
			send_email_to_helpdesk(message, log_level)
	else:
		print(str("Invalid log_level specified: "+log_level+". Needs to be \'fatal\', \'error\', \'warn\' or \'info\'. Exiting."))
		print(message)
		exit()
	print(str(" ".join([time_now(),log_level_label,message])))
	
def send_email_to_helpdesk (text, error_level="error"):
	source = "XXX"
	target = ["XXX"] # must be a list
	subject = "XXX " + error_level
	message = text
	message = """From: %s\nTo: %s\nSubject: %s\n%s""" % (source, ", ".join(target), subject, message)
	server = smtplib.SMTP("XXX")
	server.sendmail(source, target, message)
	server.quit()
