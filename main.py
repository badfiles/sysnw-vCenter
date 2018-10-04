#!/usr/bin/env python

import atexit
#import argparse
import sys
import time
import ssl
import ConfigParser

from pyVmomi import vim, vmodl
#from pyVim.task import WaitForTask
from pyVim import connect
from pyVim.connect import Disconnect, SmartConnect, GetSi

def to_bool(value):
	valid = { 'true': True, 't': True, '1': True, 'yes': True, 
	          'false': False, 'f': False, '0': False, 'no': False }
	if isinstance(value, bool):
		return value
	if not isinstance(value, basestring):
		raise ValueError('invalid literal for boolean. Not a string.')
	lower_value = value.lower()
	if lower_value in valid:
		return valid[lower_value]
	else:
		raise ValueError('invalid literal for boolean: "%s"' % value)

def read_config(section):
	config = ConfigParser.RawConfigParser()
	config.read('cred.conf')
	return config.items(section)

def main():
	GC = read_config('general')
	VC = read_config('vCenter')

	for item in GC:
		if item[0].lower() == 'output' and item[1].lower() == 'mysql':
			DBC = read_config('mySql')
			do_mysql = True

	for item in VC:
		if item[0].lower() == 'vc_address':		vC_address =	item[1]
		if item[0].lower() == 'vc_port':		vC_port =		int(item[1])
		if item[0].lower() == 'vc_user':		vC_user =		item[1]
		if item[0].lower() == 'vc_password':	vC_password =	item[1]
		if item[0].lower() == 'vc_ssl':			vC_ssl =		to_bool(item[1])

	print vC_address, vC_port, vC_ssl

	try:
		if not vC_ssl:
			service_instance = connect.SmartConnectNoSSL(host=vC_address,
			                                             user=vC_user,
			                                             pwd=vC_password,
			                                             port=vC_port)
		else:
			service_instance = connect.SmartConnect(host=vC_address,
			                                            user=vC_user,
			                                            pwd=vC_password,
			                                            port=vC_port)

		atexit.register(connect.Disconnect, service_instance)

		content = service_instance.RetrieveContent()

		container = content.rootFolder  # starting point to look into
		viewType = [vim.VirtualMachine]  # object types to look for
		recursive = True  # whether we should look into it recursively
		containerView = content.viewManager.CreateContainerView(
		                container, viewType, recursive)
		children = containerView.view
		for child in children: a =1
#			print_(child)

	except vmodl.MethodFault as error:
		print("Caught vmodl fault : " + error.msg)
		return -1

	return 0

# Start program
if __name__ == "__main__":
	main()
