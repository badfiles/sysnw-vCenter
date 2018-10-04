#!/usr/bin/env python

import atexit
#import argparse
import sys
import random
import string
import time
import ssl
import ConfigParser

from pyVmomi import vim, vmodl
from datetime import datetime
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

def GetHumanReadable(size,precision=2):
	suffixes=['B','KB','MB','GB','TB']
	suffixIndex = 0
	while size > 1024 and suffixIndex < 4:
		suffixIndex += 1
		size = size/1024.0
	return "%.*f%s"%(precision, size, suffixes[suffixIndex])

def read_config(section):
	config = ConfigParser.RawConfigParser()
	config.read('cred.conf')
	return config.items(section)

def random_vm(): # vm [name, disksize]
	vm=[]
	vm.append(''.join(random.choice(string.ascii_lowercase) for _ in range(5)))
	vm.append(random.randint( 0, 5000000000) )
	return vm
	
def random_snapshot(vm,size): # snapshot [vm name, name, size, timestamp]
	ss=[]
	ss.append(vm)
	ss.append(''.join(random.choice(string.ascii_lowercase) for _ in range(3)))
	while True:
		ssize = random.randint( 0, 5000000000)
		if ssize <= int(size): break;
	ss.append(ssize)
	ss.append('152' + ''.join(random.choice(string.digits) for _ in range(7)))
	return ss

def main():
	GC = read_config('general')
	VC = read_config('vCenter')
	runtime = int(time.time())
	do_mysql = False

	for item in GC:
		if item[0].lower() == 'snapshots':  snapshots = int(item[1])
		if item[0].lower() == 'ratio':      ratio =     int(item[1])
		if item[0].lower() == 'debug':      debug =     to_bool(item[1])
		if item[0].lower() == 'output' and item[1].lower() == 'mysql':
			DBC = read_config('mySql')
			do_mysql = True

	for item in VC:
		if item[0].lower() == 'vc_address':  vC_address =  item[1]
		if item[0].lower() == 'vc_port':     vC_port =     int(item[1])
		if item[0].lower() == 'vc_user':     vC_user =     item[1]
		if item[0].lower() == 'vc_password': vC_password = item[1]
		if item[0].lower() == 'vc_ssl':      vC_ssl =      to_bool(item[1])

	if debug: print vC_address, vC_port, vC_ssl, ratio, snapshots, runtime

	make_vms = 5
	make_max_snapshots = 10
	vms=[]
	for _ in range( make_vms ):
		vms.append(random_vm())

	sss=[]
	for item in vms:
		for _ in range(random.randint(0, make_max_snapshots)):
			sss.append(random_snapshot(item[0],item[1]))
	if debug:
		print vms
		print sss

	passed_vms=[]
	for vm in vms:
		machines = 0
		line=[]
		exceed = False
		for snapshot in sss:
			if snapshot[0] == vm[0]:
				machines += 1
				if 100 * int(snapshot[2]) > ratio * int(vm[1]): exceed = True
		if exceed == True and machines >= snapshots:
			line.append(vm[0])
			line.append(vm[1])
			line.append(machines)
			line.append(exceed)
			passed_vms.append(line)

#	if debug: print passed_vms

	if do_mysql:
		for item in DBC:
			if item[0].lower() == 'db_address':  db_address =  item[1]
			if item[0].lower() == 'db_port':     db_port =     int(item[1])
			if item[0].lower() == 'db_user':     db_user =     item[1]
			if item[0].lower() == 'db_password': db_password = item[1]

		if debug: print db_address, db_port
	else:
		if passed_vms == []: print '\n No machines have more than ' + str(snapshots) +' snapshots and snapshot/size ratio >' + str(ratio) +'%.'
		else:
			for vm in passed_vms:
				print '\n Machine ' + vm[0] + ' (disks size ' + GetHumanReadable( int(vm[1])) + ') has snapshots'
				for snapshot in sss:
					if vm[0] == snapshot[0]:
						mark = ''
						if 100 * int(snapshot[2]) > ratio * int(vm[1]): mark = '*'
						print snapshot[1], '\tCreated: ' + datetime.utcfromtimestamp(int(snapshot[3])).strftime('%m-%d-%Y %H:%M:%S'), '\tSize: ' + GetHumanReadable( int(snapshot[2])) + mark
	return 0

# Start program
if __name__ == "__main__":
	main()
