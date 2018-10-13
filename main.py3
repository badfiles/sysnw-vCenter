#!/usr/bin/python3

import atexit
#import argparse
import sys
import random
import string
import time
import ssl
import configparser
import MySQLdb

from pyVmomi import vim, vmodl
from datetime import datetime
#from pyVim.task import WaitForTask
from pyVim import connect
from pyVim.connect import Disconnect, SmartConnect, GetSi

def to_bool(value): # turns yes/no or 1/0 to a corresponding boolean value. Also takes care of true/false in a wrong case.
    valid = { 'true': True, '1': True, 'yes': True,
              'false': False, '0': False, 'no': False }
    if isinstance(value, bool):
        return value
    if not isinstance(value, str):
        raise ValueError('invalid literal for boolean. Not a string.')
    lower_value = value.lower()
    if lower_value in valid:
        return valid[lower_value]
    else:
        raise ValueError('invalid literal for boolean: "%s"' % value)

def GetHumanReadable(size,precision=2): # takes a number of bytes and returns a human readable value (ex. 1024 --> 1KB )
    suffixes=['B','KB','MB','GB','TB']
    suffixIndex = 0
    while size > 1024 and suffixIndex < 4:
        suffixIndex += 1
        size = size/1024.0
    return "%.*f%s"%(precision, size, suffixes[suffixIndex])

def read_config(section): # returns the whole config section as a key-value array
    config = configparser.RawConfigParser()
    config.read('cred.conf')
    return config.items(section)

def random_vm(): # generates a random vm as an array [name, disksize]
    vm=[]
    vm.append(''.join(random.choice(string.ascii_lowercase) for _ in range(5))) # 5 random letters
    vm.append(random.randint( 0, 5000000000) )
    return vm

def random_snapshot(vm,size): # generates a random snapshot as an array [vm name, name, size, timestamp]
    ss=[]
    ss.append(vm)
    ss.append(''.join(random.choice(string.ascii_lowercase) for _ in range(3))) # 3 random letters
    ss.append(random.randint( 0, int(size)))                                    # the size of a snapshot should not exceed the size of the vm
    ss.append('152' + ''.join(random.choice(string.digits) for _ in range(7)))  # random timestamp in unix format 152*******
    return ss

def mysql_out(config,update_existing,passed_vms,vms,snapshots): # performs mysql export with given config array and data to process
    records = 0
    for item in config:
        if item[0].lower() == 'db_address':  db_address =  item[1];      continue
        if item[0].lower() == 'db_port':     db_port =     int(item[1]); continue
        if item[0].lower() == 'db_user':     db_user =     item[1];      continue
        if item[0].lower() == 'db_password': db_password = item[1];      continue
        if item[0].lower() == 'db_base':     db_base =     item[1]

    if debug: print(db_address, db_port)

    try:
        db = None
        db = MySQLdb.connect(host=db_address,port=db_port,user=db_user,passwd=db_password,db=db_base)
        dbc = db.cursor()
        insert_data = []
        time_to_db = datetime.utcfromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S')

        for vm in passed_vms:
            for snapshot in snapshots:
                if vm[0] == snapshot[0]:
                    records += 1
                    line = (snapshot[1],
                            snapshot[0],
                            snapshot[2],
                            datetime.utcfromtimestamp(int(snapshot[3])).strftime('%Y-%m-%d %H:%M:%S')
                           )
                    insert_data.append(line)

        if debug: print(insert_data)

        action = 'sname=sname'
        if update_existing: action = "added='" + time_to_db + "'"

        dbc.executemany("INSERT INTO snapshots (sname, vmname, size, created, added) " + \
                        "VALUES (%s, %s, %s, %s, '" + time_to_db + "') ON DUPLICATE KEY UPDATE " + action,
                        insert_data
                       )
        db.commit()
        result = 'Inserted ' + str(records) +  ' record(s) into the database ' + db_base.upper()

    except MySQLdb.Error as error:
        result = error
    finally:
        if db != None: db.close()

    return result

def main():
    global debug
    GC = read_config('general') # get 'general' section of the config
    do_mysql = False
    vms=[] # this array will contain all vms
    sss=[] # this array will comtain all snapshots

    for item in GC: # parse glogal section
        if item[0].lower() == 'snapshots':       snapshots =       int(item[1]);     continue
        if item[0].lower() == 'ratio':           ratio =           int(item[1]);     continue
        if item[0].lower() == 'debug':           debug =           to_bool(item[1]); continue
        if item[0].lower() == 'random_data':     random_data =     to_bool(item[1]); continue
        if item[0].lower() == 'update_existing': update_existing = to_bool(item[1]); continue
        if item[0].lower() == 'output' and item[1].lower() == 'mysql':
            DBC = read_config('mySql')
            do_mysql = True

    if not random_data: # trying to connect to vCenter
        VC = read_config('vCenter') # get 'vCenter' section of the config

        for item in VC: # parse vCenter section
            if item[0].lower() == 'vc_address':  vC_address =  item[1];          continue
            if item[0].lower() == 'vc_port':     vC_port =     int(item[1]);     continue
            if item[0].lower() == 'vc_user':     vC_user =     item[1];          continue
            if item[0].lower() == 'vc_password': vC_password = item[1];          continue
            if item[0].lower() == 'vc_ssl':      vC_ssl =      to_bool(item[1])

        if debug: print(vC_address, vC_port, vC_ssl, ratio, snapshots, runtime)

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

            container = content.rootFolder
            viewType = [vim.VirtualMachine]
            recursive = True
            containerView = content.viewManager.CreateContainerView(
                            container, viewType, recursive)
            children = containerView.view
            for child in children: pass # parse output to arrays vms and sss if possible here

        except vmodl.MethodFault as error:
            print("Caught vmodl fault : " + error.msg)
            return -1
    else: # generate random data
        make_vms = 50
        make_max_snapshots = 5
        for _ in range( make_vms ):
            vms.append(random_vm())
        for item in vms:
            for _ in range(random.randint(0, make_max_snapshots)):
                sss.append(random_snapshot(item[0],item[1]))

    if debug:
        print(vms)
        print(sss)

    passed_vms=[] # this array will contain the vms which satisfy the conditions
    for vm in vms: # go through all vms
        snaps = 0
        line=[]
        exceed = False
        for snapshot in sss: # go through all snapshots
            if snapshot[0] == vm[0]: # count the total number of snapshots for each vm
                snaps += 1
                if 100 * int(snapshot[2]) > ratio * int(vm[1]): exceed = True # flag if any snap's size exceeds 'ratio'% of the vm size
        if exceed and snaps >= snapshots: # if there are more or exactly snapshots than 'snapshots' and the exceed flag is risen
            line.append(vm[0])
            line.append(vm[1])
            passed_vms.append(line) # put the vm info into the output array

    if debug: print(passed_vms)

    if do_mysql: # based on config perform mysql export
        if passed_vms == []: print( 'No records inserted into the database' )
        else: print( mysql_out(DBC,update_existing,passed_vms,vms,sss ))
    else: # or console output
        if passed_vms == []: print('\nNo machines have more than ' + str(snapshots) + \
        ' snapshots and snapshot/size ratio >' + str(ratio) +'%.')
        else:
            for vm in passed_vms:
                print('\nMachine ' + vm[0] + ' (total disk size ' + GetHumanReadable( int(vm[1])) + ') has snapshots')
                for snapshot in sss:
                    if vm[0] == snapshot[0]:
                        mark = ''
                        if 100 * int(snapshot[2]) > ratio * int(vm[1]): mark = '*'
                        print(snapshot[1], \
                        '\tCreated: ' + datetime.utcfromtimestamp(int(snapshot[3])).strftime('%d-%m-%Y %H:%M:%S') + ' UTC', \
                        '\tSize: ' + GetHumanReadable( int(snapshot[2])) + mark)
    return 0

# Start program
if __name__ == "__main__":
    main()
