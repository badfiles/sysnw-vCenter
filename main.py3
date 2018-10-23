#!/usr/bin/python3

import atexit
#import argparse
import sys
import time
import ssl
import MySQLdb
import multiprocessing as mp

from pyVmomi import vim, vmodl
from datetime import datetime
#from pyVim.task import WaitForTask
from pyVim import connect
from pyVim.connect import Disconnect, SmartConnect, GetSi
from classes import *

def vC_connect(config):
    "performs connection to vCenter"
    try:
        if not config.vC_ssl:
            service_instance = connect.SmartConnectNoSSL(host=config.vC_address,
                                                         user=config.vC_user,
                                                         pwd=config.vC_password,
                                                         port=config.vC_port
                                                        )
        else:
            service_instance = connect.SmartConnect(host=config.vC_address,
                                                    user=config.vC_user,
                                                    pwd=config.vC_password,
                                                    port=config.vC_port
                                                   )

        atexit.register(connect.Disconnect, service_instance)

        content = service_instance.RetrieveContent()

        container = content.rootFolder
        viewType = [vim.VirtualMachine]
        recursive = True
        containerView = content.viewManager.CreateContainerView(container, viewType, recursive)
        children = containerView.view
        for child in children: pass # parse output to vms and snapshots if possible here
        return [], []

    except vmodl.MethodFault as error:
        print("Caught vmodl fault : " + error.msg)
        return [], []

def mysql_out(config, passed_vms):
    "performs mysql export with given config and data to process"

    records = 0

    if config.debug: print(config.db_address, config.db_port)

    try:
        db = None
        db = MySQLdb.connect(host=config.db_address,
                             port=config.db_port,
                             user=config.db_user,
                             passwd=config.db_password,
                             db=config.db_base,
                             connect_timeout=config.db_timeout
                            )
        dbc = db.cursor()
        insert_data = []
        time_to_db = datetime.utcfromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S')

        for vm in passed_vms:
            for snapshot in vm.snapshots:
                records += 1
                line = (snapshot.sn_name,
                        snapshot.vm_name,
                        snapshot.size,
                        snapshot.sql_timestamp
                       )
                insert_data.append(line)

        if config.debug: print(insert_data)

        action = 'sname=sname'
        if config.update_existing: action = "added='" + time_to_db + "'"

        dbc.executemany("INSERT INTO snapshots (sname, vmname, size, created, added) " + \
                        "VALUES (%s, %s, %s, %s, '" + time_to_db + "') ON DUPLICATE KEY UPDATE " + action,
                        insert_data
                       )
        db.commit()
        result = f'Inserted {records} record(s) into the database {config.db_base.upper()}'

    except MySQLdb.Error as error:
        result = error
    finally:
        if db != None: db.close()
    return result

def vms_split(config, vms, stripe, snapshots, target):
    "iterates over vms from index=stripe[0] to stripe[1]"
    for idx,vm in enumerate(vms):
        if idx < stripe[0]: continue #FIXME find a better solution to exclude the beginning of an array
        if idx > stripe[1]: break
        key = vm.vm_name
        for snap in snapshots:
            if snap.vm_name == key:
                vm.snapshots.append(snap) #puts all snapshots to corresponding Vms
        if vm.num_snapshots >= config.snapshots and 100*vm.max_snapshot > config.ratio*vm.size:
            target.append(vm) # sorts out the Vms that satisfy the conditions

def main():
    start = time.time()
    config = Configuration('cred.conf') # create the configuration object and point to the config file
    config.populate() # transfer options from the config file to the object
    config.warn() # if there are warnings print them all out

    if config.debug: print(config)

    snapshots = []

    if config.random_data: #generate random test data
        make_vms = 10
        make_max_snapshots = 5
        vms = [ Vm.random() for _ in range(make_vms) ]
        for vm in vms:
            for _ in range(random.randint(0, make_max_snapshots)):
                snapshots.append(Snapshot.random(vm))
        totals = 'Genegated'
    else: #aquire data from vCenter
        vms, snapshots = vC_connect(config)
        totals = 'Got'

    totals = f'{totals} total Vms: {Vm.count}; Snapshots: {Snapshot.count}'

    cpus = len(os.sched_getaffinity(0))
    if cpus > 1: #process vms on all available cpu cores (only makes sense if 'snapshots' is huge or unsorted, this code is a poc)
        stripe_size = len(vms)//cpus
        passed_vms = mp.Manager().list() # use process manager to store common output array
        procs = []
        for proc_num in range(cpus): # split index field of vms (0,len(vms)-1)) into 'cpus' stripes
            end = (proc_num + 1)*stripe_size - 1
            if proc_num == cpus-1: end += len(vms)%cpus # adding the remaining indexes to the last stripe
            if end >= 0: # if there are more cores than vms do not invoke empty stripes (ex. 4 cores and 3 vms will produce stripes (0,-1) (0,0) (1,1) (2,2), so only invoke the last three
                procs.append(mp.Process(target=vms_split, args=(config, vms, (proc_num*stripe_size, end), snapshots, passed_vms)))
                procs[-1].start() # start processing a stripe
        for proc in procs: proc.join() # wait for all stripes to finish
        procs.clear()
    else: # process vms on a single cpu
        passed_vms = []
        vms_split(config, vms, (0, len(vms)), snapshots, passed_vms)

    found_vms = len(passed_vms)

    if config.do_mysql: # based on config perform mysql export
        if found_vms == 0:
            print('No records inserted into the database')
        else:
            print(mysql_out(config,passed_vms))

    if config.debug or not config.do_mysql:
        sep = "="*80
        seb = "-"*80
        print(sep)
        print(totals)
        print(sep)
        print(f'Vms found: {found_vms}')
        print(seb)
        print('None') if found_vms == 0 else print( *passed_vms, sep='\n')
        if config.debug:
            stats = [ (vm.vm_name, vm.num_snapshots, int(100*vm.max_snapshot/vm.size)) for vm in vms ]
            print(sep)
            print(f'Vm stats (name, snapshots, max.snap/size ratio):')
            print(seb)
            print( *stats,  sep='\n')
        print(sep)
        print(f'Execution time, sec: {time.time() - start}')

    return 0

# Start program
if __name__ == "__main__":
    main()
