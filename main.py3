#!/usr/bin/python3

import atexit
#import argparse
import sys
import os
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

def GetHumanReadable(size,precision=2): # takes a number of bytes and returns a human readable value (ex. 1024 --> 1KB )
    suffixes=['B','KB','MB','GB','TB']
    suffixIndex = 0
    while size > 1024 and suffixIndex < 4:
        suffixIndex += 1
        size = size/1024.0
    return "%.*f%s"%(precision, size, suffixes[suffixIndex])

class Configuration: # Class to hold the configuration and process a config file
    def __init__(self, file_name): # set default values
        self.snapshots: int = 2
        self.ratio: int = 50
        self.debug: bool = False
        self.random_data: bool = True
        self.update_existing: bool = False
        self.output: str = 'console'

        self.db_address: str = 'localhost'
        self.db_port: int = 3306
        self.db_timeout: int = 2
        self.db_user: str = 'user'
        self.db_password: str = 'password'
        self.db_base: str = 'base'

        self.vC_address: str = 'localhost'
        self.vC_port: int = 443
        self.vC_user: str = 'user'
        self.vC_password: str = 'password'
        self.vC_ssl: bool = True

        self.file_name = file_name
        self.warnings = []
        self.__excluded_options = []
        self.__options = set()
        self.do_mysql = False

    def props(self): # get all class attributes except system ones
        return [_ for _ in self.__dict__.keys() if _[:1] != '_']

    def __run_section(self,config,section): # reads the given section of a config file, returns inverse result to run in a while loop
        try:
            attr = ''
            if self.__options == set(): self.__options = set(self.props()).intersection(config.options(section))
            for attr in self.__options:
                if attr in self.__excluded_options: continue
                self.__excluded_options.append(attr)
                if isinstance(getattr(self,attr), bool): setattr(self, attr, config.getboolean(section,attr)); continue
                if isinstance(getattr(self,attr), int):  setattr(self, attr, config.getint(section,attr));     continue
                setattr(self, attr, config.get(section,attr))
            self.__excluded_options = []
            self.__options = set()
            return False

        except (configparser.Error, ValueError) as error: # treat all errors as warnings as there are default values
            if attr == '': self.warnings.append( str(error) ); return False
            else: self.warnings.append( "option '" + attr + "' has a wrong value: " + str(error) ); return True

    def populate(self): # process the file in 'file_name'
        try:
            config = configparser.ConfigParser()
            config_path =  os.path.join( os.path.abspath( os.path.dirname( __file__ ) ), self.file_name)
            if not os.path.isfile(config_path): raise OSError("file '" + config_path + "' does not exist")
            config.read(config_path)
            while self.__run_section(config,'general'): pass # to catch all erroneous and acceptable options in a section should be run multiple times
            if not self.random_data:
                while self.__run_section(config,'vCenter'): pass
            if self.output.lower() == 'mysql':
                while self.__run_section(config,'mySql'): pass
                self.do_mysql = True

        except (OSError, configparser.Error) as error: # treat all errors as warnings as there are default values
            self.warnings.append( str(error) )

    def warn(self): # prints warnings if any
        if not self.warnings == []:
            print('There have been configuration file errors:')
            for item in self.warnings:
                print (item)

def random_vm(): # generates a random vm as an array [name, disksize]
    vm = []
    vm.append(''.join(random.choice(string.ascii_lowercase) for _ in range(5))) # 5 random letters
    vm.append(random.randint( 0, 5000000000) )
    return vm

def random_snapshot(vm,size): # generates a random snapshot as an array [vm name, name, size, timestamp]
    ss = []
    ss.append(vm)
    ss.append(''.join(random.choice(string.ascii_lowercase) for _ in range(3))) # 3 random letters
    ss.append(random.randint( 0, int(size)))                                    # the size of a snapshot should not exceed the size of the vm
    ss.append('152' + ''.join(random.choice(string.digits) for _ in range(7)))  # random timestamp in unix format 152*******
    return ss

def mysql_out(config,passed_vms,vms,snapshots): # performs mysql export with given config and data to process
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
            for snapshot in snapshots:
                if vm[0] == snapshot[0]:
                    records += 1
                    line = (snapshot[1],
                            snapshot[0],
                            snapshot[2],
                            datetime.utcfromtimestamp(int(snapshot[3])).strftime('%Y-%m-%d %H:%M:%S')
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
        result = 'Inserted ' + str(records) + ' record(s) into the database ' + config.db_base.upper()

    except MySQLdb.Error as error:
        result = error
    finally:
        if db != None: db.close()

    return result

def main():
    config = Configuration('cred.conf') # create the configuration object and point to the config file
    config.populate() # transfer options from the config file to the object
    config.warn() # if there are warnings print them all out

    vms = []       # this array will contain all vms
    snapshots = [] # this array will contain all snapshots

    if not config.random_data: # trying to connect to vCenter

        if config.debug: print(config.vC_address, config.vC_port, config.vC_ssl, config.ratio, config.snapshots)

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
            for child in children: pass # parse output to arrays vms and snapshots if possible here

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
                snapshots.append(random_snapshot(item[0],item[1]))

    if config.debug:
        print(vms)
        print(snapshots)

    passed_vms=[] # this array will contain the vms which satisfy the conditions
    for vm in vms: # go through all vms
        snaps = 0
        line = []
        exceed = False
        for snapshot in snapshots: # go through all snapshots
            if snapshot[0] == vm[0]: # count the total number of snapshots for each vm
                snaps += 1
                if 100 * int(snapshot[2]) > config.ratio * int(vm[1]): exceed = True # flag if any snap's size exceeds 'ratio'% of the vm size
        if exceed and snaps >= config.snapshots: # if there are more or exactly snapshots than 'snapshots' and the exceed flag is risen
            line.append(vm[0])
            line.append(vm[1])
            passed_vms.append(line) # put the vm info into the output array

    if config.debug: print(passed_vms)

    if config.do_mysql: # based on config perform mysql export
        if passed_vms == []: print( 'No records inserted into the database' )
        else: print( mysql_out(config,passed_vms,vms,snapshots))
    else: # or console output
        if passed_vms == []: print('\nNo machines have more than ' + str(config.snapshots) + \
                                   ' snapshots and snapshot/size ratio >' + str(config.ratio) +'%.'
                                  )
        else:
            for vm in passed_vms:
                print('\nMachine ' + vm[0] + ' (total disk size ' + GetHumanReadable( int(vm[1])) + ') has snapshots')
                for snapshot in snapshots:
                    if vm[0] == snapshot[0]:
                        mark = ''
                        if 100 * int(snapshot[2]) > config.ratio * int(vm[1]): mark = '*'
                        print(snapshot[1], \
                              '\tCreated: ' + datetime.utcfromtimestamp(int(snapshot[3])).strftime('%d-%m-%Y %H:%M:%S') + ' UTC', \
                              '\tSize: ' + GetHumanReadable( int(snapshot[2])) + mark \
                             )

    return 0

# Start program
if __name__ == "__main__":
    main()
