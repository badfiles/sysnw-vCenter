import configparser
import os
import random
import string
from datetime import datetime

def GetHumanReadable(size,precision=2):
    "takes a number of bytes and returns a human readable value (ex. 1024 --> 1KB )"
    suffixes=['B','KB','MB','GB','TB']
    suffixIndex = 0
    while size > 1024 and suffixIndex < 4:
        suffixIndex += 1
        size = size/1024.0
    return "%.*f%s"%(precision, size, suffixes[suffixIndex])

class Vm:
    count = 0
    def __init__(self, vm_name='', size=0):
        self.vm_name = vm_name
        self.size = size
        self.snapshots = []
        Vm.count += 1

    def __repr__(self):
        return f"Vm('{self.vm_name}',{self.size})"

    def __str__(self):
        des = f"Virtual machine name '{self.vm_name}' has total disksize {self.hr_size}"
        if self.has_snapshot:
            for sn in self.snapshots:
                des += "\n\t" + str(sn)
        return des

    def __gt__(self, other):
        return self.size > other.size

    @property
    def hr_size(self):
        return GetHumanReadable(self.size)

    @property
    def has_snapshot(self):
        return True if len(self.snapshots)>0 else False

    @property
    def num_snapshots(self):
        return len(self.snapshots)

    @property
    def max_snapshot(self):
        return max(self.snapshots).size if self.has_snapshot else 0

    @property
    def hr_timestamp(self):
        return f"{datetime.utcfromtimestamp(self.timestamp).strftime('%d-%m-%Y %H:%M:%S')} UTC"

    @property
    def sql_timestamp(self):
        return datetime.utcfromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S')

    @classmethod
    def random(cls):
        "generates a random vm as Vm"
        vm = cls()
        vm.vm_name = ''.join(random.choice(string.ascii_lowercase) for _ in range(5)) # 5 random letters
        vm.size = random.randint( 0, 5_000_000_000)
        return vm

class Snapshot(Vm):
    count = 0
    def __init__(self, sn_name='', size=0, vm_name='', timestamp=''):
        self.vm_name = vm_name
        self.size = size
        self.sn_name = sn_name
        self.timestamp = timestamp
        Snapshot.count += 1

    def __repr__(self):
        return f"Snapshot('{self.sn_name}',{self.size},'{self.vm_name}',{self.timestamp})"

    def __str__(self):
        return f"Snapshot '{self.sn_name}' for machine '{self.vm_name}' has size {self.hr_size} and created {self.hr_timestamp}"

    @classmethod
    def random(cls, vm):
        "generates a vm's random snapshot as Snapshot"
        sn = cls()
        sn.sn_name = ''.join(random.choice(string.ascii_lowercase) for _ in range(3)) # 3 random letters
        sn.vm_name = vm.vm_name
        sn.size = random.randint( 0, vm.size) # the size of a snapshot should not exceed the size of the vm
        sn.timestamp = int('152' + ''.join(random.choice(string.digits) for _ in range(7)))  # random timestamp in unix format 152*******
        return sn

class Configuration:
    "Class to hold the configuration and process a config file"
    def __init__(self, file_name):
        "set default values"
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
        self.__excluded_options = ['file_name','do_mysql','warnings']
        self.do_mysql = False

    def __str__(self):
        result = 80*'=' + f'\nConfiguration from {self.full_path}\n' + 80*'='
        for attr in self.props():
            if attr in self.__excluded_options: continue
            result += f'\n{attr} = {getattr(self,attr)}'
        if len(self.warnings) > 0:
            result += '\n'+ 80*'='+'\nThere have been configuration file errors:'
            for item in self.warnings:
               result += f'\n{item}'
        return result

    @property
    def full_path(self):
        return os.path.join( os.path.abspath( os.path.dirname( __file__ ) ), self.file_name)

    def props(self):
        "get all class attributes except system ones"
        return [_ for _ in self.__dict__.keys() if _[:1] != '_']

    def __run_section(self,config,section):
        "reads the given section of a config file"
        all_done = False
        attr = None
        options = set(self.props()).intersection(config.options(section))
        excluded_options = set(self.__excluded_options)
        while not all_done: # to catch all erroneous and acceptable options in a section should be run multiple times
            try:
                for attr in options:
                    if attr in excluded_options: continue
                    excluded_options.add(attr)
                    if isinstance(getattr(self,attr), bool): setattr(self, attr, config.getboolean(section,attr)); continue
                    if isinstance(getattr(self,attr), int):  setattr(self, attr, config.getint(section,attr));     continue
                    setattr(self, attr, config.get(section,attr))
                all_done = True

            except (configparser.Error, ValueError) as error: # treat all errors as warnings as there are default values
                if attr == None: self.warnings.append( str(error) ); all_done = True
                else: self.warnings.append( f"option '{attr}' has a wrong value: '{error}'")

    def populate(self):
        "process the file in 'file_name"
        try:
            config = configparser.ConfigParser()
            if not os.path.isfile(self.full_path): raise OSError(f"file '{self.full_path}' does not exist")
            config.read(self.full_path)
            self.__run_section(config,'general')
            if not self.random_data: self.__run_section(config,'vCenter')
            if self.output.lower() == 'mysql':
                self.__run_section(config,'mySql')
                self.do_mysql = True

        except (OSError, configparser.Error) as error: # treat all errors as warnings as there are default values
            self.warnings.append( str(error) )

    def warn(self):
        "prints warnings if any"
        if len(self.warnings) > 0:
            print('There have been configuration file errors:', *self.warnings, sep='\n')
