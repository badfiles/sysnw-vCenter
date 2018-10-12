# sysnw-vCenter
The program connects to the vCenter and prints out the Virtual Machine names with the list of snapshots, if the number of snapshots is more than one and any one of them exceeds 50% of the total machine disksize. The conditions are configurable.
The alternative action is putting the below fields into the table SNAPSHOTS of a given MySql database.

| sname | vmname | size | created | added |
|--|--|--|--|--|
| snapshot name | VM name | snapshot size | snapshot creation time | the time the record has been added |

The configuration file is more or less self-explanatory.

To run the program you need to make sure `main.py3` has the executable attribute.
The program takes no arguments.

# Dependencies

 - Python v. 3.4
 - [MySQLdb](http://mysql-python.sourceforge.net/MySQLdb.html)
 - [pyVmomi](https://github.com/vmware/pyvmomi)
