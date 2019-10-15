"""
Name: Data Store to store and process json data in a file

This is a simple implementation to demonstrate the Data Store using a file with the functional and non-functional
requirements.

Author: Maheshkrishna A G
Email: maheshkrishnagopal@gmail.com
Date: 14-Sep-2019
"""
import os
import json
import sys
import datetime, time
from lockfile import LockFile
from exceptions import *


class DataStore:
    """
    DataStore is the main class that holds all the functionalities of the project such as CREATE, READ, DELETE the json
    objects in the data store file.
    This class takes the optional argument of the file path, without which the program takes the current working
    directory as the file path.
    """
    def __init__(self, default_path=os.getcwd()):
        """
        Constructor for the class DataStore, which will get automatically called when a instance is initiated for the
        class DataStore. During the call, this constructor creates an empty file in the user specified path / cwd.
        :param default_path: path to create the data store file.
        """

        # Create file in the path provided / in cwd.

        self.file_path = default_path + "\\datastore.txt"
        if not(os.path.exists(self.file_path)):
            with open(self.file_path, "w") as file:
                pass

    def check_existing(self, key):
        """
        check_existing is a method that receives a key as input and checks whether the key is available in the data
        store file.
        :param key: key to check if it exists
        :return: True, if the key exists. False, otherwise.
        """
        with open(self.file_path, 'r') as json_file:
            data = json_file.read()

        exist_flag = False
        for i in data.splitlines():
            for e in json.loads(i).keys():
                if e == key:
                    exist_flag = True

        if exist_flag:
            return True
        else:
            return False

    def check_ttl(self, key):
        """
        check_ttl method takes a key as input and checks if the key's Time To Live time has expired. It calculates the
        TTL period with the help of user input while creating the record. It subtracts the created time and the current
        time to validate it with the TTL parameter.
        :param key: key to check if the TTL is still alive.
        :return: Returns True, if the TTL is not expired and False, otherwise.
        """
        for line in open(self.file_path, 'r').read().splitlines():
            d = json.loads(line)
            if key in d:
                ttl_secs = d['ttl']
                if ttl_secs == 'infinite':
                    return True
                curr_time = datetime.datetime.now()
                created_time = datetime.datetime.strptime(d['created_time'], "%Y-%m-%d %H:%M:%S")
                diff_in_secs = (curr_time - created_time).total_seconds()
                if diff_in_secs > ttl_secs:
                    return False
                else:
                    return True
            else:
                continue

    def read(self, key):
        """
        read method the DataStore class, takes key as input and read through the data store file to give the
        corresponding value of it. It returns the value only if the key is existing in the data store file. Otherwise,
        this method raise an exception.
        :param key: Key to read the data store file
        :return: None
        """
        global output
        lock = LockFile(self.file_path)
        with lock:
            with open(self.file_path, 'r') as file:
                file_d = file.read()
            ttl = self.check_ttl(key)
            if ttl:
                rec_flag = False
                for line in file_d.splitlines():
                    for k, v in json.loads(line).items():
                        # print(i)
                        if k == key:
                            rec_flag = True
                            output = v
                if rec_flag:
                    print(output)
                else:
                    raise KeyNotExistError("KeyNotExist: 4821 - Queried Key not available in the file!")
            else:
                raise TTLOverDueError("TTLOverDue: 9834 - The queried key has expired!")

    def delete(self, user_key):
        """
        delete method receives the key of the data store and read through the data store file to delete it, if and only,
        if the key is existing in the file. Otherwise, delete method will raise an exception.
        :param key: key to delete from the data store file.
        :return: None
        """
        if self.check_existing(user_key):
            ttl = self.check_ttl(user_key)
            if ttl:
                lock = LockFile(self.file_path)
                with lock:
                    with open(self.file_path, "r") as input:
                        with open(os.getcwd() + "\\x.txt", "w") as output:
                            for line in input.read().splitlines():
                                rec = json.loads(line)
                                if user_key not in rec:
                                    print(rec)
                                    output.write(line+'\n')
                    os.remove(self.file_path)
                    os.rename(os.getcwd()+"\\x.txt", self.file_path)
                    print('Record deleted from the Data Store!')
            else:
                raise TTLOverDueError("TTLOverDue: 9834 - The queried key has expired!")
        else:
            raise KeyNotExistError("Key Not Exists: 0016 - The Key is not available")

    def check_file_size(self):
        """
        As per the non-functional requirements, the data store file size can never exceed 1 GB, hence, the
        check_file_size method checks the file size to make sure it is not exceeding that size. This method is being
        called whenever there is a create call happens.
        :return: True, if the file size not exceeded. False, otherwise.
        """
        file_size = os.path.getsize(self.file_path)
        if file_size < 1024**3:
            return True
        else:
            return False

    def create(self, key, value, ttl_seconds='infinite'):
        """
        create method creates an entry in the datastore file, by receiving the key, value and ttl_seconds from the user,
        if and only if the key is not already existed. The optional parameter ttl_seconds takes a default value
        'infinite', which means the record do not have a TTL value.
        :param ttl_seconds: time to live (optional)
        :param key: key to create a record in the data store file
        :param value: value for the key created
        :return: None
        """

        if type(key) is str:
            if len(key) <= 32:
                try:
                    value = json.loads(value)
                except Exception as e:
                    print(e)
                if ttl_seconds != 'infinite' and not(type(ttl_seconds) is int):
                    raise TTLTypeError("TTLTypeError: 6398 - The TTLSeconds parameter can only be 'int'")

                if sys.getsizeof(value)/1024 > 16:
                    raise SizeError("Size Error: 1652 - The value is exceeding the size limit!!")

                if self.check_existing(key):
                    raise KeyExistsError("KeyExistsError: 8732 - The key already exist in the file!")
                else:
                    if self.check_file_size():
                        lock = LockFile(self.file_path)
                        with lock:
                            with open(self.file_path, 'a') as f:
                                x = dict()
                                x[key] = value
                                if ttl_seconds != 'infinite':
                                    x['ttl'] = ttl_seconds
                                else:
                                    x['ttl'] = 'infinite'
                                curr_time = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
                                x['created_time'] = curr_time
                                json.dump(x, f)
                                f.write('\n')
                                print('Record Created in the Data Store file!')
                                return True
                    else:
                        raise FileSizeExceeded("FileSizeExceeded: 4444 - The Data Store file exceeded 1 GB!")

            else:
                raise LengthError("ValueError: 1982 - Length of the KEY exceeded the limit of 32 characters")
        else:
            print("Key can only be a str type!!!")
            return False
