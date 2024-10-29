# cgatcore/remote/abstract.py

from abc import ABC, abstractmethod


class AbstractRemoteObject(ABC):
    '''This is an abstract class that all RemoteObjects will
       inherit from. This is an abstract class to rigidly define
       the abstract methods of this RemoteObject class'''

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @abstractmethod
    def exists(self):
        pass

    @abstractmethod
    def download(self):
        pass

    @abstractmethod
    def upload(self):
        pass

    @abstractmethod
    def delete_file(self):
        pass