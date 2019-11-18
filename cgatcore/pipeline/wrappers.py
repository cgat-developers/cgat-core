class EmptyRunner(object):

    def __init__(self, name):
        self.__name__ = name

    def __call__(self, *args, **kwargs):
        pass


class PassThroughRunner(object):
    def __init__(self, name, f):
        self.__name__ = name
        self.f = f

    def __call__(self, *args, **kwargs):
        self.f(*args, **kwargs)
