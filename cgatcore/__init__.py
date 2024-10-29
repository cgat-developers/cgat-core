# cgatcore/__init__.py
import importlib

pipeline = importlib.import_module('cgatcore.pipeline')
remote = importlib.import_module('cgatcore.remote')
