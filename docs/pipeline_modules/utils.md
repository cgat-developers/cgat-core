# Utilities for cgatcore Pipelines - Documentation

This document provides an overview of the utility functions and classes defined in `utils.py` for use in cgatcore pipelines. The utilities include functions for context inspection, testing mode checks, and decorators for managing documentation strings. These functions are particularly helpful for debugging and providing context information during the execution of a cgatcore pipeline.

## Table of Contents
- [EmptyRunner Class](#emptyrunner-class)
- [is_test Function](#is_test-function)
- [get_caller_locals Function](#get_caller_locals-function)
- [get_caller Function](#get_caller-function)
- [get_calling_function Function](#get_calling_function-function)
- [add_doc Decorator](#add_doc-decorator)

### EmptyRunner Class

```python
class EmptyRunner:
    def __init__(self, name):
        self.__name__ = name

    def __call__(self, *args, **kwargs):
        pass
```

The `EmptyRunner` class is a simple utility that creates an object with a callable interface that does nothing when called. It is primarily useful as a placeholder in situations where a no-operation handler is required.

- **Attributes**:
  - `name`: A name assigned to the instance for identification purposes.

- **Methods**:
  - `__call__(self, *args, **kwargs)`: This method is a no-operation handler.

### is_test Function

```python
def is_test():
    return "--is-test" in sys.argv
```

The `is_test()` function checks whether the pipeline is being run in testing mode.

- **Returns**:
  - `bool`: Returns `True` if `--is-test` is passed as a command-line argument; otherwise, `False`.

This function is useful for conditionally enabling or disabling testing-specific behaviour.

### get_caller_locals Function

```python
def get_caller_locals(decorators=0):
    f = sys._getframe(2 + decorators)
    args = inspect.getargvalues(f)
    return args.locals
```

The `get_caller_locals()` function returns the local variables of the calling function. This is useful for debugging or inspecting the state of the caller.

- **Parameters**:
  - `decorators (int)`: The number of decorator contexts to go up to find the caller of interest. Default is `0`.

- **Returns**:
  - `dict`: A dictionary of local variables defined in the caller's context.

### get_caller Function

```python
def get_caller(decorators=0):
    frm = inspect.stack()
    return inspect.getmodule(frm[2 + decorators].frame)
```

The `get_caller()` function returns the calling class/module. This helps identify the caller and can be helpful for logging and debugging.

- **Parameters**:
  - `decorators (int)`: The number of decorator contexts to go up to find the caller of interest. Default is `0`.

- **Returns**:
  - `module`: The calling module or `None` if not found.

### get_calling_function Function

```python
def get_calling_function(decorators=0):
    frm = inspect.stack()
    return frm[2 + decorators].function
```

The `get_calling_function()` function returns the name of the calling function. This is useful for introspection and debugging to know which function invoked the current function.

- **Parameters**:
  - `decorators (int)`: The number of decorator contexts to go up to find the caller of interest. Default is `0`.

- **Returns**:
  - `str`: The name of the calling function, or `None` if not found.

### add_doc Decorator

```python
def add_doc(value, replace=False):
    def _doc(func):
        @wraps(func)
        def wrapped_func(*args, **kwargs):
            return func(*args, **kwargs)

        if func.__doc__:
            lines = value.__doc__.split("\n")
            for x, line in enumerate(lines):
                if line.strip() == "":
                    break
            if not replace:
                lines.insert(x + 1, " " * 4 + func.__doc__)
                wrapped_func.__doc__ = "\n".join(lines)
            else:
                wrapped_func.__doc__ = value.__doc__
        else:
            wrapped_func.__doc__ = value.__doc__
        
        return wrapped_func

    return _doc
```

The `add_doc()` function is a decorator that adds or replaces the docstring of the decorated function.

- **Parameters**:
  - `value`: The function or string whose documentation will be added.
  - `replace (bool)`: If `True`, the existing documentation is replaced. Otherwise, the new documentation is appended after the existing documentation. Default is `False`.

- **Returns**:
  - A decorated function with the modified or updated docstring.

This utility is helpful for ensuring that custom decorators or utility functions carry informative and up-to-date documentation, which can help when generating automated docs or maintaining code.

### General Notes

- The functions use Python's `inspect` and `sys` libraries for introspection and manipulation of stack frames, which can be useful for debugging complex pipelines.
- The `logging` module is used for error handling to ensure that potential issues (e.g., accessing out-of-range stack frames) are logged rather than silently ignored.

### Example Usage

```python
@add_doc(is_test)
def example_function():
    """This is an example function."""
    print("Example function running.")

if __name__ == "__main__":
    if is_test():
        print("Running in testing mode.")
    else:
        example_function()
```

In this example, `example_function()` is decorated with the `add_doc()` decorator, using the documentation from `is_test()`. This effectively appends the `is_test()` docstring to `example_function()`.

### Conclusion

These utilities provide helpful functionality for cgatcore pipelines by allowing developers to inspect caller contexts, easily handle testing conditions, and dynamically update function documentation. The use of the `inspect` library allows access to stack frames, making these utilities especially useful for debugging and dynamic analysis during pipeline execution.

