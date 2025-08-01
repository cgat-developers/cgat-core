"""File verification utilities for pipeline execution.

This module provides utilities to verify file existence with retry logic
to handle filesystem sync delays and race conditions.
"""

import os
import time
import functools
import logging
import ruffus
from typing import List, Union, Callable, Any

# Get logger
logger = logging.getLogger(__name__)


def verify_files_exist(file_paths: Union[str, List[str]], 
                       max_retries: int = 5,
                       initial_wait: float = 0.1) -> bool:
    """Verify that all specified files exist, with retry logic.
    
    Implements exponential backoff to handle filesystem sync delays.
    
    Args:
        file_paths: A single file path or list of file paths to verify
        max_retries: Maximum number of retries if files aren't found
        initial_wait: Initial wait time in seconds (doubles with each retry)
        
    Returns:
        True if all files exist after verification, False otherwise
    """
    if isinstance(file_paths, str):
        file_paths = [file_paths]
    
    # Quick check if all files exist immediately
    missing_files = [f for f in file_paths if not os.path.exists(f)]
    if not missing_files:
        return True
        
    # Retry with exponential backoff for filesystem sync delays
    wait_time = initial_wait
    for retry in range(max_retries):
        logger.debug(f"Some output files not found yet, waiting {wait_time}s: {missing_files}")
        time.sleep(wait_time)
        
        # Check again after waiting
        missing_files = [f for f in missing_files if not os.path.exists(f)]
        
        # If all files now exist, we're done
        if not missing_files:
            logger.debug(f"All output files now exist after retry {retry+1}")
            return True
            
        # Double wait time for next retry (exponential backoff)
        wait_time *= 2
        
    # If we get here, some files are still missing after all retries
    if missing_files:
        logger.warning(f"Some files still missing after {max_retries} retries: {missing_files}")
    return not bool(missing_files)


def with_file_verification(orig_func):
    """Decorator to add file verification with retry to Ruffus task functions.
    
    This decorator wraps a Ruffus task function to verify output files exist
    before marking the task as complete.
    """
    @functools.wraps(orig_func)
    def wrapper(*args, **kwargs):
        # Call the original function
        result = orig_func(*args, **kwargs)
        
        # For Ruffus task functions, typically the first arg is the input files
        # and the second arg is the output files
        if len(args) >= 2 and args[1]:
            output_files = args[1]
            if isinstance(output_files, str):
                output_files = [output_files]
            elif isinstance(output_files, (list, tuple)):
                # Flatten any nested lists
                flat_outputs = []
                for item in output_files:
                    if isinstance(item, (list, tuple)):
                        flat_outputs.extend(item)
                    else:
                        flat_outputs.append(item)
                output_files = flat_outputs
            
            # Only verify if there are output files
            if output_files:
                logger.debug(f"Verifying output files exist: {output_files}")
                verified = verify_files_exist(output_files)
                if not verified:
                    logger.warning(f"Some output files could not be verified for task: {orig_func.__name__}")
        
        # Return the original result
        return result
    
    return wrapper


def verify_task_outputs(max_retries=5, initial_wait=0.1):
    """Decorator that adds file verification with parameters to Ruffus task functions.
    
    Args:
        max_retries: Maximum number of retries if files aren't found
        initial_wait: Initial wait time in seconds (doubles with each retry)
    """
    def decorator(orig_func):
        @functools.wraps(orig_func)
        def wrapper(*args, **kwargs):
            # Call the original function
            result = orig_func(*args, **kwargs)
            
            # For Ruffus task functions, typically the first arg is the input files
            # and the second arg is the output files
            if len(args) >= 2 and args[1]:
                output_files = args[1]
                if isinstance(output_files, str):
                    output_files = [output_files]
                elif isinstance(output_files, (list, tuple)):
                    # Flatten any nested lists
                    flat_outputs = []
                    for item in output_files:
                        if isinstance(item, (list, tuple)):
                            flat_outputs.extend(item)
                        else:
                            flat_outputs.append(item)
                    output_files = flat_outputs
                
                # Only verify if there are output files
                if output_files:
                    logger.debug(f"Verifying output files exist: {output_files}")
                    verified = verify_files_exist(output_files, max_retries, initial_wait)
                    if not verified:
                        logger.warning(f"Some output files could not be verified for task: {orig_func.__name__}")
            
            # Return the original result
            return result
        
        return wrapper
    
    return decorator


# Monkey patch for Ruffus to ensure file existence checks with retry
def patch_ruffus_is_file_exists_function():
    """Monkey patch Ruffus's is_file_exists function to add retry logic.
    
    This replaces the Ruffus internal function that checks if files exist
    with our retry-enabled version.
    """
    # Import modules within function scope to avoid UnboundLocalError
    import ruffus
    import ruffus.ruffus_exceptions
    
    if not hasattr(ruffus.ruffus_exceptions, 'original_is_file_exists'):
        try:
            # Store the original function before patching
            ruffus.ruffus_exceptions.original_is_file_exists = ruffus.ruffus_exceptions.is_file_exists
            
            # Replace with our version that includes retry logic
            def patched_is_file_exists(filename):
                """Patched version of Ruffus's is_file_exists with retry logic."""
                return verify_files_exist(filename)
            
            ruffus.ruffus_exceptions.is_file_exists = patched_is_file_exists
            logger.info("Successfully patched Ruffus is_file_exists function with retry logic")
            return True
        except (ImportError, AttributeError) as e:
            logger.warning(f"Failed to patch Ruffus is_file_exists: {e}")
            return False
    else:
        logger.debug("Ruffus is_file_exists already patched")
        return True
