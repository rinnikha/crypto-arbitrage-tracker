"""
Enhanced error handling, custom exceptions, and logging utilities.
"""
import logging
import sys
import traceback
from typing import Dict, Any, Optional, List, Type, TypeVar, Callable
from functools import wraps
import time
import json
from datetime import datetime

# Type variables
T = TypeVar('T')


class AppError(Exception):
    """Base exception for application-specific errors."""

    def __init__(self, message: str, code: Optional[str] = None,
                 details: Optional[Dict[str, Any]] = None):
        """
        Initialize the exception.

        Args:
            message: Error message
            code: Error code
            details: Additional error details
        """
        self.message = message
        self.code = code
        self.details = details or {}
        super().__init__(self.message)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the error to a dictionary.

        Returns:
            Error dictionary
        """
        result = {
            "message": self.message,
            "type": self.__class__.__name__
        }

        if self.code:
            result["code"] = self.code

        if self.details:
            result["details"] = self.details

        return result

    def __str__(self) -> str:
        """String representation of the error."""
        parts = [self.message]

        if self.code:
            parts.append(f"Code: {self.code}")

        if self.details:
            parts.append(f"Details: {json.dumps(self.details, default=str)}")

        return " | ".join(parts)


class ConfigError(AppError):
    """Exception for configuration errors."""
    pass


class DatabaseError(AppError):
    """Exception for database errors."""
    pass


class ApiError(AppError):
    """Exception for API errors."""
    pass


class ValidationError(AppError):
    """Exception for validation errors."""
    pass


class NotFoundError(AppError):
    """Exception for not found errors."""
    pass


def setup_logging(level: str = "INFO",
                  log_file: Optional[str] = None,
                  log_format: Optional[str] = None) -> None:
    """
    Set up logging configuration.

    Args:
        level: Logging level
        log_file: Log file path
        log_format: Log format string
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Default format includes thread name which is helpful for debugging
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s"

    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Add file handler if log file is specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(file_handler)

    # Set level of common noisy loggers
    for logger_name in ["urllib3", "requests", "sqlalchemy.engine"]:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def log_exception(exc: Exception, logger: Optional[logging.Logger] = None) -> None:
    """
    Log an exception with detailed information.

    Args:
        exc: Exception to log
        logger: Logger to use (default: root logger)
    """
    logger = logger or logging.getLogger()

    # Get exception details
    exc_type = type(exc).__name__
    exc_msg = str(exc)
    exc_tb = traceback.format_exc()

    # Log with error level
    logger.error(
        f"Exception: {exc_type}: {exc_msg}\n"
        f"Traceback:\n{exc_tb}"
    )


def log_execution_time(func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator to log function execution time.

    Args:
        func: Function to decorate

    Returns:
        Decorated function
    """
    logger = logging.getLogger(func.__module__)

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()

        try:
            # Execute the function
            result = func(*args, **kwargs)

            # Log execution time
            elapsed = time.time() - start_time
            logger.debug(f"{func.__name__} executed in {elapsed:.4f} seconds")

            return result
        except Exception as e:
            # Log execution time and re-raise exception
            elapsed = time.time() - start_time
            logger.debug(f"{func.__name__} failed after {elapsed:.4f} seconds: {e}")
            raise

    return wrapper


def error_handler(error_map: Dict[Type[Exception], Type[AppError]] = None,
                  default_error: Type[AppError] = AppError,
                  logger: Optional[logging.Logger] = None) -> Callable:
    """
    Decorator for consistent error handling.

    Args:
        error_map: Mapping of exception types to AppError types
        default_error: Default AppError type to use
        logger: Logger to use (default: module logger)

    Returns:
        Decorator function
    """
    error_map = error_map or {}

    def decorator(func):
        func_logger = logger or logging.getLogger(func.__module__)

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                # Execute the function
                return func(*args, **kwargs)
            except AppError as e:
                # Already an AppError, just log and re-raise
                func_logger.error(f"AppError in {func.__name__}: {e}")
                raise
            except Exception as e:
                # Convert to appropriate AppError type
                error_type = error_map.get(type(e), default_error)

                # Log the original exception
                log_exception(e, func_logger)

                # Create and raise AppError
                app_error = error_type(
                    message=str(e),
                    details={
                        "function": func.__name__,
                        "original_type": type(e).__name__,
                        "timestamp": datetime.now().isoformat()
                    }
                )

                raise app_error

        return wrapper

    return decorator