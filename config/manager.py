"""
Centralized configuration manager with validation and environment fallbacks.
"""
import os
import logging
from typing import Dict, Any, Optional, List, Set, Union, TypeVar, Callable, Type
import json
from pathlib import Path
from dotenv import load_dotenv
import re
from datetime import datetime

logger = logging.getLogger(__name__)

# Type variables
T = TypeVar('T')


class ConfigError(Exception):
    """Exception for configuration errors."""
    pass


class ConfigManager:
    """
    Centralized configuration manager.

    This class provides a unified interface for accessing configuration settings
    from various sources (environment variables, config files, etc.) with proper
    validation and fallbacks.
    """

    def __init__(self, env_file: Optional[str] = None, config_dir: Optional[str] = None,
                 environment: Optional[str] = None):
        """
        Initialize the configuration manager.

        Args:
            env_file: Path to .env file (default: .env)
            config_dir: Path to configuration directory (default: ./config)
            environment: Environment name (default: from ENV or 'development')
        """
        # Load environment variables
        self._load_env_file(env_file)

        # Set config directory
        self.config_dir = Path(config_dir or './config')

        # Set environment
        self.environment = environment or os.getenv("ENVIRONMENT", "development")
        logger.info(f"Running in {self.environment} environment")

        # Load config files
        self._config_cache = {}

        # Initialize settings dictionary
        self.settings = {}
        self._load_settings()

    def _load_env_file(self, env_file: Optional[str] = None) -> None:
        """
        Load environment variables from .env file.

        Args:
            env_file: Path to .env file (default: .env)
        """
        if env_file and os.path.exists(env_file):
            load_dotenv(env_file)
            logger.info(f"Loaded environment variables from {env_file}")
        else:
            # Try default locations
            for path in ['.env', '.env.local', f'.env.{os.getenv("ENVIRONMENT", "development")}']:
                if os.path.exists(path):
                    load_dotenv(path)
                    logger.info(f"Loaded environment variables from {path}")
                    break

    def _load_settings(self) -> None:
        """Load settings from various sources."""
        # Load settings from config files
        self._load_config_files()

        # Apply environment-specific overrides
        self._apply_env_overrides()

    def _load_config_files(self) -> None:
        """Load settings from configuration files."""
        # Load base settings
        self.settings.update(self._load_config_file('settings.py'))

        # Load environment-specific settings
        env_settings = self._load_config_file(f'settings_{self.environment}.py')
        if env_settings:
            self.settings.update(env_settings)

        # Load exchange settings
        exchange_settings = self._load_config_file('exchanges.py')
        if exchange_settings:
            self.settings.update(exchange_settings)

    def _load_config_file(self, filename: str) -> Dict[str, Any]:
        """
        Load configuration from a file.

        Args:
            filename: Configuration file name

        Returns:
            Configuration dictionary
        """
        if filename in self._config_cache:
            return self._config_cache[filename]

        config = {}

        file_path = self.config_dir / filename
        if not file_path.exists():
            logger.warning(f"Configuration file not found: {file_path}")
            return config

        try:
            # Python module
            if filename.endswith('.py'):
                # Use a safer approach - read the file and execute it in a controlled context
                # Create a namespace with common builtins and __file__ variable
                namespace = {
                    '__file__': str(file_path),
                    'os': os,
                    'Path': Path,
                    'datetime': datetime
                }

                with open(file_path, 'r') as f:
                    content = f.read()

                # Execute in the prepared namespace
                try:
                    exec(content, namespace)
                except Exception as e:
                    logger.error(f"Error executing config file {filename}: {e}")

                # Extract uppercase variables for config
                for key, value in namespace.items():
                    if key.isupper() and not key.startswith('_'):
                        config[key] = value

            # JSON file
            elif filename.endswith('.json'):
                with open(file_path, 'r') as f:
                    config = json.load(f)

            # YAML file
            elif filename.endswith(('.yaml', '.yml')):
                try:
                    import yaml
                    with open(file_path, 'r') as f:
                        config = yaml.safe_load(f)
                except ImportError:
                    logger.error("YAML not supported. Install PyYAML to use YAML configuration files.")
        except Exception as e:
            logger.error(f"Error loading configuration from {file_path}: {e}")

        # Cache the result
        self._config_cache[filename] = config

        return config

    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides."""
        # For each settings key, check for environment variable override
        for key in list(self.settings.keys()):
            env_key = key.upper()
            if env_key in os.environ:
                # Convert environment variable to appropriate type
                env_value = os.environ[env_key]
                current_value = self.settings[key]

                try:
                    # Try to convert to the same type as the current value
                    if isinstance(current_value, bool):
                        self.settings[key] = env_value.lower() in ('true', 'yes', '1', 'y')
                    elif isinstance(current_value, int):
                        self.settings[key] = int(env_value)
                    elif isinstance(current_value, float):
                        self.settings[key] = float(env_value)
                    elif isinstance(current_value, list):
                        self.settings[key] = env_value.split(',')
                    elif isinstance(current_value, dict):
                        try:
                            self.settings[key] = json.loads(env_value)
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON in environment variable {env_key}")
                    else:
                        # String or other types
                        self.settings[key] = env_value
                except (ValueError, TypeError) as e:
                    logger.warning(
                        f"Error converting environment variable {env_key} to {type(current_value).__name__}: {e}")

    def get(self, key: str, default: Optional[T] = None) -> Union[Any, T]:
        """
        Get a configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        return self.settings.get(key, default)

    def get_int(self, key: str, default: Optional[int] = None) -> Optional[int]:
        """
        Get an integer configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Integer value or default
        """
        value = self.get(key, default)
        if value is None:
            return None

        try:
            return int(value)
        except (ValueError, TypeError):
            logger.warning(f"Invalid integer value for {key}: {value}")
            return default

    def get_float(self, key: str, default: Optional[float] = None) -> Optional[float]:
        """
        Get a float configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Float value or default
        """
        value = self.get(key, default)
        if value is None:
            return None

        try:
            return float(value)
        except (ValueError, TypeError):
            logger.warning(f"Invalid float value for {key}: {value}")
            return default

    def get_bool(self, key: str, default: Optional[bool] = None) -> Optional[bool]:
        """
        Get a boolean configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Boolean value or default
        """
        value = self.get(key, default)
        if value is None:
            return None

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            return value.lower() in ('true', 'yes', '1', 'y')

        try:
            return bool(value)
        except (ValueError, TypeError):
            logger.warning(f"Invalid boolean value for {key}: {value}")
            return default

    def get_list(self, key: str, default: Optional[List] = None) -> Optional[List]:
        """
        Get a list configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            List value or default
        """
        value = self.get(key, default)
        if value is None:
            return None

        if isinstance(value, list):
            return value

        if isinstance(value, str):
            return value.split(',')

        try:
            return list(value)
        except (ValueError, TypeError):
            logger.warning(f"Invalid list value for {key}: {value}")
            return default

    def get_dict(self, key: str, default: Optional[Dict] = None) -> Optional[Dict]:
        """
        Get a dictionary configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Dictionary value or default
        """
        value = self.get(key, default)
        if value is None:
            return None

        if isinstance(value, dict):
            return value

        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON string for {key}: {value}")
                return default

        logger.warning(f"Invalid dictionary value for {key}: {value}")
        return default

    def require(self, key: str) -> Any:
        """
        Get a required configuration value.

        Args:
            key: Configuration key

        Returns:
            Configuration value

        Raises:
            ConfigError: If the key is not found
        """
        value = self.get(key)
        if value is None:
            raise ConfigError(f"Required configuration key not found: {key}")
        return value

    def validate(self, validations: Dict[str, Callable[[Any], bool]]) -> None:
        """
        Validate configuration values.

        Args:
            validations: Dictionary of key -> validation function

        Raises:
            ConfigError: If validation fails
        """
        for key, validator in validations.items():
            value = self.get(key)
            if value is not None and not validator(value):
                raise ConfigError(f"Invalid configuration value for {key}: {value}")


# Global configuration manager instance
config = ConfigManager()


def get_config() -> ConfigManager:
    """
    Get the global configuration manager instance.

    Returns:
        Global configuration manager
    """
    return config