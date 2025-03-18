"""
Mapping framework for converting between DTOs and external data formats.
Similar to AutoMapper in C#.
"""
import logging
from typing import Dict, Any, TypeVar, Generic, Type, Callable, Optional, List, Union

logger = logging.getLogger(__name__)

# Type variables
S = TypeVar('S')  # Source type
T = TypeVar('T')  # Target type


class MappingError(Exception):
    """Error during data mapping."""
    pass


class Mapper(Generic[S, T]):
    """
    Generic mapper for converting between data types.

    This class provides functionality similar to AutoMapper in C#,
    allowing for declarative mapping between source and target objects.
    """

    def __init__(self, target_type: Type[T]):
        """
        Initialize the mapper.

        Args:
            target_type: Type of target objects
        """
        self.target_type = target_type
        self.field_mappings: Dict[str, Union[str, Callable]] = {}
        self.value_converters: Dict[str, Callable] = {}
        self.custom_mapping: Optional[Callable] = None
        self.fallback_values: Dict[str, Any] = {}

    def map_field(self, target_field: str, source_field: str) -> 'Mapper[S, T]':
        """
        Map a field from source to target.

        Args:
            target_field: Field name in target object
            source_field: Field name in source object

        Returns:
            Self for chaining
        """
        self.field_mappings[target_field] = source_field
        return self

    def map_field_with_converter(self, target_field: str, source_field: str,
                                 converter: Callable) -> 'Mapper[S, T]':
        """
        Map a field with a converter function.

        Args:
            target_field: Field name in target object
            source_field: Field name in source object
            converter: Function to convert the value

        Returns:
            Self for chaining
        """
        self.field_mappings[target_field] = source_field
        self.value_converters[target_field] = converter
        return self

    def map_with_custom_function(self, mapping_func: Callable[[Dict[str, Any]], Dict[str, Any]]) -> 'Mapper[S, T]':
        """
        Set a custom mapping function.

        Args:
            mapping_func: Function that takes source dict and returns target dict

        Returns:
            Self for chaining
        """
        self.custom_mapping = mapping_func
        return self

    def set_default_value(self, field: str, value: Any) -> 'Mapper[S, T]':
        """
        Set a default value for a field.

        Args:
            field: Field name
            value: Default value

        Returns:
            Self for chaining
        """
        self.fallback_values[field] = value
        return self

    def map(self, source: Union[S, Dict[str, Any]]) -> T:
        """
        Map a single source object to target type.

        Args:
            source: Source object or dictionary

        Returns:
            Target object

        Raises:
            MappingError: If mapping fails
        """
        try:
            # Convert source to dictionary if it's not already
            source_dict = source if isinstance(source, dict) else self._object_to_dict(source)

            # Apply custom mapping if defined
            if self.custom_mapping:
                target_dict = self.custom_mapping(source_dict)
            else:
                # Apply field mappings
                target_dict = self._apply_field_mappings(source_dict)

            # Create target object
            return self._create_target(target_dict)
        except Exception as e:
            logger.error(f"Error mapping to {self.target_type.__name__}: {e}")
            raise MappingError(f"Failed to map to {self.target_type.__name__}: {e}")

    def map_many(self, sources: List[Union[S, Dict[str, Any]]]) -> List[T]:
        """
        Map multiple source objects to target type.

        Args:
            sources: List of source objects or dictionaries

        Returns:
            List of target objects
        """
        result = []
        for source in sources:
            try:
                target = self.map(source)
                result.append(target)
            except Exception as e:
                logger.warning(f"Error mapping item: {e}")
                # Continue with next item

        return result

    def _apply_field_mappings(self, source_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply field mappings to source dictionary.

        Args:
            source_dict: Source dictionary

        Returns:
            Target dictionary
        """
        target_dict = {}

        # Apply field mappings
        for target_field, source_field in self.field_mappings.items():
            if isinstance(source_field, str):
                # Simple field mapping
                value = self._get_nested_value(source_dict, source_field)

                # Apply converter if defined
                if target_field in self.value_converters:
                    try:
                        value = self.value_converters[target_field](value)
                    except Exception as e:
                        logger.warning(f"Error converting field {target_field}: {e}")
                        value = None
            else:
                # Source field is a function
                try:
                    value = source_field(source_dict)
                except Exception as e:
                    logger.warning(f"Error computing field {target_field}: {e}")
                    value = None

            # Set value in target dict
            if value is not None:
                target_dict[target_field] = value
            elif target_field in self.fallback_values:
                target_dict[target_field] = self.fallback_values[target_field]

        return target_dict

    def _get_nested_value(self, data: Dict[str, Any], field: str) -> Any:
        """
        Get a value from a nested dictionary using dot notation.

        Args:
            data: Dictionary to get value from
            field: Field name with dot notation (e.g., 'user.name')

        Returns:
            Field value or None if not found
        """
        if '.' not in field:
            return data.get(field)

        parts = field.split('.')
        current = data

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current.get(part)
            else:
                return None

        return current

    def _object_to_dict(self, obj: Any) -> Dict[str, Any]:
        """
        Convert an object to a dictionary.

        Args:
            obj: Object to convert

        Returns:
            Dictionary representation
        """
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        elif hasattr(obj, '__dict__'):
            return obj.__dict__.copy()
        else:
            return {}

    def _create_target(self, target_dict: Dict[str, Any]) -> T:
        """
        Create a target object from a dictionary.

        Args:
            target_dict: Dictionary with target values

        Returns:
            Target object
        """
        return self.target_type(**target_dict)


class MapperRegistry:
    """
    Registry for mappers.

    This class provides a central registry for all mappers,
    allowing them to be reused throughout the application.
    """

    def __init__(self):
        """Initialize the registry."""
        self.mappers = {}

    def register(self, name: str, mapper: Mapper) -> None:
        """
        Register a mapper.

        Args:
            name: Mapper name
            mapper: Mapper instance
        """
        self.mappers[name] = mapper

    def get(self, name: str) -> Optional[Mapper]:
        """
        Get a mapper by name.

        Args:
            name: Mapper name

        Returns:
            Mapper instance or None if not found
        """
        return self.mappers.get(name)

    def map(self, name: str, source: Any) -> Any:
        """
        Map a source object using a named mapper.

        Args:
            name: Mapper name
            source: Source object

        Returns:
            Mapped object

        Raises:
            MappingError: If mapper not found or mapping fails
        """
        mapper = self.get(name)
        if not mapper:
            raise MappingError(f"Mapper not found: {name}")

        return mapper.map(source)

    def map_many(self, name: str, sources: List[Any]) -> List[Any]:
        """
        Map multiple source objects using a named mapper.

        Args:
            name: Mapper name
            sources: Source objects

        Returns:
            Mapped objects

        Raises:
            MappingError: If mapper not found
        """
        mapper = self.get(name)
        if not mapper:
            raise MappingError(f"Mapper not found: {name}")

        return mapper.map_many(sources)


# Global mapper registry
mapper_registry = MapperRegistry()


def get_mapper_registry() -> MapperRegistry:
    """
    Get the global mapper registry.

    Returns:
        Global mapper registry
    """
    return mapper_registry