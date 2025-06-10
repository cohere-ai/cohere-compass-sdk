from typing import Any

# import models into model package
from pydantic import BaseModel


class ValidatedModel(BaseModel):
    """A subclass of BaseModel providing additional validation during initialization."""

    class Config:  # noqa: D106
        arbitrary_types_allowed = True
        use_enum_values = True

    @classmethod
    def attribute_in_model(cls, attr_name: str):
        """Check if a given attribute name is present in the model fields."""
        return attr_name in cls.model_fields

    def __init__(self, **data: dict[str, Any]):
        """
        Initialize the model with the given data.

        :param data: A dictionary of attribute names and their values.

        :raises ValueError: If an attribute name in the data is not valid for the model.
        """
        for name, _value in data.items():
            if not self.attribute_in_model(name):
                raise ValueError(
                    f"{name} is not a valid attribute for {self.__class__.__name__}"
                )
        super().__init__(**data)


from cohere_compass.models.config import *  # noqa: E402, F403
from cohere_compass.models.core import *  # noqa: E402, F403
from cohere_compass.models.datasources import *  # noqa: E402, F403
from cohere_compass.models.documents import *  # noqa: E402, F403
from cohere_compass.models.rbac import *  # noqa: E402, F403
from cohere_compass.models.search import *  # noqa: E402, F403
