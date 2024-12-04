from typing import Any

# import models into model package
from pydantic import BaseModel


class ValidatedModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        use_enum_values = True

    @classmethod
    def attribute_in_model(cls, attr_name: str):
        return attr_name in cls.model_fields

    def __init__(self, **data: dict[str, Any]):
        for name, _value in data.items():
            if not self.attribute_in_model(name):
                raise ValueError(
                    f"{name} is not a valid attribute for {self.__class__.__name__}"
                )
        super().__init__(**data)


from cohere.compass.models.config import *  # noqa: E402, F403
from cohere.compass.models.datasources import *  # noqa: E402, F403
from cohere.compass.models.documents import *  # noqa: E402, F403
from cohere.compass.models.rbac import *  # noqa: E402, F403
from cohere.compass.models.search import *  # noqa: E402, F403
