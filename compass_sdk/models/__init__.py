# import models into model package
from pydantic import BaseModel


class ValidatedModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        use_enum_values = True

    @classmethod
    def attribute_in_model(cls, attr_name):
        return attr_name in cls.__fields__

    def __init__(self, **data):
        for name, value in data.items():
            if not self.attribute_in_model(name):
                raise ValueError(
                    f"{name} is not a valid attribute for {self.__class__.__name__}"
                )
        super().__init__(**data)


from compass_sdk.models.config import *  # noqa: E402, F403
from compass_sdk.models.datasources import *  # noqa: E402, F403
from compass_sdk.models.documents import *  # noqa: E402, F403
from compass_sdk.models.rbac import *  # noqa: E402, F403
from compass_sdk.models.search import *  # noqa: E402, F403
