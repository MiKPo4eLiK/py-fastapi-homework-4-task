from datetime import date
from typing import (
    Annotated,
    Optional,
)

from fastapi import (
    UploadFile,
    File,
)
from pydantic import (
    BaseModel,
    AfterValidator,
)

from validation import (
    validate_name,
    validate_image,
    validate_gender,
    validate_birth_date,
    validate_info,
)


class ProfileRequestSchema(BaseModel):
    first_name: Annotated[Optional[str], AfterValidator(validate_name)] = None
    last_name: Annotated[Optional[str], AfterValidator(validate_name)] = None
    gender: Annotated[Optional[str], AfterValidator(validate_gender)] = None
    date_of_birth: Annotated[Optional[date], AfterValidator(validate_birth_date)] = None
    info: Annotated[Optional[str], AfterValidator(validate_info)] = None
    avatar: Annotated[Optional[UploadFile], AfterValidator(validate_image), File()] = (
        None
    )


class ProfileResponseSchema(BaseModel):
    id: int | None
    user_id: int | None
    first_name: str | None
    last_name: str | None
    gender: str | None
    date_of_birth: date | None
    info: str | None
    avatar: str | None

    model_config = {"from_attributes": True}
