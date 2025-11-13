import re
from datetime import date
from io import BytesIO

from PIL import Image
from fastapi import UploadFile

from database.models.accounts import GenderEnum


def validate_name(name: str) -> str:
    """
    Validates that the name contains only English letters and is non-empty.
    Returns the lowercase version of the name.
    """
    if re.search(r"^[A-Za-z]+$", name) is None:
        raise ValueError(f"{name} contains non-English letters or is empty")
    return name.lower()


def validate_image(avatar: UploadFile) -> UploadFile:
    """
    Validates the uploaded image file.

    Checks:
    - File size must not exceed 1 MB.
    - Image format must be one of the supported formats: JPG, JPEG, PNG.
    - Resets file pointer to start regardless of success or failure.
    """
    supported_image_formats = ["JPG", "JPEG", "PNG"]
    max_file_size = 1 * 1024 * 1024  # 1 MB

    contents = avatar.file.read()
    if len(contents) > max_file_size:
        avatar.file.seek(0)
        raise ValueError("Image size exceeds 1 MB")

    try:
        image = Image.open(BytesIO(contents))
        # Normalize format for reliable comparison
        image_format = (image.format or "").upper()
        if image_format not in supported_image_formats:
            raise ValueError(
                f"Unsupported image format: {image_format}. "
                f"Use one of: {', '.join(supported_image_formats)}."
            )
    except IOError:
        raise ValueError("Invalid image format")
    finally:
        # Always reset the file pointer regardless of success or failure
        avatar.file.seek(0)

    return avatar


def validate_gender(gender: str) -> str:
    """
    Validates that the gender is one of the allowed values defined in GenderEnum.
    Returns the string value for JSON serialization and downstream consistency.
    """
    try:
        return GenderEnum(gender).value
    except ValueError:
        raise ValueError(
            f"Gender must be one of: {', '.join(g.value for g in GenderEnum)}"
        )


def validate_birth_date(birth_date: date) -> date:
    """
    Validates that the birth_date is realistic and that the user is at least 18 years old.
    Also ensures the date is not in the future.
    """
    if birth_date.year < 1900:
        raise ValueError("Invalid birth date - year must be greater than 1900.")

    today = date.today()
    if birth_date > today:
        raise ValueError("Birth date cannot be in the future.")

    age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    if age < 18:
        raise ValueError("You must be at least 18 years old to register.")
    return birth_date


def validate_info(info: str | None = None) -> str | None:
    """
    Validates optional info field.
    Strips whitespace and ensures non-empty content if provided.
    """
    if info is None:
        return None
    stripped_info = info.strip()
    if len(stripped_info) == 0:
        raise ValueError("Info field cannot be empty or contain only spaces.")
    return stripped_info
