import re
from datetime import date
from io import BytesIO

from PIL import Image
from fastapi import UploadFile

from database.models.accounts import GenderEnum


def validate_name(name: str) -> str:
    if re.search(r"^[A-Za-z]*$", name) is None:
        raise ValueError(f"{name} contains non-english letters")
    return name.lower()


def validate_image(avatar: UploadFile) -> UploadFile:
    supported_image_formats = ["JPG", "JPEG", "PNG"]
    max_file_size = 1 * 1024 * 1024

    contents = avatar.file.read()
    if len(contents) > max_file_size:
        raise ValueError("Image size exceeds 1 MB")

    try:
        image = Image.open(BytesIO(contents))
        avatar.file.seek(0)
        image_format = image.format
        if image_format not in supported_image_formats:
            raise ValueError(
                f"Unsupported image format: {image_format}. Use one of next: {supported_image_formats}"
            )
    except IOError:
        raise ValueError("Invalid image format")
    return avatar


def validate_gender(gender: str) -> str:
    try:
        gender = GenderEnum(gender)
    except ValueError:
        raise ValueError(
            f"Gender must be one of: {', '.join(g.value for g in GenderEnum)}"
        )
    return gender


def validate_birth_date(birth_date: date) -> date:
    if birth_date.year < 1900:
        raise ValueError("Invalid birth date - year must be greater than 1900.")

    age = (date.today() - birth_date).days // 365
    if age < 18:
        raise ValueError("You must be at least 18 years old to register.")
    return birth_date


def validate_info(info: str = None) -> str:
    if info:
        striped_info = info.strip()
        if len(striped_info) != 0:
            return striped_info
    raise ValueError("Info field cannot be empty or contain only spaces.")
