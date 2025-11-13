import os

from fastapi import (
    APIRouter,
    Depends,
    status,
    Header,
    Path,
    HTTPException,
    Form,
)
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Annotated
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from config import (
    get_jwt_auth_manager,
    get_s3_storage_client,
)
from exceptions import (
    S3ConnectionError,
    S3FileUploadError,
    TokenExpiredError,
    InvalidTokenError,
)
from schemas.profiles import (
    ProfileRequestSchema,
    ProfileResponseSchema,
)
from database import (
    get_db,
    UserModel,
    UserProfileModel,
    UserGroupModel,
)
from security.interfaces import JWTAuthManagerInterface
from storages import S3StorageInterface


router = APIRouter()


@router.post(
    "/users/{user_id}/profile/",
    response_model=ProfileResponseSchema,
    status_code=status.HTTP_201_CREATED,
)
async def create_profile(
    profile_data: Annotated[ProfileRequestSchema, Form()],
    user_id: Annotated[int, Path()],
    header: Annotated[str | None, Header(alias="authorization")] = None,
    jwt_manager: JWTAuthManagerInterface = Depends(get_jwt_auth_manager),
    db: AsyncSession = Depends(get_db),
    s3_client: S3StorageInterface = Depends(get_s3_storage_client),
):
    if not header:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header is missing.",
        )

    parts = header.split()
    if len(parts) != 2 or parts[0] != "Bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Authorization header format. Expected 'Bearer <token>'.",
        )

    token = parts[1]
    try:
        token_data = jwt_manager.decode_access_token(token)
    except TokenExpiredError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired.",
        )
    except InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token.",
        )

    token_user_id = token_data.get("user_id")

    query = (
        select(UserModel)
        .options(joinedload(UserModel.profile))
        .where(UserModel.id == user_id)
    )
    user = (await db.execute(query)).scalars().first()

    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found or not active.",
        )

    if token_user_id != user_id:
        query = (
            select(UserGroupModel.name)
            .join(UserModel)
            .where(UserModel.id == token_user_id)
        )
        token_user_group_name = (await db.execute(query)).scalar_one_or_none()
        if token_user_group_name != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to edit this profile.",
            )

    if user.profile:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User already has a profile.",
        )

    file_url = None
    if profile_data.avatar:
        uploaded_file = profile_data.avatar
        try:
            extension = os.path.splitext(uploaded_file.filename)[1] or ".jpg"
            new_filename = f"avatars/{user_id}_avatar{extension}"

            file_bytes = await uploaded_file.read()
            await s3_client.upload_file(file_name=new_filename, file_data=file_bytes)

            file_url = await s3_client.get_file_url(file_name=new_filename)
        except (S3FileUploadError, S3ConnectionError) as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to upload avatar. Please try again later.",
            ) from e
    else:
        new_filename = None

    profile_dict = profile_data.model_dump(exclude_unset=True)
    profile_dict["avatar"] = new_filename
    profile_dict["user_id"] = user_id

    profile = UserProfileModel(**profile_dict)
    db.add(profile)
    await db.commit()
    await db.refresh(profile)

    response_data = ProfileResponseSchema.model_validate(profile, from_attributes=True)
    response_data.avatar = file_url
    return response_data
