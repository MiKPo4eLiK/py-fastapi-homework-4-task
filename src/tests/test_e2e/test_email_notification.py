from email_validator import (
    validate_email,
    EmailNotValidError,
)
from sqlalchemy import select
from sqlalchemy.orm import joinedload
from validators import url as validate_url
import pytest
import httpx
from bs4 import BeautifulSoup
import asyncio

from database import (
    ActivationTokenModel,
    UserModel,
    RefreshTokenModel,
    PasswordResetTokenModel
)


async def fetch_email(mailhog_url, expected_to, expected_subject, timeout=5):
    """
    Fetch email from MailHog, retrying for a few seconds until the expected message appears.
    """
    async with httpx.AsyncClient() as client:
        for _ in range(timeout):
            resp = await client.get(mailhog_url)
            resp.raise_for_status()
            messages = resp.json()["items"]
            for msg in messages:
                to_header = msg["Content"]["Headers"]["To"]
                subject_header = msg["Content"]["Headers"].get("Subject", [""])[0]
                if expected_to in to_header and expected_subject in subject_header:
                    return msg
            await asyncio.sleep(1)
    raise AssertionError(f"Email to {expected_to} with subject '{expected_subject}' not found after {timeout}s")


@pytest.mark.e2e
@pytest.mark.order(1)
@pytest.mark.asyncio
async def test_registration(e2e_client, reset_db_once_for_e2e, settings, seed_user_groups, e2e_db_session) -> None:
    """
    End-to-end test for user registration.

    This test verifies the following:
    1. A user can successfully register with valid credentials.
    2. An activation email is sent to the provided email address.
    3. The email contains the correct activation link.

    Steps:
    - Send a POST request to the registration endpoint with user data.
    - Assert the response status code and returned user data.
    - Fetch the list of emails from MailHog via its API.
    - Verify that an email was sent to the expected recipient.
    - Ensure the email body contains the activation link.
    """
    user_data = {"email": "test@mate.com", "password": "StrongPassword123!"}

    # Send registration request
    response = await e2e_client.post("/api/v1/accounts/register/", json=user_data)
    assert response.status_code == 201, f"Expected 201, got {response.status_code}"
    response_data = response.json()
    assert response_data["email"] == user_data["email"]

    # Ensure DB state is committed before checking MailHog
    await e2e_db_session.commit()
    e2e_db_session.expire_all()

    # Fetch email from MailHog
    mailhog_url = f"http://{settings.EMAIL_HOST}:{settings.MAILHOG_API_PORT}/api/v2/messages"
    email = await fetch_email(mailhog_url, expected_to=user_data["email"], expected_subject="Account Activation")

    email_html = email["Content"]["Body"]
    email_subject = email["Content"]["Headers"].get("Subject", [""])[0]
    assert email_subject == "Account Activation", f"Expected subject 'Account Activation', but got '{email_subject}'"

    # Parse email HTML and validate content
    soup = BeautifulSoup(email_html, "html.parser")
    email_element = soup.find("strong", id="email")
    assert email_element is not None, "Email element with id 'email' not found!"
    try:
        validate_email(email_element.text)
    except EmailNotValidError as e:
        pytest.fail(f"The email link {email_element.text} is not valid: {e}")
    assert email_element.text == user_data["email"], "Email content does not match!"

    link_element = soup.find("a", id="link")
    assert link_element is not None, "Activation link element with id 'link' not found!"
    activation_url = link_element["href"]
    assert validate_url(activation_url), f"The URL '{activation_url}' is not valid!"


@pytest.mark.e2e
@pytest.mark.order(2)
@pytest.mark.asyncio
async def test_account_activation(e2e_client, settings, e2e_db_session) -> None:
    """
    End-to-end test for account activation.

    This test verifies the following:
    1. The activation token is valid.
    2. The account can be activated using the token.
    3. The account's status is updated to active in the database.
    4. An email confirming activation is sent to the user.

    Steps:
    - Retrieve the activation token from the database.
    - Send a POST request to the activation endpoint with the token.
    - Assert the response status code and verify the account is activated.
    - Fetch the list of emails from MailHog via its API.
    - Verify the email sent confirms the activation and contains the expected details.
    """
    user_email = "test@mate.com"

    # Retrieve activation token from DB
    stmt = select(ActivationTokenModel).join(UserModel).where(UserModel.email == user_email)
    result = await e2e_db_session.execute(stmt)
    activation_token_record = result.scalars().first()
    assert activation_token_record, f"Activation token for email {user_email} not found!"
    token_value = activation_token_record.token

    # Send activation request
    activation_url = "/api/v1/accounts/activate/"
    response = await e2e_client.post(activation_url, json={"email": user_email, "token": token_value})
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    response_data = response.json()
    assert response_data["message"] == "User account activated successfully.", "Unexpected activation message!"

    await e2e_db_session.commit()
    e2e_db_session.expire_all()

    # Verify user is active in DB
    stmt_user = select(UserModel).where(UserModel.email == user_email)
    result_user = await e2e_db_session.execute(stmt_user)
    activated_user = result_user.scalars().first()
    assert activated_user.is_active, f"User {user_email} is not active!"

    # Verify activation email
    mailhog_url = f"http://{settings.EMAIL_HOST}:{settings.MAILHOG_API_PORT}/api/v2/messages"
    email = await fetch_email(mailhog_url, expected_to=user_email, expected_subject="Account Activated Successfully")

    email_html = email["Content"]["Body"]
    soup = BeautifulSoup(email_html, "html.parser")

    email_element = soup.find("strong", id="email")
    assert email_element is not None, "Email element with id 'email' not found!"
    try:
        validate_email(email_element.text)
    except EmailNotValidError as e:
        pytest.fail(f"The email link {email_element.text} is not valid: {e}")
    assert email_element.text == user_email, "Email content does not match the user's email!"

    link_element = soup.find("a", id="link")
    assert link_element is not None, "Login link element with id 'link' not found!"
    login_url = link_element["href"]
    assert validate_url(login_url), f"The URL '{login_url}' is not valid!"


# ---- User login tests ----
@pytest.mark.e2e
@pytest.mark.order(3)
@pytest.mark.asyncio
async def test_user_login(e2e_client, e2e_db_session) -> None:
    """
    End-to-end test for user login (async version).

    This test verifies the following:
    1. A user can log in with valid credentials.
    2. The API returns an access token and a refresh token.
    3. The refresh token is stored in the database.

    Steps:
    - Send a POST request to the login endpoint with the user's credentials.
    - Assert the response status code and verify the returned access and refresh tokens.
    - Validate that the refresh token is stored in the database.
    """
    user_data = {"email": "test@mate.com", "password": "StrongPassword123!"}

    login_url = "/api/v1/accounts/login/"
    response = await e2e_client.post(login_url, json=user_data)
    assert response.status_code == 201, f"Expected status code 201, got {response.status_code}"
    response_data = response.json()
    assert "access_token" in response_data, "Access token is missing in the response!"
    assert "refresh_token" in response_data, "Refresh token is missing in the response!"

    refresh_token = response_data["refresh_token"]

    await e2e_db_session.commit()
    e2e_db_session.expire_all()

    stmt = select(RefreshTokenModel).options(joinedload(RefreshTokenModel.user)).where(
        RefreshTokenModel.token == refresh_token
    )
    result = await e2e_db_session.execute(stmt)
    stored_token = result.scalars().first()
    assert stored_token is not None, "Refresh token was not stored in the database!"
    assert stored_token.user.email == user_data["email"], "Refresh token is linked to the wrong user!"


# ---- Password reset tests ----
@pytest.mark.e2e
@pytest.mark.order(4)
@pytest.mark.asyncio
async def test_request_password_reset(e2e_client, e2e_db_session, settings) -> None:
    """
    End-to-end test for requesting a password reset (async version).

    This test verifies the following:
    1. If the user exists and is active, a password reset token is generated.
    2. A password reset email is sent to the user.
    3. The email contains the correct reset link.

    Steps:
    - Send a POST request to the password reset request endpoint.
    - Assert the response status code and message.
    - Verify that a password reset token is created for the user.
    - Fetch the list of emails from MailHog via its API.
    - Verify the email was sent and contains the correct information.
    """
    user_email = "test@mate.com"
    reset_url = "/api/v1/accounts/password-reset/request/"

    response = await e2e_client.post(reset_url, json={"email": user_email})
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    response_data = response.json()
    assert response_data["message"] == "If you are registered, you will receive an email with instructions."

    await e2e_db_session.commit()
    e2e_db_session.expire_all()

    stmt = select(PasswordResetTokenModel).join(UserModel).where(UserModel.email == user_email)
    reset_token = (await e2e_db_session.execute(stmt)).scalars().first()
    assert reset_token, f"Password reset token for email {user_email} was not created!"

    mailhog_url = f"http://{settings.EMAIL_HOST}:{settings.MAILHOG_API_PORT}/api/v2/messages"
    email_data = await fetch_email(mailhog_url, expected_to=user_email, expected_subject="Password Reset Request")

    email_html = email_data["Content"]["Body"]
    soup = BeautifulSoup(email_html, "html.parser")

    email_element = soup.find("strong", id="email")
    assert email_element is not None, "Email element with id 'email' not found!"
    validate_email(email_element.text)
    assert email_element.text == user_email, "Email content does not match the user's email!"

    link_element = soup.find("a", id="link")
    assert link_element is not None, "Reset link element with id 'link' not found!"
    assert validate_url(link_element["href"])
