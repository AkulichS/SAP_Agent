"""
mailer.py — outbound e-mail, used for one thing: password delivery.

The password rules the product enforces only work if a generated password can reach
its owner without a human reading it: a reset mints a random password, mails it to the
user, and flags the account so the next login must replace it. Nobody — not even the
sys_admin who pressed the button — needs to see it.

Configuration is entirely environment-driven (`src/.env`), because SMTP credentials are
deployment secrets, not config-store data:

    SMTP_HOST        mail server host          (unset ⇒ e-mail disabled)
    SMTP_PORT        default 587 (25 for `none`, 465 for `ssl`)
    SMTP_USER        login user (optional — omit for an open relay)
    SMTP_PASSWORD    login password
    SMTP_FROM        envelope/From address     (default: SMTP_USER)
    SMTP_SECURITY    starttls (default) | ssl | none
    SMTP_TIMEOUT     socket timeout seconds    (default 15)
    APP_URL          base URL put in the mail so the user can click through

`is_configured()` is False on a deployment that never set SMTP_HOST — the callers then
degrade instead of failing: a sys_admin reset shows the generated password on screen to
hand over out-of-band, and the self-service "forgot password" flow reports that e-mail
delivery is not available. Nothing here ever raises past its caller unnoticed:
`send_password_email` raises `MailError` with the server's own message.

Every call blocks on a socket, so web callers must run it in a threadpool.
"""

from __future__ import annotations

import logging
import os
import smtplib
import ssl as ssl_mod
from email.message import EmailMessage

logger = logging.getLogger(__name__)


class MailError(RuntimeError):
    """Delivery failed — the message text is safe to show an administrator."""


_DEFAULT_PORTS = {"ssl": 465, "none": 25, "starttls": 587}


def _security() -> str:
    mode = (os.environ.get("SMTP_SECURITY") or "starttls").strip().lower()
    return mode if mode in _DEFAULT_PORTS else "starttls"


def is_configured() -> bool:
    """True when this deployment can send mail at all (SMTP_HOST is set)."""
    return bool((os.environ.get("SMTP_HOST") or "").strip())


def sender_address() -> str:
    return ((os.environ.get("SMTP_FROM") or os.environ.get("SMTP_USER") or "").strip()
            or "no-reply@sap-period-close.local")


def send_message(to: str, subject: str, body: str) -> None:
    """Send one plain-text message. Raises MailError on any failure."""
    if not is_configured():
        raise MailError("E-mail is not configured on this deployment (SMTP_HOST unset)")
    if not (to or "").strip():
        raise MailError("No recipient address")

    mode = _security()
    host = os.environ["SMTP_HOST"].strip()
    port = int(os.environ.get("SMTP_PORT") or _DEFAULT_PORTS[mode])
    timeout = int(os.environ.get("SMTP_TIMEOUT") or 15)
    user = (os.environ.get("SMTP_USER") or "").strip()
    password = os.environ.get("SMTP_PASSWORD") or ""

    msg = EmailMessage()
    msg["From"] = sender_address()
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        if mode == "ssl":
            server = smtplib.SMTP_SSL(host, port, timeout=timeout,
                                      context=ssl_mod.create_default_context())
        else:
            server = smtplib.SMTP(host, port, timeout=timeout)
        with server:
            if mode == "starttls":
                server.starttls(context=ssl_mod.create_default_context())
            if user:
                server.login(user, password)
            server.send_message(msg)
    except (smtplib.SMTPException, OSError) as exc:
        logger.warning("Mail to %s failed: %s", to, exc)
        raise MailError(f"Could not send e-mail: {exc}") from exc
    logger.info("Password e-mail sent to %s", to)


def send_password_email(to: str, username: str, password: str, *, reset: bool) -> None:
    """Deliver a freshly generated password to its owner.

    `reset=False` is the initial password minted when the account is created; True is a
    reset of an existing one. Both are one-time: the account is flagged so the first
    login with it must set a password only the user knows."""
    what = "reset" if reset else "created"
    url = (os.environ.get("APP_URL") or "").strip()
    where = f"\nSign in at: {url}\n" if url else ""
    send_message(
        to,
        "SAP Period Close — your password has been " + what,
        f"Your SAP Period Close account password has been {what}.\n"
        f"\n"
        f"  Username: {username}\n"
        f"  Password: {password}\n"
        f"{where}"
        f"\n"
        f"This password is temporary: the first time you sign in with it you will be\n"
        f"asked to replace it with one only you know. Nobody else has been shown it.\n"
        f"\n"
        f"If you did not expect this message, tell your system administrator.\n",
    )
