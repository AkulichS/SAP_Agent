"""mailer.py — password delivery over SMTP, with no network in sight.

The SMTP client is faked; what is asserted is the contract the web layer leans on:
an unconfigured deployment says so instead of pretending, a server error surfaces as
`MailError` rather than an exception nobody catches, and the generated password
actually reaches the message body.
"""

import smtplib

import pytest

import mailer

_ENV = ("SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD", "SMTP_FROM",
        "SMTP_SECURITY", "SMTP_TIMEOUT", "APP_URL")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in _ENV:
        monkeypatch.delenv(name, raising=False)


class FakeSMTP:
    """Records what a send would have done. `fail` makes the send raise like a server."""
    instances: list["FakeSMTP"] = []
    fail = False

    def __init__(self, host, port, timeout=None, context=None):
        self.host, self.port = host, port
        self.started_tls = False
        self.login_args = None
        self.sent = []
        FakeSMTP.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def starttls(self, context=None):
        self.started_tls = True

    def login(self, user, password):
        self.login_args = (user, password)

    def send_message(self, msg):
        if FakeSMTP.fail:
            raise smtplib.SMTPRecipientsRefused({})
        self.sent.append(msg)


@pytest.fixture
def smtp(monkeypatch):
    FakeSMTP.instances, FakeSMTP.fail = [], False
    monkeypatch.setattr(mailer.smtplib, "SMTP", FakeSMTP)
    monkeypatch.setattr(mailer.smtplib, "SMTP_SSL", FakeSMTP)
    return FakeSMTP


def test_unconfigured_deployment_is_not_a_silent_no_op():
    assert mailer.is_configured() is False
    with pytest.raises(mailer.MailError):
        mailer.send_message("u@example.com", "subject", "body")


def test_send_uses_starttls_and_the_default_port(monkeypatch, smtp):
    monkeypatch.setenv("SMTP_HOST", "mail.example.com")
    monkeypatch.setenv("SMTP_USER", "robot@example.com")
    monkeypatch.setenv("SMTP_PASSWORD", "pw")
    assert mailer.is_configured() is True

    mailer.send_message("u@example.com", "Subject", "Body")
    conn = smtp.instances[0]
    assert (conn.host, conn.port) == ("mail.example.com", 587)
    assert conn.started_tls and conn.login_args == ("robot@example.com", "pw")
    assert conn.sent[0]["To"] == "u@example.com"
    assert conn.sent[0]["From"] == "robot@example.com"      # falls back to SMTP_USER


def test_ssl_mode_skips_starttls_and_uses_465(monkeypatch, smtp):
    monkeypatch.setenv("SMTP_HOST", "mail.example.com")
    monkeypatch.setenv("SMTP_SECURITY", "ssl")
    mailer.send_message("u@example.com", "Subject", "Body")
    conn = smtp.instances[0]
    assert conn.port == 465 and not conn.started_tls
    assert conn.login_args is None                          # no SMTP_USER ⇒ no login


def test_server_refusal_becomes_a_mail_error(monkeypatch, smtp):
    monkeypatch.setenv("SMTP_HOST", "mail.example.com")
    smtp.fail = True
    with pytest.raises(mailer.MailError):
        mailer.send_message("u@example.com", "Subject", "Body")


def test_password_email_carries_the_credentials(monkeypatch, smtp):
    monkeypatch.setenv("SMTP_HOST", "mail.example.com")
    monkeypatch.setenv("APP_URL", "https://close.example.com")
    mailer.send_password_email("u@example.com", "ivanov", "Tmp4Pass9xYz", reset=True)
    body = smtp.instances[0].sent[0].get_content()
    assert "ivanov" in body and "Tmp4Pass9xYz" in body
    assert "https://close.example.com" in body
    assert "temporary" in body                              # says it must be replaced
