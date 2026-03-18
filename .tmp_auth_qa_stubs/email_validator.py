class EmailNotValidError(ValueError):
    pass


class _Result:
    def __init__(self, email: str):
        local_part, _, domain = email.partition("@")
        self.email = email
        self.normalized = email
        self.local_part = local_part
        self.domain = domain


def validate_email(email: str, *args, **kwargs):
    if not isinstance(email, str) or "@" not in email or "." not in email.split("@")[-1]:
        raise EmailNotValidError("Invalid email address")
    return _Result(email)
