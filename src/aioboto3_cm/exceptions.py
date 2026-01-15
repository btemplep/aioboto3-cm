"""AIOBoto3 CM Exceptions
"""
__all__ = [
    "AIOBoto3CMError",
    "SessionNotFoundError",
    "SessionConflictError"
]

class AIOBoto3CMError(Exception):
    """Root Exception"""
    pass


class SessionNotFoundError(Exception):
    pass


class SessionConflictError(Exception):
    pass