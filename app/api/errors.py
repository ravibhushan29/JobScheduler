"""
The single error envelope used by every non-2xx response:

    {
      "error": {
        "status_code": 404,
        "type": "not_found",
        "message": "Unknown task_id: 8f14e45f-...",
        "details": null
      }
    }

Endpoints simply `raise HTTPException(status_code, detail)`; the handlers
registered by `register_exception_handlers` reshape it. They catch
Starlette's HTTPException (the base class), so framework-raised errors such
as an unknown route (404) or wrong method (405) get the same shape.
"""

import json
import logging
from enum import Enum
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import exc as sa_exc
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)


class ErrorType(str, Enum):
    """Machine-readable error category. Clients should branch on this, not on `message`."""

    BAD_REQUEST = "bad_request"
    UNAUTHORIZED = "unauthorized"
    FORBIDDEN = "forbidden"
    NOT_FOUND = "not_found"
    METHOD_NOT_ALLOWED = "method_not_allowed"
    CONFLICT = "conflict"
    VALIDATION_ERROR = "validation_error"
    SERVICE_UNAVAILABLE = "service_unavailable"
    INTERNAL_ERROR = "internal_error"
    HTTP_ERROR = "http_error"  # any other status code


_TYPE_BY_STATUS = {
    400: ErrorType.BAD_REQUEST,
    401: ErrorType.UNAUTHORIZED,
    403: ErrorType.FORBIDDEN,
    404: ErrorType.NOT_FOUND,
    405: ErrorType.METHOD_NOT_ALLOWED,
    409: ErrorType.CONFLICT,
    422: ErrorType.VALIDATION_ERROR,
    500: ErrorType.INTERNAL_ERROR,
    503: ErrorType.SERVICE_UNAVAILABLE,
}


class ErrorDetail(BaseModel):
    status_code: int = Field(..., examples=[404])
    type: ErrorType = Field(..., examples=[ErrorType.NOT_FOUND])
    message: str = Field(..., examples=["Unknown task_id: 8f14e45f-fceb-4dc9-9a0f-example"])
    details: Optional[Any] = Field(
        default=None, description="Extra structured context, e.g. field-level validation errors."
    )


class ErrorResponse(BaseModel):
    """Body of every non-2xx response."""

    error: ErrorDetail


_DESCRIPTIONS = {
    400: "The request is not valid for the resource's current state.",
    401: "Missing or invalid API key.",
    404: "The requested resource does not exist.",
    409: "The request conflicts with the resource's current state.",
    422: "The request failed schema validation.",
    503: "A dependency (task queue or database) is unavailable.",
}


def error_responses(*status_codes: int) -> dict:
    """Build a route's `responses=` mapping so /docs shows the error envelope for each code."""
    return {code: {"model": ErrorResponse, "description": _DESCRIPTIONS.get(code, "Error.")} for code in status_codes}


def error_body(status_code: int, message: str, details: Any = None) -> dict:
    """The JSON body for an error response."""
    error_type = _TYPE_BY_STATUS.get(status_code, ErrorType.HTTP_ERROR)
    return ErrorResponse(
        error=ErrorDetail(status_code=status_code, type=error_type, message=message, details=details)
    ).model_dump(mode="json")


async def _http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content=error_body(exc.status_code, str(exc.detail)),
        headers=getattr(exc, "headers", None),
    )


async def _validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    # Errors from custom validators carry the original exception object in
    # `ctx`, which isn't JSON-serializable; stringify it rather than crash.
    details = json.loads(json.dumps(exc.errors(), default=str))
    return JSONResponse(
        status_code=422,
        content=error_body(422, "Request validation failed; see 'details' for each field error.", details),
    )


async def _database_unavailable_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Database unavailable on %s %s: %s", request.method, request.url.path, exc)
    return JSONResponse(status_code=503, content=error_body(503, "The database is unavailable; try again shortly."))


async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content=error_body(500, "Internal server error."))


def register_exception_handlers(app: FastAPI) -> None:
    """Install the handlers that give every error the envelope above."""
    app.add_exception_handler(StarletteHTTPException, _http_exception_handler)
    app.add_exception_handler(RequestValidationError, _validation_exception_handler)
    for db_error in (sa_exc.OperationalError, sa_exc.InterfaceError, sa_exc.TimeoutError):
        app.add_exception_handler(db_error, _database_unavailable_handler)
    app.add_exception_handler(Exception, _unhandled_exception_handler)
