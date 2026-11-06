"""
Optional OpenTelemetry tracing for the API, workers and scheduler.

Off unless TRACING_ENABLED (it switches on automatically when
OTEL_EXPORTER_OTLP_ENDPOINT is set). When on, one trace follows a job end to
end: the HTTP request -> the database writes -> publishing to Redis -> the
worker running the task (Celery propagates the trace context in the message
headers) -> its database writes and any outgoing HTTP calls (webhooks).

Standard OTel environment variables apply, e.g.:
    OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
    OTEL_TRACES_SAMPLER=parentbased_traceidratio  OTEL_TRACES_SAMPLER_ARG=0.1
    OTEL_RESOURCE_ATTRIBUTES=deployment.environment=prod
"""

import logging
from typing import Optional

from app.config import settings

logger = logging.getLogger(__name__)

_configured: Optional[str] = None


def setup_tracing(service_name: str, fastapi_app=None) -> bool:
    """Configure tracing for this process (idempotent). Returns True if tracing is on."""
    global _configured
    if not settings.TRACING_ENABLED:
        return False
    if _configured is None:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.celery import CeleryInstrumentor
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
        from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create({SERVICE_NAME: service_name, SERVICE_VERSION: settings.APP_VERSION})
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)

        # Engines are created lazily (one per event loop), after this runs:
        # instrumenting globally covers every engine created from now on.
        SQLAlchemyInstrumentor().instrument(enable_commenter=False)
        CeleryInstrumentor().instrument()  # publish side (API, scheduler) and run side (workers)
        HTTPXClientInstrumentor().instrument()
        _configured = service_name
        logger.info("Tracing enabled: service=%s endpoint=%s", service_name, settings.OTEL_EXPORTER_OTLP_ENDPOINT)

    if fastapi_app is not None:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(fastapi_app, excluded_urls="health,health/ready,metrics")
    return True
