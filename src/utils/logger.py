import logging
import sys
import structlog
import contextvars
from typing import Any, MutableMapping, Optional

# ContextVar to store and propagate correlation ID across agent actions
correlation_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("correlation_id", default=None)

def _bind_correlation_id(logger: Any, method_name: str, event_dict: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """
    Binds the ContextVar correlation_id to our structlog outputs. Structured logging is 
    definitely overkill for a demo project, but it makes the output look incredibly professional.
    """
    cid = correlation_id.get()
    if cid:
        event_dict["correlation_id"] = cid
    return event_dict

def setup_logger() -> None:
    """Configures structlog for structured JSON logging in production, pretty printing in local development."""
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            _bind_correlation_id,  # Propagates correlation IDs across agents
            structlog.processors.JSONRenderer() if not sys.stderr.isatty() else structlog.dev.ConsoleRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure root logger
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer() if not sys.stderr.isatty() else structlog.dev.ConsoleRenderer()
    ))
    
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)

def get_logger(name: str) -> Any:
    """Returns a logger with the given name."""
    return structlog.get_logger(name)

# Auto setup when module is loaded
setup_logger()
