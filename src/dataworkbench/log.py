# dataworkbench/log.py
import logging
import sys


def setup_logger(
    name: str | None = None,
    level: int = logging.INFO,
    format_string: str | None = None,
    add_console_handler: bool = True,
    add_file_handler: bool = False,
    file_path: str | None = None,
    propagate: bool = False,
) -> logging.Logger:
    """
    Configure and return a logger with consistent settings across the dataworkbench package.

    Args:
        name: Logger name (defaults to the caller's module name if None)
        level: The logging level to set
        format_string: Custom format string for log messages (uses a default if None)
        add_console_handler: Whether to add a StreamHandler for console output
        add_file_handler: Whether to add a FileHandler for file output
        file_path: Path to log file (required if add_file_handler is True)
        propagate: Whether the logger should propagate to parent loggers
        extra_handlers: Additional handlers to add

    Returns:
        A configured logger instance
    """
    if name is None:
        # Get the caller's module name if not provided
        import inspect

        frame = inspect.currentframe().f_back
        name = frame.f_globals["__name__"]

    # Use a default format if none provided
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    formatter = logging.Formatter(format_string)

    # Get or create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = propagate

    # Clear existing handlers to avoid duplication
    if logger.handlers:
        logger.handlers.clear()

    # Add console handler if requested
    if add_console_handler:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(level)
        logger.addHandler(console_handler)

    # Add file handler if requested
    if add_file_handler:
        if file_path is None:
            raise ValueError("file_path must be provided when add_file_handler is True")

        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)

    return logger
