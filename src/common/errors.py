class PipelineError(Exception):
    """Base class for all pipeline errors."""


class SourceError(PipelineError):
    """Error caused by external data source failure."""


class DataValidationError(PipelineError):
    """Error caused by data violating contract."""


class SystemError(PipelineError):
    """Internal pipeline/system error."""
