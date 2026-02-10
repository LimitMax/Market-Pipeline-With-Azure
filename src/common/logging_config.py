import logging

class DefaultLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.event = getattr(record, "event", "-")
        record.run_id = getattr(record, "run_id", "-")
        return True

def setup_logging():
    """
    System logging configuration:
    - System logs -> file only
    - Progress logs -> print() only
    """

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    #HAPUS semua default handler
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    #FILE LOG (system / audit log)
    file_handler = logging.FileHandler("pipeline.log")

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(event)s | %(run_id)s | %(message)s"
    )
    file_handler.setFormatter(formatter)

    file_handler.addFilter(DefaultLogFilter())

    root.addHandler(file_handler)
