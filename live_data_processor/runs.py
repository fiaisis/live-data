from collections.abc import Callable

from schemas.compiled_schemas.RunStart import RunStart


def _create_run_identifier(run_start: RunStart | None) -> tuple[str, str] | None:
    """
    Given a run start message, create a run identifier

    :param run_start: The run start message
    :return: A tuple containing the start time and run name
    """
    if run_start is None:
        return None
    return str(run_start.StartTime()), str(run_start.RunName())


class RunContext:
    def __init__(self, get_current_run: Callable[[], RunStart | None]) -> None:
        self.get_current_run = get_current_run
