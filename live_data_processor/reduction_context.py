from enum import Enum, auto


class States(Enum):
    WAITINGFORRUN = auto()
    INITIALIZINGWORKSPACE = auto()
    REALTIME = auto()


class ReductionContext:
    def __init__(self):
        self.current_state = States.WAITINGFORRUN

    def process_event(self, event):
        match (self.current_state, event):
            case (States.WAITINGFORRUN, "waiting"):
                self.current_state = States.WAITINGFORRUN
            case (States.INITIALIZINGWORKSPACE, "initializing"):
                self.current_state = States.INITIALIZINGWORKSPACE
            case (States.REALTIME, "realtime"):
                self.current_state = States.REALTIME
            case (States.REALTIME, "waiting"):
                self.current_state = States.WAITINGFORRUN
