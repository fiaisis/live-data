from enum import Enum, auto


class States(Enum):
    WAITINGFORRUN = auto()
    INITIALIZINGWORKSPACE = auto()
    REALTIMEPROCESSING = auto()


class ReductionContext:
    def __init__(self):
        self.current_state = States.WAITINGFORRUN

    def process_event(self, event):
        match (self.current_state, event):
            case (States.INITIALIZINGWORKSPACE, "initializing"):
                self.current_state = States.INITIALIZINGWORKSPACE
            case (States.REALTIMEPROCESSING, "realtime"):
                self.current_state = States.REALTIMEPROCESSING
            case (States.REALTIMEPROCESSING, "waiting"):
                self.current_state = States.WAITINGFORRUN
