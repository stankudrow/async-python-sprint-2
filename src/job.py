from datetime import datetime


class Job:
    def __init__(
        self,
        start_at: None | datetime | str = None,
        max_working_time=-1,
        max_retries=0,
        dependencies=[]
    ):
        pass

    def run(self):
        pass

    def pause(self):
        pass

    def stop(self):
        pass
