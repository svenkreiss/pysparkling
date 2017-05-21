import logging

log = logging.getLogger(__name__)


class TaskContext(object):
    def __init__(self, stage_id=0, partition_id=0,
                 max_retries=3, retry_wait=0):
        self.stage_id = stage_id
        self.partition_id = partition_id
        self.max_retries = max_retries
        self.retry_wait = retry_wait

        self.attempt_number = 0
        self.is_completed = False
        self.is_running_locally = True
        self.task_completion_listeners = []

    def _create_child(self):
        return TaskContext(stage_id=self.stage_id + 1,
                           partition_id=self.partition_id)

    def attemptNumber(self):
        return self.attempt_number

    def partitionId(self):
        return self.partition_id

    def stageId(self):
        return self.stage_id
