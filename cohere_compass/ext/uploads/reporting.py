"""Progress events of an upload job, for callers that display or log them."""

from cohere_compass.ext.uploads.models import FileStatus, SourceFile, UploadOutcome
from cohere_compass.ext.uploads.pruning import PruneResult


class UploadReporter:
    """
    Receives an upload job's progress. Every method does nothing by default.

    Subclass it and override the events you need. `run_upload_job` calls the
    methods from the event loop, so they must return quickly and must not block.
    """

    def job_started(self, job_id: str | None, location: str | None, total_files: int, workers: int) -> None:
        """Handle the start of the job. *job_id* and *location* are None for a dry run."""

    def file_phase_changed(self, source: SourceFile, status: FileStatus) -> None:
        """Handle a file moving to a new phase, such as hashing or processing."""

    def file_outcome(self, source: SourceFile, outcome: UploadOutcome) -> None:
        """Handle the job being done with a file for this run."""

    def prune_completed(self, result: PruneResult) -> None:
        """Handle the end of pruning, or of working out what a dry run would prune."""

    def job_interrupted(self, job_id: str | None) -> None:
        """Handle the cancellation of the job. *job_id* is None for a dry run."""

    def job_finished(self) -> None:
        """Handle the end of the run, whether the job finished, failed, or was cancelled."""
