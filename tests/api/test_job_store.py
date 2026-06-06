from api import job_store
from api.schemas import JobStatus


def setup_function() -> None:
    job_store._jobs.clear()


def test_sync_jobs_do_not_overlap() -> None:
    first = job_store.create_sync_job_if_none_running()

    assert first is not None
    assert job_store.create_sync_job_if_none_running() is None
    assert job_store.create_sync_and_process_job_if_none_running() is None


def test_sync_and_process_blocks_process_and_sync() -> None:
    job = job_store.create_sync_and_process_job_if_none_running()

    assert job is not None
    assert job_store.create_sync_job_if_none_running() is None
    assert job_store.create_process_like_job_if_none_running("process") is None

    job_store.update_job(job.job_id, status=JobStatus.done)

    assert job_store.create_sync_job_if_none_running() is not None
