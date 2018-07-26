import time
import logging


def add_scheduled_job(jobs, interval, func, *args, **kwargs):
    jobs = jobs.copy()

    # Schedule the new job to run right away
    next_scheduled_time = time.monotonic()
    jobs.append((next_scheduled_time, interval, func, args, kwargs))

    # No real need to sort the jobs here, but nice to
    # if we're going to inspect the jobs list later.
    return sorted(jobs)


def run_scheduled_jobs(jobs):
    if not jobs:
        return jobs

    current_time = time.monotonic()

    # Sort jobs to run in scheduled time order to run them as
    # close to their scheduled time as possible.
    to_run = sorted(job for job in jobs if job[0] <= current_time)

    if not to_run:
        return jobs

    # We'll sort the remaining jobs later, so don't bother now.
    jobs = [job for job in jobs if job[0] > current_time]

    for scheduled_time, interval, func, args, kwargs in to_run:
        try:
            func(*args, **kwargs)
        except Exception:
            logging.exception('Error while running scheduled job; function: %(func_name)s, args: %(args)r, kwargs: %(kwargs)r',
                              {'func_name': func.__name__, 'args': args, 'kwargs': kwargs})

        # Make sure next scheduled time is past the current time,
        # in case we skipped some runs for some reason.
        next_scheduled_time = scheduled_time + interval
        while next_scheduled_time < current_time:
            next_scheduled_time += interval

        jobs.append((next_scheduled_time, interval, func, args, kwargs))

    # No real need to sort the jobs here, but nice to
    # if we're going to inspect the jobs list later.
    return sorted(jobs)
