from collections.abc import Callable, Iterable, Iterator, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait


def in_parallel[T, R](function: Callable[[T], R], items: Sequence[T], workers: int) -> Iterator[R]:
    """`function` of each item, `workers` at once, in the items' order. Once one fails, or the
    caller stops reading, those not yet started are not."""
    with ThreadPoolExecutor(workers) as pool:
        futures = [pool.submit(function, item) for item in items]
        try:
            for future in futures:
                yield future.result()
        finally:
            for future in futures:
                future.cancel()


def expand_in_parallel[T, R](
    function: Callable[[T], tuple[R, Iterable[T]]], roots: Iterable[T], workers: int
) -> list[R]:
    """`function` of each root and of every further item a call returns, `workers` at once, each
    item started as soon as the call returning it ends. Results are in the order calls ended."""
    results = []
    with ThreadPoolExecutor(workers) as pool:
        pending: set[Future[tuple[R, Iterable[T]]]] = {
            pool.submit(function, root) for root in roots
        }
        try:
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    result, further = future.result()
                    results.append(result)
                    pending |= {pool.submit(function, item) for item in further}
        except BaseException:
            pool.shutdown(wait=False, cancel_futures=True)
            raise
    return results
