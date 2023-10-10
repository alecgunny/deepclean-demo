from typing import Iterable, Optional

from gwpy.segments import DataQualityDict, DataQualityFlag, SegmentList

OPEN_DATA_FLAGS = ["H1_DATA", "L1_DATA", "V1_DATA"]


def split_segments(segments: SegmentList, chunk_size: float) -> SegmentList:
    """
    Split a list of segments into segments that are at most
    `chunk_size` seconds long
    """
    out_segments = SegmentList()
    for segment in segments:
        start, stop = segment
        duration = stop - start
        if duration > chunk_size:
            num_segments = int((duration - 1) // chunk_size) + 1
            for i in range(num_segments):
                end = min(start + (i + 1) * chunk_size, stop)
                seg = (start + i * chunk_size, end)
                out_segments.append(seg)
        else:
            out_segments.append(segment)
    return out_segments


class DataQualityDict(DataQualityDict):
    @classmethod
    def query_non_open(
        cls, flags: Iterable[str], start: float, end: float, **kwargs
    ) -> DataQualityDict:
        try:
            return cls.query_dqsegdb(flags, start, end, **kwargs)
        except OSError as e:
            if not str(e).startswith(
                "Could not find the TLS certificate file"
            ):
                # TODO: what's the error for an expired certificate?
                raise

            # try to authenticate then re-query
            # authenticate()
            return cls.query_dqsegdb(flags, start, end, **kwargs)

    @classmethod
    def query_open(
        cls, flags: Iterable[str], start: float, end: float, **kwargs
    ) -> DataQualityDict:
        dqdict = cls()
        for flag in flags:
            dqdict[flag] = DataQualityFlag.fetch_open_data(
                flag, start, end, **kwargs
            )
        return dqdict

    @classmethod
    def query_segments(
        cls,
        flags: Iterable[str],
        start: float,
        end: float,
        min_duration: Optional[float] = None,
        chunk_size: Optional[float] = None,
        **kwargs
    ) -> SegmentList:
        flags = set(flags)
        open_flags = set(OPEN_DATA_FLAGS)

        open_data_flags = list(flags & open_flags)
        flags = list(flags - open_flags)

        segments = cls()
        if flags:
            segments.update(cls.query_non_open(flags, start, end, **kwargs))
        if open_data_flags:
            segments.update(cls.query_open(flags, start, end, **kwargs))

        segments = segments.intersection().active
        if min_duration is not None:
            segments = filter(lambda i: i[1] - i[0] >= min_duration, segments)
            segments = SegmentList(segments)

        if chunk_size is not None:
            segments = split_segments(segments, chunk_size)
        return segments
