from typing import Iterable, List, Tuple, Union

from scipy import signal

FREQUENCY = Union[float, Iterable[float]]


def normalize_frequencies(
    freq_low: FREQUENCY, freq_high: FREQUENCY
) -> Tuple[List[float], List[float]]:
    """Standardize frequency bands to sorted lists

    For a frequency band `(freq_low, freq_high)` or
    a list of such bands `(freq_low[i], freq_high[i])`,
    ensure that the number of frequencies in each argument
    is the same and that the bands denoted by them are
    non-overlapping.

    Args:
        freq_low:
            Either a single frequency representing the
            low end of a band, or a list of frequencies
            specifying the low ends of several bands.
        freq_high:
            Either a single frequency representing the
            high end of a band, or a list of frequencies
            specifying the high ends of several bands.
    Returns:
        List of low frequencies in sorted order
    """
    try:
        num_bands = len(freq_low)
    except TypeError:
        # freq_low has no `__length__` property, so
        # assume it's a single number
        try:
            # check to make sure freq_high is also
            # a single number
            num_bands = len(freq_high)
        except TypeError:
            # it is, so wrap everything in a list to keep
            # things general
            freq_low = [freq_low]
            freq_high = [freq_high]
            num_bands = 1
        else:
            # it's not, this is a problem
            raise ValueError(
                "If specifying multiple frequencies for "
                "freq_high, must do the same for freq_low"
            )
    else:
        # freq_low _does_ have a `__length__` property,
        # so make sure that freq_high has length at all
        # and that this length is the same
        try:
            if num_bands != len(freq_high):
                raise ValueError(
                    "Number of bands specified to freq_low {} "
                    "doesn't match number of bands specified "
                    "to freq_high {}".format(num_bands, len(freq_high))
                )
        except TypeError:
            raise ValueError(
                "If specifying multiple frequencies for "
                "freq_low, must do the same for freq_high"
            )

    # sort the bands so we can make sure that
    # they don't overlap at all
    freq_low, indices = zip(*sorted(zip(freq_low, range(num_bands))))
    freq_low = list(freq_low)
    freq_high = [freq_high[i] for i in indices]
    for i in range(num_bands):
        # first make sure that the low freq is
        # lower than the high freq
        if freq_low[i] >= freq_high[i]:
            raise ValueError(
                "Low frequency {} not less than high frequency {}".format(
                    freq_low[i], freq_high
                )
            )

        # iterate through all the higher bands and
        # make sure that the current high freq is
        # not higher than the higher low freqs
        if i < (num_bands - 1):
            for j in range(i + 1, num_bands):
                if freq_high[i] > freq_low[j]:
                    raise ValueError(
                        "Top of frequency band {} greater than "
                        "bottom of higher frequency band {}".format(
                            freq_high[i], freq_low[j]
                        )
                    )
    return freq_low, freq_high


def get_filt_coeffs(freq_low, freq_high, sample_rate, output="ba", order=8):
    coeffs = []
    for Wn in zip(freq_low, freq_high):
        coeff = signal.butter(
            order, Wn, btype="bandpass", fs=sample_rate, output=output
        )
        coeffs.append(coeff)
    return coeffs


class BandpassFilter:
    def __init__(
        self,
        freq_low: list[float],
        freq_high: list[float],
        sample_rate: float,
        order: int = 8,
    ) -> None:
        super().__init__()
        self.coeffs = get_filt_coeffs(
            freq_low,
            freq_high,
            sample_rate=sample_rate,
            order=order,
            output="sos",
        )

    def __call__(self, X):
        y = 0
        for coeff in self.coeffs:
            y += signal.sosfiltfilt(coeff, X, axis=-1)
        return y
