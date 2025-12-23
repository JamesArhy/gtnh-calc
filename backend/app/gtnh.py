from dataclasses import dataclass

TICKS_PER_SECOND = 20.0


@dataclass(frozen=True)
class MachineTuning:
    overclock_tiers: int = 0
    parallel: int = 1


def apply_overclock(duration_ticks: int, eut: int, tuning: MachineTuning) -> tuple[int, int]:
    # GregTech-style: each tier halves duration and quadruples EU/t.
    duration = max(1, duration_ticks)
    eu = max(0, eut)
    for _ in range(max(0, tuning.overclock_tiers)):
        duration = max(1, duration // 2)
        eu = eu * 4
    return duration, eu


def rate_per_second(count: int, duration_ticks: int) -> float:
    duration = max(1, duration_ticks)
    return (count / duration) * TICKS_PER_SECOND
