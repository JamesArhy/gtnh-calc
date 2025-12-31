from dataclasses import dataclass
from typing import Optional

TICKS_PER_SECOND = 20.0


@dataclass(frozen=True)
class MachineTuning:
    overclock_tiers: int = 0
    parallel: int = 1


@dataclass(frozen=True)
class MachineBonus:
    speed_bonus: float = 1.0
    efficiency_bonus: float = 1.0
    parallel_bonus: float = 1.0
    max_parallel: Optional[float] = None


def apply_overclock(duration_ticks: int, eut: int, tuning: MachineTuning) -> tuple[int, int]:
    # GregTech-style: each tier halves duration and quadruples EU/t.
    duration = max(1, duration_ticks)
    eu = max(0, eut)
    for _ in range(max(0, tuning.overclock_tiers)):
        duration = max(1, duration // 2)
        eu = eu * 4
    return duration, eu


def apply_machine_bonuses(duration_ticks: int, eut: int, bonus: Optional[MachineBonus]) -> tuple[int, int]:
    if bonus is None:
        return max(1, duration_ticks), max(0, eut)
    duration = max(1, duration_ticks)
    eu = max(0, eut)
    speed_bonus = bonus.speed_bonus if bonus.speed_bonus and bonus.speed_bonus > 0 else 1.0
    if speed_bonus != 1.0:
        duration = max(1, int(duration / speed_bonus))
    efficiency_bonus = (
        bonus.efficiency_bonus if bonus.efficiency_bonus and bonus.efficiency_bonus > 0 else 1.0
    )
    if efficiency_bonus != 1.0:
        eu = int(round(eu * efficiency_bonus))
    return duration, eu


def apply_tuning(
    duration_ticks: int, eut: int, tuning: MachineTuning, bonus: Optional[MachineBonus]
) -> tuple[int, int]:
    duration, eu = apply_overclock(duration_ticks, eut, tuning)
    return apply_machine_bonuses(duration, eu, bonus)


def effective_parallel(tuning: MachineTuning, bonus: Optional[MachineBonus]) -> float:
    parallel = float(max(1, tuning.parallel))
    if bonus is None:
        return parallel
    parallel *= bonus.parallel_bonus if bonus.parallel_bonus and bonus.parallel_bonus > 0 else 1.0
    if bonus.max_parallel and bonus.max_parallel > 0:
        parallel = min(parallel, bonus.max_parallel)
    return max(1.0, parallel)


def rate_per_second(count: float, duration_ticks: int) -> float:
    duration = max(1, duration_ticks)
    return (float(count) / duration) * TICKS_PER_SECOND
