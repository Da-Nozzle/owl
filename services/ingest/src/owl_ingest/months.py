import re
from dataclasses import dataclass
from typing import Self

MONTH_KEY = re.compile(r"^(\d{4})-(\d{2})$")
MONTHS_PER_YEAR = 12


@dataclass(frozen=True, slots=True, order=True)
class Month:
    year: int
    month: int

    def __post_init__(self) -> None:
        if not 1 <= self.month <= MONTHS_PER_YEAR:
            raise ValueError(f"Month {self.month} is not between 1 and 12")

    @classmethod
    def parse(cls, key: str) -> Self:
        match = MONTH_KEY.match(key)
        if match is None:
            raise ValueError(f"{key!r} is not a YYYY-MM month")
        return cls(int(match.group(1)), int(match.group(2)))

    @property
    def key(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    def minus(self, months: int) -> Self:
        index = self.year * MONTHS_PER_YEAR + self.month - 1 - months
        return type(self)(index // MONTHS_PER_YEAR, index % MONTHS_PER_YEAR + 1)

    def plus(self, months: int) -> Self:
        return self.minus(-months)
