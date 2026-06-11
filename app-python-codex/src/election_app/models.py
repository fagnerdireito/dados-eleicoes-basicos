"""Objetos de dominio compartilhados entre filtros, consultas e abas."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ElectionOption:
    year: int
    election_code: str
    election_name: str
    round_number: int
    election_type: str
    election_date: str
    is_municipal: bool

    @property
    def key(self) -> str:
        return f"{self.year}|{self.election_code}|{self.round_number}"

    @property
    def label(self) -> str:
        return f"{self.year} - {self.election_name} - {self.round_number}o turno"

    @classmethod
    def from_mapping(cls, row: dict[str, Any]) -> "ElectionOption":
        return cls(
            year=int(row["year"]),
            election_code=str(row["election_code"]),
            election_name=str(row["election_name"]),
            round_number=int(row["round_number"]),
            election_type=str(row.get("election_type") or ""),
            election_date=str(row.get("election_date") or ""),
            is_municipal=bool(row.get("is_municipal")),
        )


@dataclass(frozen=True)
class ElectionScope:
    election: ElectionOption
    uf: str
    municipality_code: str | None
    municipality_name: str | None
    office_code: str
    office_name: str
    candidate_number: str
    candidate_name: str
    candidate_party: str

    @property
    def has_municipality(self) -> bool:
        return bool(self.municipality_code)

    @property
    def sql_params(self) -> dict[str, Any]:
        return {
            "year": str(self.election.year),
            "election_code": self.election.election_code,
            "round_number": str(self.election.round_number),
            "uf": self.uf,
            "municipality_code": self.municipality_code,
            "office_code": self.office_code,
            "candidate_number": self.candidate_number,
        }
