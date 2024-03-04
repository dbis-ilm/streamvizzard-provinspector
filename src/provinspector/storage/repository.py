from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Type, TypeVar

R = TypeVar("R")


@dataclass
class InMemoryRepository:
    repo = defaultdict(list)

    def add(self, resource: R) -> None:  # type: ignore
        self.repo[type(resource)].append(resource)

    def get(self, resource_type: Type[R], **filters: Any) -> R | None:
        return next(
            (
                r
                for r in self.repo.get(resource_type, [])
                if all(getattr(r, key) == val for key, val in filters.items())
            ),
            None,
        )

    def list_all(self, resource_type: Type[R], **filters: Any) -> list[R]:
        return [
            r
            for r in self.repo.get(resource_type, [])
            if all(getattr(r, key) == val for key, val in filters.items())
        ]

    def clear(self) -> None:
        self.repo.clear()
