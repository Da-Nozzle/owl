from collections.abc import Mapping
from dataclasses import dataclass, field


# TODO bad name for file and class name
@dataclass(frozen=True, slots=True)
class InMemorySecretStore:
    """A SecretStore for tests that records which names were requested."""

    secrets: Mapping[str, str]
    requested_names: list[str] = field(default_factory=list)

    def fetch(self, name: str, args: Mapping[str, str]) -> str:
        self.requested_names.append(name)
        return self.secrets[name]
