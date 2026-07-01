from __future__ import annotations
from dataclasses import dataclass


class OwnerError(ValueError):
    """Raised when a ``Contact`` or ``Ownership`` is constructed with
    invalid values (empty strings, malformed email, etc.)."""


@dataclass
class Contact:
    """A named entity with an optional contact email — used to represent
    either an individual owner or a maintainer. When ``email`` is set it
    must contain ``@``; fuller RFC 5322 parsing is intentionally out of
    scope."""

    name: str
    email: str | None = None

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise OwnerError("Contact.name must be a non-empty string")
        if self.email is not None:
            if not self.email.strip():
                raise OwnerError("Contact.email must be a non-empty string when set")
            if "@" not in self.email:
                raise OwnerError(
                    f"Contact.email must look like an email address (got {self.email!r})"
                )


@dataclass
class Ownership:
    """Ownership and accountability metadata for a model.

    All fields are optional — set whichever are relevant. ``owners`` and
    ``maintainers`` are each one or more ``Contact`` instances, stored as a
    tuple. ``creator`` is a free-form string identifying who created the
    model (username, service account, etc.)."""

    owners: tuple[Contact, ...] | None = None
    creator: str | None = None
    maintainers: tuple[Contact, ...] | None = None

    def __post_init__(self) -> None:
        if self.creator is not None and not self.creator.strip():
            raise OwnerError(
                "Ownership.creator must be a non-empty string when set"
            )
        self.owners = self._validate_contacts("owners", self.owners)
        self.maintainers = self._validate_contacts("maintainers", self.maintainers)

    @staticmethod
    def _validate_contacts(
        field_name: str, value: tuple[Contact, ...] | None
    ) -> tuple[Contact, ...] | None:
        if value is None:
            return None
        contacts = tuple(value)
        if not contacts:
            raise OwnerError(
                f"Ownership.{field_name} must contain at least one Contact when set"
            )
        for c in contacts:
            if not isinstance(c, Contact):
                raise OwnerError(
                    f"Ownership.{field_name} must contain only Contact instances "
                    f"(got {c!r})"
                )
        return contacts
