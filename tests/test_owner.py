import pytest

from bollhav.model.owner import Contact, OwnerError, Ownership


# ── Contact ───────────────────────────────────────────────────────────


class TestContact:
    def test_stores_name_and_email(self):
        c = Contact(name="Axel", email="axel@example.com")
        assert c.name == "Axel"
        assert c.email == "axel@example.com"

    def test_email_is_optional(self):
        c = Contact(name="platform")
        assert c.email is None

    def test_rejects_empty_name(self):
        with pytest.raises(OwnerError, match="name"):
            Contact(name="")

    def test_rejects_whitespace_only_name(self):
        with pytest.raises(OwnerError, match="name"):
            Contact(name="   ")

    def test_rejects_empty_email_string(self):
        with pytest.raises(OwnerError, match="email"):
            Contact(name="Axel", email="")

    def test_rejects_whitespace_only_email(self):
        with pytest.raises(OwnerError, match="email"):
            Contact(name="Axel", email="   ")

    def test_rejects_email_without_at(self):
        with pytest.raises(OwnerError, match="email"):
            Contact(name="Axel", email="notanemail")


# ── Ownership ─────────────────────────────────────────────────────────


class TestOwnership:
    def test_all_fields_none_is_valid(self):
        o = Ownership()
        assert o.owner is None
        assert o.creator is None
        assert o.team is None

    def test_stores_all_fields(self):
        o = Ownership(
            owner=Contact(name="Axel", email="axel@example.com"),
            creator="axel",
            team=Contact(name="platform"),
        )
        assert o.owner.name == "Axel"
        assert o.creator == "axel"
        assert o.team.name == "platform"

    def test_partial_fields_valid(self):
        assert Ownership(creator="axel").creator == "axel"
        assert Ownership(team=Contact(name="analytics")).team.name == "analytics"

    def test_rejects_empty_creator(self):
        with pytest.raises(OwnerError, match="creator"):
            Ownership(creator="")

    def test_rejects_whitespace_creator(self):
        with pytest.raises(OwnerError, match="creator"):
            Ownership(creator="   ")
