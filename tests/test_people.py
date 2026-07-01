import pytest

from bollhav.model.people import Contact, OwnerError, People


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


# ── People ────────────────────────────────────────────────────────────


class TestPeople:
    def test_all_fields_none_is_valid(self):
        o = People()
        assert o.owners is None
        assert o.creator is None
        assert o.maintainers is None

    def test_stores_all_fields(self):
        o = People(
            owners=(Contact(name="Axel", email="axel@example.com"),),
            creator="axel",
            maintainers=(Contact(name="platform"),),
        )
        assert o.owners[0].name == "Axel"
        assert o.creator == "axel"
        assert o.maintainers[0].name == "platform"

    def test_partial_fields_valid(self):
        assert People(creator="axel").creator == "axel"
        assert (
            People(maintainers=(Contact(name="analytics"),)).maintainers[0].name
            == "analytics"
        )

    def test_rejects_empty_creator(self):
        with pytest.raises(OwnerError, match="creator"):
            People(creator="")

    def test_rejects_whitespace_creator(self):
        with pytest.raises(OwnerError, match="creator"):
            People(creator="   ")

    def test_accepts_multiple_owners(self):
        o = People(owners=(Contact(name="Axel"), Contact(name="Sam")))
        assert [c.name for c in o.owners] == ["Axel", "Sam"]

    def test_rejects_empty_owners_tuple(self):
        with pytest.raises(OwnerError, match="owners"):
            People(owners=())

    def test_rejects_empty_maintainers_tuple(self):
        with pytest.raises(OwnerError, match="maintainers"):
            People(maintainers=())

    def test_rejects_non_contact_in_owners(self):
        with pytest.raises(OwnerError, match="owners"):
            People(owners=("Axel",))

    def test_rejects_non_contact_in_maintainers(self):
        with pytest.raises(OwnerError, match="maintainers"):
            People(maintainers=("platform",))
