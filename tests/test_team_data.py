"""Integrity checks for ``_data/team.yml``.

The roster in that file is the single source of truth for the team page, and
the names, roles, emails and years were supplied by the department head, so
they must not be normalised, "corrected" or inferred here. These tests check the
agreed category keys and field names, the ordering rules
(head of department first, other current members by surname, alumni by
``end_year`` most recent first and then by final surname, with diacritics
folded for comparison but preserved in the stored names) and the rule that
photos may only reference files that actually exist under ``assets/img/team``.

Expected orders are derived from the data itself, never from a hard-coded
roster, so adding or moving a person only requires the file to be ordered.

Run without a Jekyll build:

    python -m unittest tests.test_team_data -v
"""
import pathlib
import unicodedata
import unittest

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
TEAM_DATA = REPO_ROOT / "_data" / "team.yml"
PHOTO_DIR = REPO_ROOT / "assets" / "img" / "team"

CATEGORY_TITLES = ["Current members", "Alumni"]
ALLOWED_FIELDS = {
    "name",
    "role",
    "email",
    "start_year",
    "end_year",
    "last_position",
    "photo",
}

HEAD = "Attila Cangi"


def load_categories():
    with TEAM_DATA.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def fold(text):
    """Return ``text`` with diacritics folded and case removed, for comparison."""
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch)).casefold()


def surname(name):
    """Sort key for the final surname component, diacritics folded for ordering."""
    tokens = name.split()
    if len(tokens) < 2:
        raise AssertionError(f"{name!r} has no surname to sort by")
    return fold(tokens[-1])


def alumni_sort_key(person):
    """Alumni order: most recent ``end_year`` first, then surname alphabetically."""
    end_year = person.get("end_year")
    if not isinstance(end_year, int) or isinstance(end_year, bool):
        raise AssertionError(f"{person.get('name')!r} needs an integer end_year to sort by")
    return (-end_year, surname(person["name"]))


class TeamDataStructureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.categories = load_categories()
        cls.current = cls.categories[0]["members"]
        cls.alumni = cls.categories[1]["members"]

    def test_categories_use_agreed_titles(self):
        self.assertEqual([c["title"] for c in self.categories], CATEGORY_TITLES)

    def test_every_member_uses_only_agreed_fields(self):
        for category in self.categories:
            for person in category["members"]:
                self.assertIsInstance(person, dict)
                self.assertLessEqual(
                    set(person), ALLOWED_FIELDS, msg=f"unexpected keys in {person}"
                )
                self.assertTrue(person.get("name"), msg=f"missing name in {person}")
                self.assertTrue(person.get("role"), msg=f"missing role in {person}")

    def test_head_of_department_comes_first(self):
        self.assertEqual(self.current[0]["name"], HEAD)
        self.assertEqual(self.current[0]["role"], "Head of Department")

    def test_current_and_alumni_categories_contain_people(self):
        self.assertTrue(self.current)
        self.assertTrue(self.alumni)

    def test_alumni_roles_years_and_last_positions(self):
        for person in self.alumni:
            self.assertTrue(person.get("role"))
            if person.get("last_position") is not None:
                self.assertIsInstance(person["last_position"], str)

    def test_active_years_are_optional_and_valid_when_supplied(self):
        for person in self.current:
            for field in ("start_year", "end_year"):
                year = person.get(field)
                if year is not None:
                    self.assertIsInstance(year, int, msg=f"{person['name']}: invalid {field}")

    def test_alumni_years_are_explicit_and_ordered(self):
        for person in self.alumni:
            start = person.get("start_year")
            end = person.get("end_year")
            self.assertIsInstance(start, int, msg=f"{person['name']} lacks start_year")
            self.assertIsInstance(end, int, msg=f"{person['name']} lacks end_year")
            self.assertLessEqual(start, end, msg=f"{person['name']} ends before start")


class TeamOrderingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        categories = load_categories()
        cls.current = categories[0]["members"]
        cls.alumni = categories[1]["members"]

    def test_current_members_sorted_by_surname_after_head(self):
        tail = [p["name"] for p in self.current[1:]]
        self.assertEqual(tail, sorted(tail, key=surname))

    def test_alumni_sorted_by_end_year_then_surname(self):
        expected = [p["name"] for p in sorted(self.alumni, key=alumni_sort_key)]
        self.assertEqual(
            [p["name"] for p in self.alumni],
            expected,
            msg="alumni must be listed by end_year descending, then by surname",
        )

    def test_alumni_end_years_are_non_increasing(self):
        years = [p["end_year"] for p in self.alumni]
        self.assertEqual(years, sorted(years, reverse=True))

    def test_names_are_unique_across_categories(self):
        names = [p["name"] for p in self.current + self.alumni]
        self.assertEqual(len(names), len(set(names)))


class AlumniSortKeyTest(unittest.TestCase):
    """Pin the ordering semantics on synthetic people, independent of the roster."""

    def test_later_end_year_comes_first_even_when_surname_would_differ(self):
        people = [
            {"name": "Anna Zeller", "end_year": 2021},
            {"name": "Bruno Adler", "end_year": 2024},
        ]
        ordered = [p["name"] for p in sorted(people, key=alumni_sort_key)]
        self.assertEqual(ordered, ["Bruno Adler", "Anna Zeller"])

    def test_same_end_year_is_broken_by_surname(self):
        people = [
            {"name": "Marta Zielinska", "end_year": 2025},
            {"name": "Peter Adler", "end_year": 2025},
        ]
        ordered = [p["name"] for p in sorted(people, key=alumni_sort_key)]
        self.assertEqual(ordered, ["Peter Adler", "Marta Zielinska"])

    def test_diacritics_fold_for_sorting(self):
        # "Ångström" sorts as "angstrom", i.e. before "berg", even though the
        # raw code point for "Å" is greater than "B".
        people = [
            {"name": "Zoë Berg", "end_year": 2024},
            {"name": "Anna Ångström", "end_year": 2024},
        ]
        ordered = [p["name"] for p in sorted(people, key=alumni_sort_key)]
        self.assertEqual(ordered, ["Anna Ångström", "Zoë Berg"])

    def test_surname_is_the_final_name_component(self):
        self.assertEqual(surname("Sneha Verma Prakash"), "prakash")
        self.assertEqual(surname("Niclas Schlünzen"), "schlunzen")

    def test_missing_end_year_is_rejected_rather_than_guessed(self):
        with self.assertRaises(AssertionError):
            alumni_sort_key({"name": "No Years", "end_year": None})


class TeamContactAndPhotoTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        categories = load_categories()
        cls.people = categories[0]["members"] + categories[1]["members"]

    def test_emails_are_unique_and_well_formed(self):
        emails = [p["email"] for p in self.people if p.get("email")]
        self.assertEqual(len(emails), len(set(emails)))
        for email in emails:
            self.assertRegex(email, r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")

    def test_photos_point_at_existing_files_inside_team_dir(self):
        for person in self.people:
            photo = person.get("photo")
            if not photo:
                continue
            relative = photo[len("/assets/") :] if photo.startswith("/assets/") else photo
            self.assertTrue(
                relative.startswith("img/team/"),
                msg=f"{person['name']} photo must live under assets/img/team",
            )
            self.assertTrue(
                (REPO_ROOT / "assets" / relative).is_file(),
                msg=f"{person['name']} photo {photo} does not exist",
            )


if __name__ == "__main__":
    unittest.main()
