import importlib
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_real_db_models():
    sys.modules.pop("db.models", None)
    real_models = importlib.import_module("db.models")
    if "db" in sys.modules:
        sys.modules["db"].models = real_models
    return real_models


def _load_module():
    _load_real_db_models()
    sys.modules.pop("db.backfill_player_salary", None)
    return importlib.import_module("db.backfill_player_salary")


class TestBackfillPlayerSalary(unittest.TestCase):
    def setUp(self):
        self.models = _load_real_db_models()
        self.module = _load_module()

        self.engine = create_engine("sqlite:///:memory:")
        self.models.Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

        with self.SessionLocal() as session:
            session.add(
                self.models.Player(
                    player_id="201939",
                    full_name="Stephen Curry",
                    is_active=True,
                    is_team=False,
                )
            )
            session.commit()

    def tearDown(self):
        self.engine.dispose()

    def test_run_is_idempotent_when_repeated(self):
        contract_players = [
            self.module.ContractPlayerEntry(
                full_name="Stephen Curry",
                player_url="https://www.basketball-reference.com/players/c/curryst01.html",
            )
        ]
        salary_rows = [self.module.SalaryRecord(season=2024, salary_usd=55761216)]

        with patch.object(self.module, "Session", self.SessionLocal), patch.object(
            self.module, "fetch_contract_players", return_value=contract_players
        ), patch.object(
            self.module, "fetch_salary_history", return_value=salary_rows
        ), patch.object(
            self.module, "BasketballReferenceClient"
        ), patch(
            "builtins.print"
        ):
            self.module.run(season=2024)
            self.module.run(season=2024)

        with self.SessionLocal() as session:
            salary_rows = (
                session.query(self.models.PlayerSalary)
                .filter(self.models.PlayerSalary.player_id == "201939")
                .all()
            )

        self.assertEqual(len(salary_rows), 1)
        self.assertEqual(salary_rows[0].season, 2024)
        self.assertEqual(salary_rows[0].salary_usd, 55761216)

    def test_run_matches_case_insensitively_after_exact_lookup_miss(self):
        contract_players = [
            self.module.ContractPlayerEntry(
                full_name="stephen curry",
                player_url="https://www.basketball-reference.com/players/c/curryst01.html",
            )
        ]
        salary_rows = [self.module.SalaryRecord(season=2024, salary_usd=55761216)]

        with patch.object(self.module, "Session", self.SessionLocal), patch.object(
            self.module, "fetch_contract_players", return_value=contract_players
        ), patch.object(
            self.module, "fetch_salary_history", return_value=salary_rows
        ), patch.object(
            self.module, "BasketballReferenceClient"
        ), patch(
            "builtins.print"
        ):
            counts = self.module.run(season=2024)

        self.assertEqual(counts.matched, 1)
        self.assertEqual(counts.unmatched, 0)

    def test_run_upserts_all_seasons_when_requested(self):
        contract_players = [
            self.module.ContractPlayerEntry(
                full_name="Stephen Curry",
                player_url="https://www.basketball-reference.com/players/c/curryst01.html",
            )
        ]
        salary_rows = [
            self.module.SalaryRecord(season=2024, salary_usd=55761216),
            self.module.SalaryRecord(season=2023, salary_usd=51915615),
            self.module.SalaryRecord(season=2022, salary_usd=48070014),
        ]

        with patch.object(self.module, "Session", self.SessionLocal), patch.object(
            self.module, "fetch_contract_players", return_value=contract_players
        ), patch.object(
            self.module, "fetch_salary_history", return_value=salary_rows
        ), patch.object(
            self.module, "BasketballReferenceClient"
        ), patch(
            "builtins.print"
        ):
            counts = self.module.run(season=None)

        self.assertEqual(counts.matched, 1)
        self.assertEqual(counts.updated, 3)

        with self.SessionLocal() as session:
            persisted_rows = (
                session.query(self.models.PlayerSalary)
                .filter(self.models.PlayerSalary.player_id == "201939")
                .order_by(self.models.PlayerSalary.season.desc())
                .all()
            )

        self.assertEqual([row.season for row in persisted_rows], [2024, 2023, 2022])
        self.assertEqual([row.salary_usd for row in persisted_rows], [55761216, 51915615, 48070014])


if __name__ == "__main__":
    unittest.main()
