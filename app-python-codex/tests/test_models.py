import unittest

from election_app.models import ElectionOption, ElectionScope


class ModelTests(unittest.TestCase):
    def test_election_option_key_and_label(self) -> None:
        election = ElectionOption(2024, "619", "Eleicoes Municipais 2024", 1, "Ordinaria", "06/10/2024", True)
        self.assertEqual(election.key, "2024|619|1")
        self.assertIn("2024", election.label)
        self.assertIn("1o turno", election.label)

    def test_scope_sql_params_preserve_election_and_round(self) -> None:
        election = ElectionOption(2022, "544", "Eleicao Geral Federal 2022", 2, "Ordinaria", "30/10/2022", False)
        scope = ElectionScope(election, "GO", "93734", "GOIANIA", "1", "Presidente", "13", "LULA", "PT")
        self.assertEqual(scope.sql_params["election_code"], "544")
        self.assertEqual(scope.sql_params["round_number"], "2")
        self.assertEqual(scope.sql_params["municipality_code"], "93734")


if __name__ == "__main__":
    unittest.main()
