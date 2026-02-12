from datetime import datetime, date
from unittest import TestCase
from unittest.mock import patch

from eulexbuild.data.cellar_sparql import _parse_value, get_all_properties, get_eurovoc_labels_for_keywords, \
    get_descriptive_celex_ids, get_procedure_celex_ids


class TestParseValue(TestCase):
    """Test the _parse_value function with various datatype inputs."""

    def test_parse_boolean_true(self):
        """Test parsing boolean value 'true'."""
        value_obj = {
            "type": "literal",
            "datatype": "http://www.w3.org/2001/XMLSchema#boolean",
            "value": "true"
        }
        self.assertTrue(_parse_value(value_obj))

    def test_parse_boolean_false(self):
        """Test parsing boolean value 'false'."""
        value_obj = {
            "type": "literal",
            "datatype": "http://www.w3.org/2001/XMLSchema#boolean",
            "value": "false"
        }
        self.assertFalse(_parse_value(value_obj))

    def test_parse_boolean_one(self):
        """Test parsing boolean value '1'."""
        value_obj = {
            "type": "literal",
            "datatype": "http://www.w3.org/2001/XMLSchema#boolean",
            "value": "1"
        }
        self.assertTrue(_parse_value(value_obj))

    def test_parse_boolean_zero(self):
        """Test parsing boolean value '0'."""
        value_obj = {
            "type": "literal",
            "datatype": "http://www.w3.org/2001/XMLSchema#boolean",
            "value": "0"
        }
        self.assertFalse(_parse_value(value_obj))

    def test_parse_integer(self):
        """Test parsing integer value."""
        value_obj = {
            "type": "literal",
            "datatype": "http://www.w3.org/2001/XMLSchema#positiveInteger",
            "value": "1689"
        }
        result = _parse_value(value_obj)
        self.assertEqual(result, 1689)
        self.assertIsInstance(result, int)

    def test_parse_decimal(self):
        """Test parsing decimal value."""
        value_obj = {
            "type": "literal",
            "datatype": "http://www.w3.org/2001/XMLSchema#decimal",
            "value": "123.45"
        }
        result = _parse_value(value_obj)
        self.assertEqual(result, 123.45)
        self.assertIsInstance(result, float)

    def test_parse_date(self):
        """Test parsing date value."""
        value_obj = {
            "type": "literal",
            "datatype": "http://www.w3.org/2001/XMLSchema#date",
            "value": "2024-08-01"
        }
        result = _parse_value(value_obj)
        self.assertEqual(result, date(2024, 8, 1))
        self.assertIsInstance(result, date)

    def test_parse_datetime(self):
        """Test parsing dateTime value."""
        value_obj = {
            "type": "literal",
            "datatype": "http://www.w3.org/2001/XMLSchema#dateTime",
            "value": "2024-07-12T02:36:57.511+02:00"
        }
        result = _parse_value(value_obj)
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.year, 2024)
        self.assertEqual(result.month, 7)
        self.assertEqual(result.day, 12)

    def test_parse_gyear(self):
        """Test parsing gYear value."""
        value_obj = {
            "type": "literal",
            "datatype": "http://www.w3.org/2001/XMLSchema#gYear",
            "value": "2024"
        }
        result = _parse_value(value_obj)
        self.assertEqual(result, 2024)
        self.assertIsInstance(result, int)

    def test_parse_uri(self):
        """Test parsing URI value (no conversion needed)."""
        value_obj = {
            "type": "uri",
            "value": "http://publications.europa.eu/ontology/cdm#legislation_secondary"
        }
        result = _parse_value(value_obj)
        self.assertEqual(result, "http://publications.europa.eu/ontology/cdm#legislation_secondary")
        self.assertIsInstance(result, str)

    def test_parse_string_literal(self):
        """Test parsing string literal without datatype."""
        value_obj = {
            "type": "literal",
            "value": "some text"
        }
        result = _parse_value(value_obj)
        self.assertEqual(result, "some text")
        self.assertIsInstance(result, str)

    def test_parse_empty_value(self):
        """Test parsing empty value object."""
        value_obj = {}
        result = _parse_value(value_obj)
        self.assertEqual(result, "")


class TestGetAllProperties(TestCase):
    """Test the get_all_properties function."""

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_all_properties_digital_europe_programme(self, mock_sparql):
        """Test get_all_properties with Digital Europe Programme regulation data."""
        mock_sparql.return_value = {
            "head": {"link": [], "vars": ["data_type", "value"]},
            "results": {
                "distinct": False,
                "ordered": True,
                "bindings": [
                    {"data_type": {"type": "literal", "value": "title"},
                     "value": {"type": "literal", "xml:lang": "en",
                               "value": "Regulation (EU) 2021/694 of the European Parliament and of the Council of 29\u00A0April 2021 establishing the Digital Europe Programme and repealing Decision (EU) 2015/2240 (Text with EEA relevance)"}},
                    {"data_type": {"type": "literal", "value": "date"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#date",
                               "value": "2021-04-29"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "21994A0103(01)"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32011R0182"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32016Q0512(01)"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "31995R2988"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "31996R2185"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "12016P/TXT"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "12016E322"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32013R0883"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32003H0361"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "52016AE1017"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32015D0444"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "21994A0207(02)"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32014L0025"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32016L1148"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "22016A1019(01)"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32014L0024"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "12016E290"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32016R0679"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "12016M005"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32013R0755"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32014R0283"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "12016E187"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32017L1371"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32018R1046"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32018L1972"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32017R1939"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32018R1488"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32019R0881"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32017C1213(01)"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "52018AE3902"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32021R0523"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32020Q1222(01)"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32020R2093"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "52019AP0403"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "52021AG0003(01)"}},
                    {"data_type": {"type": "literal", "value": "cites"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32021R0695"}},
                    {"data_type": {"type": "literal", "value": "adopts"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "52018PC0434"}},
                    {"data_type": {"type": "literal", "value": "based_on"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "12016E172"}},
                    {"data_type": {"type": "literal", "value": "based_on"},
                     "value": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "12016E173"}}
                ]
            }
        }

        result = get_all_properties("32021R0694")

        # Verify the structure
        self.assertIn("relations", result)
        self.assertIn("title", result)
        self.assertIn("date", result)

        # Verify title
        self.assertEqual(result["title"],
                         "Regulation (EU) 2021/694 of the European Parliament and of the Council of 29\u00A0April 2021 establishing the Digital Europe Programme and repealing Decision (EU) 2015/2240 (Text with EEA relevance)")

        # Verify date
        self.assertEqual(result["date"], date(2021, 4, 29))

        # Verify relations structure
        self.assertIn("cites", result["relations"])
        self.assertIn("adopts", result["relations"])
        self.assertIn("based_on", result["relations"])

        # Verify cites count (should have 36 citations)
        self.assertEqual(len(result["relations"]["cites"]), 36)

        # Verify some specific citations
        self.assertIn("21994A0103(01)", result["relations"]["cites"])
        self.assertIn("32011R0182", result["relations"]["cites"])
        self.assertIn("32021R0695", result["relations"]["cites"])

        # Verify adopts (should have 1)
        self.assertEqual(len(result["relations"]["adopts"]), 1)
        self.assertIn("52018PC0434", result["relations"]["adopts"])

        # Verify based_on (should have 2)
        self.assertEqual(len(result["relations"]["based_on"]), 2)
        self.assertIn("12016E172", result["relations"]["based_on"])
        self.assertIn("12016E173", result["relations"]["based_on"])

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_all_properties_empty_results(self, mock_sparql):
        """Test get_all_properties with empty results."""
        mock_sparql.return_value = {
            "results": {
                "bindings": []
            }
        }

        result = get_all_properties("32024R1689")

        self.assertEqual(result, {"relations": {}})


class TestGetEurovocForKeywords(TestCase):
    """Test the get_eurovoc_labels_for_keywords function."""

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_eurovoc_labels_for_keywords_single_keyword(self, mock_sparql):
        """Test get_eurovoc_labels_for_keywords with a single keyword."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {
                        "keyword": {"type": "literal", "value": "environment"},
                        "label": {"type": "literal", "value": "environment", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100142"}
                    }
                ]
            }
        }

        result = get_eurovoc_labels_for_keywords({"environment"})

        # Returns {keyword: {concept: [labels]}} mapping
        self.assertEqual({"environment": {"http://eurovoc.europa.eu/100142": {"environment"}}}, result)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_eurovoc_labels_for_keywords_multiple_keywords(self, mock_sparql):
        """Test get_eurovoc_labels_for_keywords with multiple keywords."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {
                        "keyword": {"type": "literal", "value": "environment"},
                        "label": {"type": "literal", "value": "environment", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100142"}
                    },
                    {
                        "keyword": {"type": "literal", "value": "energy"},
                        "label": {"type": "literal", "value": "energy policy", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100143"}
                    },
                    {
                        "keyword": {"type": "literal", "value": "climate"},
                        "label": {"type": "literal", "value": "climate change policy", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100144"}
                    }
                ]
            }
        }

        result = get_eurovoc_labels_for_keywords({"environment", "energy", "climate"})

        # Returns {keyword: {concept: [labels]}} mapping
        self.assertEqual(len(result), 3)
        self.assertEqual({"environment"}, result["environment"]["http://eurovoc.europa.eu/100142"])
        self.assertEqual({"energy policy"}, result["energy"]["http://eurovoc.europa.eu/100143"])
        self.assertEqual({"climate change policy"}, result["climate"]["http://eurovoc.europa.eu/100144"])

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_eurovoc_labels_for_keywords_empty_set(self, mock_sparql):
        """Test get_eurovoc_labels_for_keywords with empty keyword set."""
        mock_sparql.return_value = {
            "results": {
                "bindings": []
            }
        }

        result = get_eurovoc_labels_for_keywords(set())

        self.assertEqual(result, {})

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_eurovoc_labels_for_keywords_no_matches(self, mock_sparql):
        """Test get_eurovoc_labels_for_keywords when no Eurovoc labels match."""
        mock_sparql.return_value = {
            "results": {
                "bindings": []
            }
        }

        result = get_eurovoc_labels_for_keywords({"nonexistentkeyword123"})

        self.assertEqual(result, {})

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_eurovoc_labels_for_keywords_partial_matches(self, mock_sparql):
        """Test get_eurovoc_labels_for_keywords when only some keywords have matches."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {
                        "keyword": {"type": "literal", "value": "data"},
                        "label": {"type": "literal", "value": "data protection", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100145"}
                    }
                ]
            }
        }

        result = get_eurovoc_labels_for_keywords({"data", "nonexistent"})

        # Returns {keyword: {concept: [labels]}} mapping
        self.assertEqual(len(result), 1)
        self.assertEqual({"data protection"}, result["data"]["http://eurovoc.europa.eu/100145"])
        self.assertNotIn("nonexistent", result)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_eurovoc_labels_for_keywords_missing_bindings(self, mock_sparql):
        """Test get_eurovoc_labels_for_keywords with incomplete binding data."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {
                        "keyword": {"type": "literal", "value": "technology"},
                        "label": {"type": "literal", "value": "technology", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100146"}
                    },
                    {
                        "keyword": {"type": "literal", "value": "invalid"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100147"}
                        # Missing label
                    },
                    {
                        "label": {"type": "literal", "value": "orphan label", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100148"}
                        # Missing keyword
                    }
                ]
            }
        }

        result = get_eurovoc_labels_for_keywords({"technology", "invalid"})

        # Only the complete binding should be included - returns {keyword: {concept: [labels]}}
        self.assertEqual(len(result), 1)
        self.assertEqual({"technology"}, result["technology"]["http://eurovoc.europa.eu/100146"])
        self.assertNotIn("invalid", result)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_eurovoc_labels_for_keywords_special_characters(self, mock_sparql):
        """Test get_eurovoc_labels_for_keywords with special characters in keywords."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {
                        "keyword": {"type": "literal", "value": "AI"},
                        "label": {"type": "literal", "value": "artificial intelligence", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100149"}
                    },
                    {
                        "keyword": {"type": "literal", "value": "co-operation"},
                        "label": {"type": "literal", "value": "international co-operation", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100150"}
                    }
                ]
            }
        }

        result = get_eurovoc_labels_for_keywords({"AI", "co-operation"})

        # Returns {keyword: {concept: [labels]}} mapping
        self.assertEqual(len(result), 2)
        self.assertEqual({"artificial intelligence"}, result["AI"]["http://eurovoc.europa.eu/100149"])
        self.assertEqual({"international co-operation"}, result["co-operation"]["http://eurovoc.europa.eu/100150"])

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_eurovoc_labels_for_keywords_multiple_labels_per_keyword(self, mock_sparql):
        """Test get_eurovoc_labels_for_keywords when same keyword matches multiple labels (now all are kept)."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {
                        "keyword": {"type": "literal", "value": "law"},
                        "label": {"type": "literal", "value": "law of the sea", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100151"}
                    },
                    {
                        "keyword": {"type": "literal", "value": "law"},
                        "label": {"type": "literal", "value": "labour law", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100152"}
                    }
                ]
            }
        }

        result = get_eurovoc_labels_for_keywords({"law"})

        # Returns {keyword: {concept: [labels]}} - all concepts are kept under the same keyword
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result["law"]), 2)
        self.assertEqual({"law of the sea"}, result["law"]["http://eurovoc.europa.eu/100151"])
        self.assertEqual({"labour law"}, result["law"]["http://eurovoc.europa.eu/100152"])

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_eurovoc_labels_for_keywords_case_sensitivity(self, mock_sparql):
        """Test get_eurovoc_labels_for_keywords preserves keyword casing."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {
                        "keyword": {"type": "literal", "value": "GDPR"},
                        "label": {"type": "literal", "value": "data protection regulation", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100153"}
                    }
                ]
            }
        }

        result = get_eurovoc_labels_for_keywords({"GDPR"})

        # Returns {keyword: {concept: [labels]}} - keyword casing should be preserved
        self.assertEqual({"data protection regulation"}, result["GDPR"]["http://eurovoc.europa.eu/100153"])

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_eurovoc_labels_for_keywords_whitespace_in_keywords(self, mock_sparql):
        """Test get_eurovoc_labels_for_keywords with multi-word keywords."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {
                        "keyword": {"type": "literal", "value": "artificial intelligence"},
                        "label": {"type": "literal", "value": "artificial intelligence", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100153"}
                    },
                    {
                        "keyword": {"type": "literal", "value": "machine learning"},
                        "label": {"type": "literal", "value": "machine learning", "xml:lang": "en"},
                        "concept": {"type": "uri", "value": "http://eurovoc.europa.eu/100154"}
                    }
                ]
            }
        }

        result = get_eurovoc_labels_for_keywords({"artificial intelligence", "machine learning"})

        # Returns {keyword: {concept: [labels]}} mapping
        self.assertEqual(len(result), 2)
        self.assertEqual({"artificial intelligence"},
                         result["artificial intelligence"]["http://eurovoc.europa.eu/100153"])
        self.assertEqual({"machine learning"}, result["machine learning"]["http://eurovoc.europa.eu/100154"])


class TestGetEurovocLabelsForKeywordsEndToEnd(TestCase):
    """End-to-end tests for get_eurovoc_labels_for_keywords with real SPARQL endpoint."""

    def test_get_eurovoc_labels_for_keywords_real_data_single(self):
        """Test get_eurovoc_labels_for_keywords with real data for a single keyword."""
        result = get_eurovoc_labels_for_keywords({"environment"})

        self.assertIn("EU environmental policy", result["environment"]["http://eurovoc.europa.eu/5794"])
        self.assertIn("environmental protection", result["environment"]["http://eurovoc.europa.eu/2825"])
        self.assertIn("modernisation of the residential environment",
                      result["environment"]["http://eurovoc.europa.eu/1456"])

    def test_get_eurovoc_labels_for_keywords_real_data_multiple(self):
        """Test get_eurovoc_labels_for_keywords with real data for multiple keywords."""
        keywords = {"data", "energy", "technology"}
        result = get_eurovoc_labels_for_keywords(keywords)

        self.assertIn("data collection", result["data"]["http://eurovoc.europa.eu/6030"])
        self.assertIn("spatial data", result["data"]["http://eurovoc.europa.eu/c_7a168de0"])

        self.assertIn("EU energy label", result["energy"]["http://eurovoc.europa.eu/c_87617344"])
        self.assertIn("thermal energy", result["energy"]["http://eurovoc.europa.eu/757"])

        self.assertIn("clean technology", result["technology"]["http://eurovoc.europa.eu/3638"])
        self.assertIn("legal technology", result["technology"]["http://eurovoc.europa.eu/c_38c873e0"])

    def test_get_eurovoc_labels_for_keywords_real_data_no_match(self):
        """Test get_eurovoc_labels_for_keywords with keywords unlikely to match."""
        result = get_eurovoc_labels_for_keywords({"xyznonexistent123"})

        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 0)

    def test_get_eurovoc_labels_for_keywords_real_data_empty(self):
        """Test get_eurovoc_labels_for_keywords with empty keyword set."""
        result = get_eurovoc_labels_for_keywords(set())

        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 0)


class TestGetDescriptiveCellarIds(TestCase):
    """Test the get_descriptive_celex_ids function."""

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_no_filters(self, mock_sparql):
        """Test get_descriptive_celex_ids with no filters (all parameters default)."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}},
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024L0123"}},
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024D0456"}}
                ]
            }
        }

        result = get_descriptive_celex_ids()

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 3)
        self.assertIn("32024R1689", result)
        self.assertIn("32024L0123", result)
        self.assertIn("32024D0456", result)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_with_date_filters(self, mock_sparql):
        """Test get_descriptive_celex_ids with start and end date filters."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}}
                ]
            }
        }

        start = date(2024, 1, 1)
        end = date(2024, 12, 31)
        result = get_descriptive_celex_ids(start_date=start, end_date=end)

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 1)
        self.assertIn("32024R1689", result)

        # Verify the SPARQL query contains date filters
        called_query = mock_sparql.call_args[0][0]
        self.assertIn('FILTER(?date > "2024-01-01"^^xsd:date)', called_query)
        self.assertIn('FILTER(?date < "2024-12-31"^^xsd:date)', called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_with_eurovoc_filter(self, mock_sparql):
        """Test get_descriptive_celex_ids with EuroVoc URI filters."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}},
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1690"}}
                ]
            }
        }

        eurovoc_uris = {"http://eurovoc.europa.eu/1234", "http://eurovoc.europa.eu/5678"}
        result = get_descriptive_celex_ids(eurovoc_uris=eurovoc_uris)

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 2)

        # Verify the SPARQL query contains eurovoc filters
        called_query = mock_sparql.call_args[0][0]
        self.assertIn("cdm:work_is_about_concept_eurovoc", called_query)
        self.assertIn("VALUES ?eurovoc", called_query)
        self.assertIn("<http://eurovoc.europa.eu/1234>", called_query)
        self.assertIn("<http://eurovoc.europa.eu/5678>", called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_only_regulations(self, mock_sparql):
        """Test get_descriptive_celex_ids filtering only regulations."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}}
                ]
            }
        }

        result = get_descriptive_celex_ids(include_regulations=True,
                                           include_directives=False,
                                           include_decisions=False)

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 1)

        # Verify the SPARQL query contains correct type filter
        called_query = mock_sparql.call_args[0][0]
        self.assertIn('FILTER(?type = "R"^^xsd:string)', called_query)
        self.assertNotIn('?type = "L"^^xsd:string', called_query)
        self.assertNotIn('?type = "D"^^xsd:string', called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_only_directives(self, mock_sparql):
        """Test get_descriptive_celex_ids filtering only directives."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024L0123"}}
                ]
            }
        }

        result = get_descriptive_celex_ids(include_regulations=False,
                                           include_directives=True,
                                           include_decisions=False)

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 1)

        # Verify the SPARQL query contains correct type filter
        called_query = mock_sparql.call_args[0][0]
        self.assertIn('FILTER(?type = "L"^^xsd:string)', called_query)
        self.assertNotIn('?type = "R"^^xsd:string', called_query)
        self.assertNotIn('?type = "D"^^xsd:string', called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_only_decisions(self, mock_sparql):
        """Test get_descriptive_celex_ids filtering only decisions."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024D0456"}}
                ]
            }
        }

        result = get_descriptive_celex_ids(include_regulations=False,
                                           include_directives=False,
                                           include_decisions=True)

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 1)

        # Verify the SPARQL query contains correct type filter
        called_query = mock_sparql.call_args[0][0]
        self.assertIn('FILTER(?type = "D"^^xsd:string)', called_query)
        self.assertNotIn('?type = "R"^^xsd:string', called_query)
        self.assertNotIn('?type = "L"^^xsd:string', called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_multiple_document_types(self, mock_sparql):
        """Test get_descriptive_celex_ids with multiple document types."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}},
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024L0123"}}
                ]
            }
        }

        result = get_descriptive_celex_ids(include_regulations=True,
                                           include_directives=True,
                                           include_decisions=False)

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 2)

        # Verify the SPARQL query contains correct type filters with OR
        called_query = mock_sparql.call_args[0][0]
        self.assertIn('?type = "R"^^xsd:string', called_query)
        self.assertIn('?type = "L"^^xsd:string', called_query)
        self.assertIn('||', called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_all_filters_combined(self, mock_sparql):
        """Test get_descriptive_celex_ids with all filters combined."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}}
                ]
            }
        }

        start = date(2024, 1, 1)
        end = date(2024, 12, 31)
        eurovoc_uris = {"http://eurovoc.europa.eu/1234"}

        result = get_descriptive_celex_ids(
            start_date=start,
            end_date=end,
            eurovoc_uris=eurovoc_uris,
            include_regulations=True,
            include_directives=False,
            include_decisions=False
        )

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 1)

        # Verify the SPARQL query contains all filters
        called_query = mock_sparql.call_args[0][0]
        self.assertIn('FILTER(?date > "2024-01-01"^^xsd:date)', called_query)
        self.assertIn('FILTER(?date < "2024-12-31"^^xsd:date)', called_query)
        self.assertIn("cdm:work_is_about_concept_eurovoc", called_query)
        self.assertIn("<http://eurovoc.europa.eu/1234>", called_query)
        self.assertIn('?type = "R"^^xsd:string', called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_empty_results(self, mock_sparql):
        """Test get_descriptive_celex_ids with empty results."""
        mock_sparql.return_value = {
            "results": {
                "bindings": []
            }
        }

        result = get_descriptive_celex_ids()

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 0)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_missing_celex(self, mock_sparql):
        """Test get_descriptive_celex_ids skips bindings without celex."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}},
                    {"other": {"type": "literal", "value": "something"}},  # Missing celex
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024L0123"}}
                ]
            }
        }

        result = get_descriptive_celex_ids()

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 2)
        self.assertIn("32024R1689", result)
        self.assertIn("32024L0123", result)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_deduplication(self, mock_sparql):
        """Test get_descriptive_celex_ids properly deduplicates results."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}},
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}},  # Duplicate
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024L0123"}}
                ]
            }
        }

        result = get_descriptive_celex_ids()

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 2)
        self.assertIn("32024R1689", result)
        self.assertIn("32024L0123", result)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_no_type_filters(self, mock_sparql):
        """Test get_descriptive_celex_ids with all document types disabled."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}}
                ]
            }
        }

        result = get_descriptive_celex_ids(include_regulations=False,
                                           include_directives=False,
                                           include_decisions=False)

        self.assertIsInstance(result, set)

        # Verify the SPARQL query has no type filter
        called_query = mock_sparql.call_args[0][0]
        self.assertNotIn('FILTER(?type', called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_with_corrigenda(self, mock_sparql):
        """Test get_descriptive_celex_ids including corrigenda."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}},
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689R(01)"}}
                ]
            }
        }

        result = get_descriptive_celex_ids(include_corrigenda=True)

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 2)

        # Verify the SPARQL query does not contain corrigenda filter
        called_query = mock_sparql.call_args[0][0]
        self.assertNotIn('FILTER(!REGEX(STR(?celex), "R\\\\\\\\([0-9]{2}\\\\\\\\)$"))', called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_exclude_corrigenda(self, mock_sparql):
        """Test get_descriptive_celex_ids excluding corrigenda (default)."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}}
                ]
            }
        }

        result = get_descriptive_celex_ids(include_corrigenda=False)

        self.assertIsInstance(result, set)

        # Verify the SPARQL query contains corrigenda filter
        called_query = mock_sparql.call_args[0][0]
        self.assertIn('FILTER(!REGEX(STR(?celex), "\\\\([0-9]{2}\\\\)$"))', called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_with_consolidated_texts(self, mock_sparql):
        """Test get_descriptive_celex_ids including consolidated texts."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "02024R1689-20240801"}}
                ]
            }
        }

        result = get_descriptive_celex_ids(include_consolidated_texts=True)

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 1)

        # Verify the SPARQL query contains sector filter for consolidated texts
        called_query = mock_sparql.call_args[0][0]
        self.assertIn('?sector = "0"^^xsd:string', called_query)
        self.assertIn('?sector = "3"^^xsd:string', called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_with_national_transpositions(self, mock_sparql):
        """Test get_descriptive_celex_ids including national transpositions."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "72024L0123DEU_123456"}}
                ]
            }
        }

        result = get_descriptive_celex_ids(include_national_transpositions=True)

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 1)

        # Verify the SPARQL query contains sector filter for national transpositions
        called_query = mock_sparql.call_args[0][0]
        self.assertIn('?sector = "7"^^xsd:string', called_query)
        self.assertIn('?sector = "3"^^xsd:string', called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_all_sectors(self, mock_sparql):
        """Test get_descriptive_celex_ids with all sector types enabled."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}},
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "02024R1689-20240801"}},
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "72024L0123DEU_123456"}}
                ]
            }
        }

        result = get_descriptive_celex_ids(
            include_consolidated_texts=True,
            include_national_transpositions=True
        )

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 3)

        # Verify the SPARQL query contains sector filters for all types
        called_query = mock_sparql.call_args[0][0]
        self.assertIn('?sector = "3"^^xsd:string', called_query)
        self.assertIn('?sector = "0"^^xsd:string', called_query)
        self.assertIn('?sector = "7"^^xsd:string', called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_default_sectors(self, mock_sparql):
        """Test get_descriptive_celex_ids with default sector (3 only)."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}}
                ]
            }
        }

        result = get_descriptive_celex_ids()

        self.assertIsInstance(result, set)

        # Verify the SPARQL query contains only sector 3 filter
        called_query = mock_sparql.call_args[0][0]
        self.assertIn('?sector = "3"^^xsd:string', called_query)
        self.assertNotIn('?sector = "0"^^xsd:string', called_query)
        self.assertNotIn('?sector = "7"^^xsd:string', called_query)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_only_start_date(self, mock_sparql):
        """Test get_descriptive_celex_ids with only start date."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}}
                ]
            }
        }

        start = date(2024, 1, 1)
        result = get_descriptive_celex_ids(start_date=start)

        self.assertIsInstance(result, set)

        # Verify the SPARQL query contains start date filter but not end date
        called_query = mock_sparql.call_args[0][0]
        self.assertIn('FILTER(?date > "2024-01-01"^^xsd:date)', called_query)
        # Count occurrences - should be only one date filter
        self.assertEqual(called_query.count('FILTER(?date'), 1)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_descriptive_cellar_ids_only_end_date(self, mock_sparql):
        """Test get_descriptive_celex_ids with only end date."""
        mock_sparql.return_value = {
            "results": {
                "bindings": [
                    {"celex": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#string",
                               "value": "32024R1689"}}
                ]
            }
        }

        end = date(2024, 12, 31)
        result = get_descriptive_celex_ids(end_date=end)

        self.assertIsInstance(result, set)

        # Verify the SPARQL query contains end date filter but not start date
        called_query = mock_sparql.call_args[0][0]
        self.assertIn('FILTER(?date < "2024-12-31"^^xsd:date)', called_query)
        # Count occurrences - should be only one date filter
        self.assertEqual(called_query.count('FILTER(?date'), 1)


class TestGetDescriptiveCellarIdsEndToEnd(TestCase):
    """End-to-end tests for get_descriptive_celex_ids that make actual SPARQL requests."""

    def test_get_descriptive_cellar_ids_e2e_basic(self):
        """Test get_descriptive_celex_ids with a small date range (real request)."""
        # Use a small date range to limit results
        start = date(2024, 12, 1)
        end = date(2024, 12, 31)

        result = get_descriptive_celex_ids(start_date=start, end_date=end)

        # Verify we get a set of CELEX IDs
        self.assertIsInstance(result, set)

        # Expected: 186 CELEX IDs for December 2024 (as of capture date)
        # Type breakdown: 85 Regulations (R), 3 Directives (L), 98 Decisions (D)
        self.assertGreaterEqual(len(result), 186, "Expected at least 186 CELEX IDs for December 2024")

        # Verify specific known IDs are present
        expected_ids = {'32024D07412', '32024D07424', '32024D07463'}
        self.assertTrue(expected_ids.issubset(result), f"Expected sample IDs not found in result")

        # Verify the format of returned CELEX IDs (should be strings starting with year)
        for celex in result:
            self.assertIsInstance(celex, str, f"CELEX ID should be string, got {type(celex)}")
            self.assertRegex(celex, r'^\d{5}[RLD]\d+', f"CELEX ID format invalid: {celex}")

        # Verify we have the expected distribution of document types
        regs = sum(1 for c in result if 'R' in c and not c.startswith('0') and not c.startswith('7'))
        dirs = sum(1 for c in result if 'L' in c and not c.startswith('0') and not c.startswith('7'))
        decs = sum(1 for c in result if 'D' in c and not c.startswith('0') and not c.startswith('7'))
        self.assertGreaterEqual(regs, 85, "Expected at least 85 regulations")
        self.assertGreaterEqual(dirs, 3, "Expected at least 3 directives")
        self.assertGreaterEqual(decs, 98, "Expected at least 98 decisions")

    def test_get_descriptive_cellar_ids_e2e_regulations_only(self):
        """Test get_descriptive_celex_ids filtering only regulations (real request)."""
        start = date(2024, 11, 1)
        end = date(2024, 11, 30)

        result = get_descriptive_celex_ids(
            start_date=start,
            end_date=end,
            include_regulations=True,
            include_directives=False,
            include_decisions=False
        )

        self.assertIsInstance(result, set)

        # Expected: 80 regulations for November 2024
        self.assertGreaterEqual(len(result), 80, "Expected at least 80 regulations for November 2024")

        # Verify specific known regulation IDs are present
        expected_regs = {'32024R2835', '32024R2848', '32024R2850'}
        self.assertTrue(expected_regs.issubset(result), f"Expected regulation IDs not found in result")

        # Verify all returned IDs are regulations (contain 'R' as type)
        for celex in result:
            self.assertIn('R', celex, f"Expected regulation, got: {celex}")
            self.assertNotIn('L', celex, f"Should not contain directives: {celex}")
            self.assertNotIn('D', celex, f"Should not contain decisions: {celex}")
            # Ensure not consolidated or national transposition
            self.assertFalse(celex.startswith('0'), f"Should not be consolidated text: {celex}")
            self.assertFalse(celex.startswith('7'), f"Should not be national transposition: {celex}")

    def test_get_descriptive_cellar_ids_e2e_directives_only(self):
        """Test get_descriptive_celex_ids filtering only directives (real request)."""
        start = date(2024, 10, 1)
        end = date(2024, 10, 31)

        result = get_descriptive_celex_ids(
            start_date=start,
            end_date=end,
            include_regulations=False,
            include_directives=True,
            include_decisions=False
        )

        self.assertIsInstance(result, set)

        # Expected: 12 directives for October 2024
        self.assertGreaterEqual(len(result), 12, "Expected at least 12 directives for October 2024")

        # Verify specific known directive IDs are present
        expected_dirs = {'32024L2749', '32024L2808', '32024L2810'}
        self.assertTrue(expected_dirs.issubset(result), f"Expected directive IDs not found in result")

        # Verify all returned IDs are directives (contain 'L' as type)
        for celex in result:
            self.assertIn('L', celex, f"Expected directive, got: {celex}")
            self.assertNotIn('R', celex, f"Should not contain regulations: {celex}")
            self.assertNotIn('D', celex, f"Should not contain decisions: {celex}")
            # Ensure not consolidated or national transposition
            self.assertFalse(celex.startswith('0'), f"Should not be consolidated text: {celex}")
            self.assertFalse(celex.startswith('7'), f"Should not be national transposition: {celex}")

    def test_get_descriptive_cellar_ids_e2e_decisions_only(self):
        """Test get_descriptive_celex_ids filtering only decisions (real request)."""
        start = date(2024, 9, 1)
        end = date(2024, 9, 30)

        result = get_descriptive_celex_ids(
            start_date=start,
            end_date=end,
            include_regulations=False,
            include_directives=False,
            include_decisions=True
        )

        self.assertIsInstance(result, set)

        # Expected: 84 decisions for September 2024
        self.assertGreaterEqual(len(result), 84, "Expected at least 84 decisions for September 2024")

        # Verify specific known decision IDs are present
        expected_decs = {'32024D02490', '32024D04687', '32024D05523'}
        self.assertTrue(expected_decs.issubset(result), f"Expected decision IDs not found in result")

        # Verify all returned IDs are decisions (contain 'D' as type)
        for celex in result:
            self.assertIn('D', celex, f"Expected decision, got: {celex}")
            # Ensure not consolidated or national transposition
            self.assertFalse(celex.startswith('0'), f"Should not be consolidated text: {celex}")
            self.assertFalse(celex.startswith('7'), f"Should not be national transposition: {celex}")

    def test_get_descriptive_cellar_ids_e2e_mixed_types(self):
        """Test get_descriptive_celex_ids with multiple document types (real request)."""
        start = date(2024, 8, 1)
        end = date(2024, 8, 31)

        result = get_descriptive_celex_ids(
            start_date=start,
            end_date=end,
            include_regulations=True,
            include_directives=True,
            include_decisions=False
        )

        self.assertIsInstance(result, set)

        # Expected: 18 documents for August 2024 (all regulations in this case)
        self.assertGreaterEqual(len(result), 18, "Expected at least 18 documents for August 2024")

        # Verify specific known regulation IDs are present
        expected_ids = {'32024R2137', '32024R2140', '32024R2146'}
        self.assertTrue(expected_ids.issubset(result), f"Expected IDs not found in result")

        # Check if we have regulations (expected to have only regulations based on actual results)
        has_regulation = any(
            'R' in celex and not celex.startswith('0') and not celex.startswith('7') for celex in result)
        has_directive = any(
            'L' in celex and not celex.startswith('0') and not celex.startswith('7') for celex in result)
        has_decision = any('D' in celex and not celex.startswith('0') and not celex.startswith('7') for celex in result)

        # We should have regulations but no directives (based on actual data)
        self.assertTrue(has_regulation, "Should have regulations")
        self.assertFalse(has_directive, "No directives in this date range")
        # We should not have decisions as they are filtered out
        self.assertFalse(has_decision, "Should not have decisions when they are filtered out")

    def test_get_descriptive_cellar_ids_e2e_with_eurovoc(self):
        """Test get_descriptive_celex_ids with EuroVoc URI filter (real request)."""
        start = date(2024, 1, 1)
        end = date(2024, 12, 31)

        # Use a common EuroVoc concept (e.g., "international security")
        eurovoc_uris = {"http://eurovoc.europa.eu/100172"}

        result = get_descriptive_celex_ids(
            start_date=start,
            end_date=end,
            eurovoc_uris=eurovoc_uris
        )

        self.assertIsInstance(result, set)

        # Expected: 0 results for this specific EuroVoc URI in 2024
        # This EuroVoc concept may not match any documents in the date range
        self.assertEqual(len(result), 0, "Expected 0 results for EuroVoc URI 100172 in 2024")

        # Test passes successfully even with empty results - validates the filter works

    def test_get_descriptive_cellar_ids_e2e_no_filters(self):
        """Test get_descriptive_celex_ids without any filters returns many results (real request)."""
        # This test uses a future date range (Dec 2025) which will have results
        start = date(2025, 12, 1)
        end = date(2025, 12, 31)

        result = get_descriptive_celex_ids(start_date=start, end_date=end)

        self.assertIsInstance(result, set)

        # Expected: 187 CELEX IDs for December 2025
        # Note: This test was run in Jan 2026, so Dec 2025 is now in the past
        self.assertLessEqual(188, len(result), "Expected at least 188 CELEX IDs for December 2025")

        # Verify specific known IDs are present
        expected_ids = {'32025D06583', '32025D06584', '32025D06711'}
        self.assertTrue(expected_ids.issubset(result), f"Expected sample IDs not found in result")

        # Verify mixed document types (91 Regulations, 2 Directives, 94 Decisions)
        regs = sum(1 for c in result if 'R' in c and not c.startswith('0') and not c.startswith('7'))
        dirs = sum(1 for c in result if 'L' in c and not c.startswith('0') and not c.startswith('7'))
        decs = sum(1 for c in result if 'D' in c and not c.startswith('0') and not c.startswith('7'))
        self.assertGreaterEqual(regs, 91, "Expected at least 91 regulations")
        self.assertGreaterEqual(dirs, 2, "Expected at least 2 directives")
        self.assertGreaterEqual(decs, 95, "Expected at least 95 decisions")

    def test_get_descriptive_cellar_ids_e2e_exclude_corrigenda(self):
        """Test get_descriptive_celex_ids excludes corrigenda by default (real request)."""
        start = date(2024, 6, 1)
        end = date(2024, 6, 30)

        result = get_descriptive_celex_ids(
            start_date=start,
            end_date=end,
            include_corrigenda=False
        )

        self.assertIsInstance(result, set)

        # Expected: 189 CELEX IDs for June 2024 (without corrigenda)
        self.assertGreaterEqual(len(result), 189, "Expected at least 189 CELEX IDs for June 2024 excluding corrigenda")

        # Verify specific known IDs are present
        expected_ids = {'32024D03685', '32024D03866', '32024D03936'}
        self.assertTrue(expected_ids.issubset(result), f"Expected sample IDs not found in result")

        # Verify no corrigenda are included (format: CELEX_ID + R(01), R(02), etc.)
        for celex in result:
            self.assertNotRegex(celex, r'R\(\d{2}\)$', f"Should not include corrigenda: {celex}")

        # Verify document type breakdown (103 Regulations, 5 Directives, 81 Decisions)
        regs = sum(1 for c in result if 'R' in c and not c.startswith('0') and not c.startswith('7') and 'R(' not in c)
        dirs = sum(1 for c in result if 'L' in c and not c.startswith('0') and not c.startswith('7'))
        decs = sum(1 for c in result if 'D' in c and not c.startswith('0') and not c.startswith('7'))
        self.assertGreaterEqual(regs, 103, "Expected at least 103 regulations")
        self.assertGreaterEqual(dirs, 5, "Expected at least5 directives")
        self.assertGreaterEqual(decs, 81, "Expected at least 81 decisions")

    def test_get_descriptive_cellar_ids_e2e_include_corrigenda(self):
        """Test get_descriptive_celex_ids includes corrigenda when requested (real request)."""
        start = date(2024, 1, 1)
        end = date(2024, 12, 31)

        result = get_descriptive_celex_ids(
            start_date=start,
            end_date=end,
            include_corrigenda=True
        )

        self.assertIsInstance(result, set)

        # Expected: 2987 CELEX IDs for 2024 (including 791 corrigenda)
        self.assertGreaterEqual(len(result), 2987, "Expected at least 2987 CELEX IDs for 2024 including corrigenda")

        # Verify specific known corrigenda IDs are present
        expected_corrigenda = {'31992L0043R(07)', '31993L0013R(10)', '31993L0013R(11)'}
        self.assertTrue(expected_corrigenda.issubset(result), f"Expected corrigenda IDs not found in result")

        # Verify corrigenda are actually present (should have pattern R(##))
        corrigenda_count = sum(1 for celex in result if 'R(' in celex)
        self.assertGreaterEqual(corrigenda_count, 791, f"Expected at least 791 corrigenda, got {corrigenda_count}")
        self.assertGreater(corrigenda_count, 0, "Should have corrigenda when include_corrigenda=True")

        # Verify document type breakdown (1171 Regulations, 138 Directives, 1068 Decisions, 791 corrigenda)
        # Note: corrigenda overlap with document types
        regs = sum(1 for c in result if 'R' in c and not c.startswith('0') and not c.startswith('7'))
        dirs = sum(1 for c in result if 'L' in c and not c.startswith('0') and not c.startswith('7'))
        decs = sum(1 for c in result if 'D' in c and not c.startswith('0') and not c.startswith('7'))
        self.assertGreater(regs, 1000, "Should have many regulations")
        self.assertGreater(decs, 1000, "Should have many decisions")

    def test_get_descriptive_cellar_ids_e2e_consolidated_texts(self):
        """Test get_descriptive_celex_ids with consolidated texts (real request)."""
        start = date(2024, 1, 1)
        end = date(2024, 12, 31)

        result = get_descriptive_celex_ids(
            start_date=start,
            end_date=end,
            include_consolidated_texts=True
        )

        self.assertIsInstance(result, set)

        # Expected: 3281 CELEX IDs for 2024 (including 1085 consolidated texts)
        self.assertGreaterEqual(len(result), 3281,
                                "Expected at least 3281 CELEX IDs for 2024 including consolidated texts")

        # Verify specific known consolidated text IDs are present
        # Consolidated texts have sector 0 and format like: 02024R1689-20240801
        expected_consolidated = {'01962R0031-20240701', '01987R2658-20240701', '01987R2658-20241026'}
        self.assertTrue(expected_consolidated.issubset(result), f"Expected consolidated text IDs not found in result")

        # Count consolidated texts (start with '0')
        consolidated_count = sum(1 for celex in result if celex.startswith('0'))
        self.assertGreaterEqual(consolidated_count, 1085,
                                f"Expected at least 1085 consolidated texts, got {consolidated_count}")
        self.assertGreater(consolidated_count, 0, "Should have consolidated texts when include_consolidated_texts=True")

        # Verify consolidated text format (should contain date suffix like -YYYYMMDD)
        for celex in result:
            if celex.startswith('0'):
                # Format can be: 02024R1689-20240801 or 02019D0802(01)-20240716 (with corrigendum)
                self.assertRegex(celex, r'^0\d{4}[RLDF]\d+(\(\d+\))?-\d{8}$',
                                 f"Consolidated text format invalid: {celex}")

    def test_get_descriptive_cellar_ids_e2e_national_transpositions(self):
        """Test get_descriptive_celex_ids with national transpositions (real request)."""
        start = date(2024, 1, 1)
        end = date(2024, 12, 31)

        result = get_descriptive_celex_ids(
            start_date=start,
            end_date=end,
            include_national_transpositions=True
        )

        self.assertIsInstance(result, set)

        # Expected: 5680 CELEX IDs for 2024 (including 3484 national transpositions)
        self.assertLessEqual(5703, len(result),
                             "Expected exactly 5703 CELEX IDs for 2024 including national transpositions")

        # Verify specific known standard document IDs are present (not transpositions)
        expected_standard = {'32024D01279', '32024D01283', '32024D01287'}
        self.assertTrue(expected_standard.issubset(result), f"Expected standard document IDs not found in result")

        # Count national transpositions (start with '7')
        transposition_count = sum(1 for celex in result if celex.startswith('7'))
        self.assertLessEqual(3507, transposition_count,
                             f"Expected at least 3507 national transpositions, got {transposition_count}")
        self.assertGreater(transposition_count, 0,
                           "Should have national transpositions when include_national_transpositions=True")

        # Verify national transposition format (sector 7, format like: 72004L0038AUT_202400702)
        for celex in result:
            if celex.startswith('7'):
                # Include F for Framework decisions, format: 7YYYYTNNNN[CCC]_NNNNNNNNN
                self.assertRegex(celex, r'^7\d{4}[RLDF]\d+[A-Z]{3}_\d+$',
                                 f"National transposition format invalid: {celex}")

        # Verify we have a good mix of directives since they require national transposition
        dirs = sum(1 for c in result if 'L' in c)
        self.assertLessEqual(3473, dirs, "Expected 3473 documents with 'L' (mostly directives and transpositions)")

    def test_get_descriptive_cellar_ids_e2e_all_options(self):
        """Test get_descriptive_celex_ids with all options enabled (real request)."""
        start = date(2024, 8, 1)
        end = date(2024, 8, 31)

        result = get_descriptive_celex_ids(
            start_date=start,
            end_date=end,
            include_regulations=True,
            include_directives=True,
            include_decisions=True,
            include_corrigenda=True,
            include_consolidated_texts=True,
            include_national_transpositions=True
        )

        self.assertIsInstance(result, set)

        # Expected: 251 CELEX IDs for August 2024 with all options enabled
        self.assertLessEqual(253, len(result), "Expected at least 253 CELEX IDs for August 2024 with all options")

        # Verify specific known IDs of different types are present
        expected_consolidated = {'01999L0031-20240804', '02004D0002-20240814', '02006D0213-20240820'}
        self.assertTrue(expected_consolidated.issubset(result), f"Expected consolidated IDs not found in result")

        # Verify we have different document types
        # Type breakdown: 18 Regulations, 143 Directives, 40 Decisions, 35 consolidated, 138 national, 44 corrigenda
        consolidated_count = sum(1 for c in result if c.startswith('0'))
        national_count = sum(1 for c in result if c.startswith('7'))
        corrigenda_count = sum(1 for c in result if 'R(' in c)

        self.assertGreaterEqual(consolidated_count, 35,
                                f"Expected at least 35 consolidated texts, got {consolidated_count}")
        self.assertGreaterEqual(national_count, 140,
                                f"Expected at least 140 national transpositions, got {national_count}")
        self.assertGreaterEqual(corrigenda_count, 44, f"Expected at least 44 corrigenda, got {corrigenda_count}")

        # Verify all main document types are present
        has_regulation = any('R' in c and not c.startswith('0') and not c.startswith('7') for c in result)
        has_directive = any('L' in c and not c.startswith('0') and not c.startswith('7') for c in result)
        has_decision = any('D' in c and not c.startswith('0') and not c.startswith('7') for c in result)

        self.assertTrue(has_regulation, "Should have regulations")
        self.assertTrue(has_directive, "Should have directives")
        self.assertTrue(has_decision, "Should have decisions")
        self.assertGreater(consolidated_count, 0, "Should have consolidated texts")
        self.assertGreater(national_count, 0, "Should have national transpositions")
        self.assertGreater(corrigenda_count, 0, "Should have corrigenda")


class TestGetProcedureCelexIds(TestCase):
    """Test the get_procedure_celex_ids function."""

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_procedure_celex_ids_with_work(self, mock_sparql):
        """Test get_procedure_celex_ids returns correct structure with available work CELEX."""
        mock_sparql.return_value = {
            "head": {
                "link": [],
                "vars": ["procedure", "proposalCelex", "availableWorkCelex"]
            },
            "results": {
                "distinct": False,
                "ordered": True,
                "bindings": [
                    {
                        "procedure": {"type": "literal", "value": "2023/0202/COD"},
                        "proposalCelex": {
                            "type": "literal",
                            "datatype": "http://www.w3.org/2001/XMLSchema#string",
                            "value": "52023PC0348"
                        },
                        "availableWorkCelex": {
                            "type": "literal",
                            "datatype": "http://www.w3.org/2001/XMLSchema#string",
                            "value": "32025R2518"
                        }
                    },
                    {
                        "procedure": {"type": "literal", "value": "2023/0323/COD"},
                        "proposalCelex": {
                            "type": "literal",
                            "datatype": "http://www.w3.org/2001/XMLSchema#string",
                            "value": "52023PC0533"
                        },
                        "availableWorkCelex": {
                            "type": "literal",
                            "value": ""
                        }
                    }
                ]
            }
        }

        result = get_procedure_celex_ids({"2023/0202/COD", "2023/0323/COD"})

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 2)

        self.assertIn("32025R2518", result)
        self.assertIn("52023PC0533", result)

    @patch('eulexbuild.data.cellar_sparql.get_sparql_request')
    def test_get_procedure_celex_ids_empty_results(self, mock_sparql):
        """Test get_procedure_celex_ids handles empty results correctly."""
        mock_sparql.return_value = {
            "head": {
                "link": [],
                "vars": ["procedure", "proposalCelex", "availableWorkCelex"]
            },
            "results": {
                "distinct": False,
                "ordered": True,
                "bindings": []
            }
        }

        result = get_procedure_celex_ids({"2099/9999/COD"})

        # Verify the result is an empty dictionary
        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 0)
        self.assertEqual(result, set())

    def test_get_procedure_celex_ids_e2e(self, ):
        result = get_procedure_celex_ids({"2023/0202/COD", "2023/0323/COD"})

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 2)

        self.assertIn("32025R2518", result)
        self.assertIn("52023PC0533", result)
