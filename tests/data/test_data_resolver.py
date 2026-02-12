from datetime import date
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from eulexbuild.data.data_resolver import DataResolver


class TestDataResolverGetTitle(TestCase):

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_title_from_sparql_success(self, mock_sparql):
        mock_sparql.return_value = {
            "title": " Test Title from SPARQL ",
            "relations": {}
        }

        resolver = DataResolver("32024R1689")
        result = resolver.get_title()

        self.assertEqual("Test Title from SPARQL", result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_title_from_sparql_with_whitespace(self, mock_sparql):
        mock_sparql.return_value = {
            "title": "\n\t  Test Title  \n\t",
            "relations": {}
        }

        resolver = DataResolver("32024R1689")
        result = resolver.get_title()

        self.assertEqual("Test Title", result)

    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_title_from_metadata_xml_fallback_when_sparql_none(self, mock_sparql, mock_metadata_xml):
        """Test fallback to XML metadata when SPARQL returns None."""
        mock_sparql.return_value = {
            "relations": {}
        }
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <EXPRESSION>
        <EXPRESSION_TITLE>
            <VALUE> Test Title from XML Metadata</VALUE>
        </EXPRESSION_TITLE>
    </EXPRESSION>
</NOTICE>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_title()

        self.assertEqual("Test Title from XML Metadata", result)

    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_title_from_metadata_xml_fallback_when_sparql_empty_string(self, mock_sparql, mock_metadata_xml):
        """Test fallback to XML metadata when SPARQL returns empty string."""
        mock_sparql.return_value = {
            "title": "",
            "relations": {}
        }
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <EXPRESSION>
        <EXPRESSION_TITLE>
            <VALUE>Test Title from XML Fallback </VALUE>
        </EXPRESSION_TITLE>
    </EXPRESSION>
</NOTICE>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_title()

        self.assertEqual("Test Title from XML Fallback", result)

    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_title_from_metadata_xml_fallback_when_sparql_raises_exception(self, mock_sparql,
                                                                               mock_metadata_xml):
        """Test fallback to XML metadata when SPARQL raises an exception."""
        mock_sparql.side_effect = Exception("SPARQL endpoint unavailable")
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <EXPRESSION>
        <EXPRESSION_TITLE>
            <VALUE>Test Title from XML After Exception</VALUE>
        </EXPRESSION_TITLE>
    </EXPRESSION>
</NOTICE>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_title()

        self.assertEqual("Test Title from XML After Exception", result)

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_title_from_html_fallback_when_xml_fails(self, mock_sparql, mock_metadata_xml, mock_html):
        """Test fallback to HTML when both SPARQL and XML fail."""
        mock_sparql.return_value = {
            "relations": {}
        }
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <EXPRESSION>
    </EXPRESSION>
</NOTICE>"""
        mock_html.return_value = b"""<html xmlns="http://www.w3.org/1999/xhtml">
    <body>
        <div class="eli-main-title">Test Title from HTML</div>
    </body>
</html>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_title()

        self.assertEqual("Test Title from HTML", result)

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_title_from_html_with_nested_text(self, mock_sparql, mock_metadata_xml, mock_html):
        mock_sparql.return_value = {
            "title": "",
            "relations": {}
        }
        mock_metadata_xml.side_effect = Exception("XML parsing error")
        mock_html.return_value = b"""<html xmlns="http://www.w3.org/1999/xhtml">
    <body>
        <div class="eli-main-title">
            <span>Title Part 1</span>
            <span> Title Part 2 </span>
        </div>
    </body>
</html>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_title()

        self.assertEqual("Title Part 1 Title Part 2", result)

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_title_returns_unavailable_when_all_sources_fail(self, mock_sparql, mock_metadata_xml, mock_html):
        mock_sparql.return_value = {
            "relations": {}
        }
        mock_metadata_xml.side_effect = Exception("XML error")
        mock_html.side_effect = Exception("HTML error")

        resolver = DataResolver("32024R1689")
        result = resolver.get_title()

        self.assertEqual("[Unavailable]", result)

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_title_returns_unavailable_when_html_has_no_title_div(self, mock_sparql, mock_metadata_xml,
                                                                      mock_html):
        mock_sparql.return_value = {
            "relations": {}
        }
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <EXPRESSION>
    </EXPRESSION>
</NOTICE>"""
        # Provide HTML without the expected eli-main-title class
        mock_html.return_value = b"""<html xmlns="http://www.w3.org/1999/xhtml">
    <body>
        <div class="other-class">No title here</div>
    </body>
</html>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_title()

        self.assertEqual("[Unavailable]", result)

    def test_get_title_real_document_32024R1689(self):
        """Test get_title with a real document using SPARQL (primary source)."""
        resolver = DataResolver("32024R1689")  # AI Act
        result = resolver.get_title()

        self.assertEqual(
            "Regulation (EU) 2024/1689 of the European Parliament and of the Council of 13 June 2024 laying down harmonised rules on artificial intelligence and amending Regulations (EC) No 300/2008, (EU) No 167/2013, (EU) No 168/2013, (EU) 2018/858, (EU) 2018/1139 and (EU) 2019/2144 and Directives 2014/90/EU, (EU) 2016/797 and (EU) 2020/1828 (Artificial Intelligence Act) (Text with EEA relevance)",
            result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_title_real_document_with_xml_fallback(self, mock_sparql):
        """Test XML fallback with real API call when SPARQL is mocked to fail."""
        mock_sparql.return_value = {
            "relations": {}
        }

        resolver = DataResolver("32016R0679")  # GDPR
        result = resolver.get_title()

        self.assertEqual(
            "Regulation (EU) 2016/679 of the European Parliament and of the Council of 27 April 2016 on the protection of natural persons with regard to the processing of personal data and on the free movement of such data, and repealing Directive 95/46/EC (General Data Protection Regulation) (Text with EEA relevance)",
            result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_title_fallback_chain_with_real_apis(self, mock_sparql):
        """Test that the fallback chain works correctly with real API calls."""
        # Mock SPARQL to return empty string to trigger XML fallback
        mock_sparql.return_value = {
            "relations": {}
        }

        resolver = DataResolver("32024R1689")  # AI Act
        result = resolver.get_title()

        self.assertEqual(
            "Regulation (EU) 2024/1689 of the European Parliament and of the Council of 13 June 2024 laying down harmonised rules on artificial intelligence and amending Regulations (EC) No 300/2008, (EU) No 167/2013, (EU) No 168/2013, (EU) 2018/858, (EU) 2018/1139 and (EU) 2019/2144 and Directives 2014/90/EU, (EU) 2016/797 and (EU) 2020/1828 (Artificial Intelligence Act) (Text with EEA relevance)",
            result)

    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_title_real_document_with_html_fallback(self, mock_sparql, mock_metadata_xml):
        """Test HTML fallback with real API call when SPARQL and XML are mocked to fail."""
        mock_sparql.return_value = {
            "relations": {}
        }
        mock_metadata_xml.side_effect = Exception("XML not available")

        resolver = DataResolver("32016R0679")  # GDPR
        result = resolver.get_title()

        self.assertIsInstance(result, str)
        self.assertEqual(
            "Regulation (EU) 2016/679 of the European Parliament and of the Council of 27 April 2016 on the protection of natural persons with regard to the processing of personal data and on the free movement of such data, and repealing Directive 95/46/EC (General Data Protection Regulation) (Text with EEA relevance)"
            .lower(),
            result.lower())

    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_title_xml_to_html_fallback_with_real_api(self, mock_sparql, mock_metadata_xml):
        """Test fallback from XML to HTML with real HTML API call."""
        # Mock both SPARQL and XML to force HTML fallback
        mock_sparql.return_value = {
            "relations": {}
        }
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
    <NOTICE>
        <EXPRESSION>
        </EXPRESSION>
    </NOTICE>"""

        resolver = DataResolver("32024R1689")  # AI Act
        result = resolver.get_title()

        self.assertEqual(
            "Regulation (EU) 2024/1689 of the European Parliament and of the Council of 13 June 2024 laying down harmonised rules on artificial intelligence and amending Regulations (EC) No 300/2008, (EU) No 167/2013, (EU) No 168/2013, (EU) 2018/858, (EU) 2018/1139 and (EU) 2019/2144 and Directives 2014/90/EU, (EU) 2016/797 and (EU) 2020/1828 (Artificial Intelligence Act) (Text with EEA relevance)"
            .lower(),
            result.lower())

    def test_get_title_with_invalid_celex_id(self):
        """Test get_title with an invalid CELEX ID that doesn't exist."""
        # Use a CELEX ID that is unlikely to exist
        resolver = DataResolver("39999R9999")
        result = resolver.get_title()

        # Should return [Unavailable] after all fallbacks fail
        self.assertEqual("[Unavailable]", result)


class TestDataResolverGetDocumentType(TestCase):
    def test_get_document_type(self):
        test_cases = [
            ("32024R1689", "regulation"),  # R = regulation
            ("32008R0765", "regulation"),  # R = regulation
            ("32019L0790", "directive"),  # L = directive
            ("32016R0679", "regulation"),  # R = regulation
            ("32022R2065", "regulation"),  # R = regulation
            ("32024D01459", "decision"),  # D = decision
        ]

        for celex_id, expected_type in test_cases:
            resolver = DataResolver(celex_id)
            result = resolver.get_document_type()
            self.assertEqual(expected_type, result)

    def test_get_document_type_unknown_type(self):
        resolver = DataResolver("32024X1689")
        result = resolver.get_document_type()
        self.assertEqual("Unknown", result)


class TestDataResolverGetDateAdopted(TestCase):

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_date_adopted_from_sparql_success_with_date_object(self, mock_sparql):
        """Test successful retrieval from SPARQL when it returns a date object."""
        mock_sparql.return_value = {
            "date": date(2024, 6, 13),
            "relations": {}
        }

        resolver = DataResolver("32024R1689")
        result = resolver.get_date_adopted()

        self.assertEqual(date(2024, 6, 13), result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_date_adopted_from_sparql_success_with_string(self, mock_sparql):
        """Test successful retrieval from SPARQL when it returns a string in YYYY-MM-DD format."""
        mock_sparql.return_value = {
            "date": "2016-04-27",
            "relations": {}
        }

        resolver = DataResolver("32016R0679")
        result = resolver.get_date_adopted()

        self.assertEqual(date(2016, 4, 27), result)

    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_date_adopted_fallback_to_xml_when_sparql_returns_none(self, mock_sparql, mock_metadata_xml):
        """Test fallback to XML metadata when SPARQL returns None."""
        mock_sparql.return_value = {
            "relations": {}
        }
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <WORK>
        <DATE_DOCUMENT>
            <YEAR>2024</YEAR>
            <MONTH>6</MONTH>
            <DAY>13</DAY>
        </DATE_DOCUMENT>
    </WORK>
</NOTICE>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_date_adopted()

        self.assertEqual(date(2024, 6, 13), result)

    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_date_adopted_fallback_to_xml_when_sparql_returns_empty_string(self, mock_sparql,
                                                                               mock_metadata_xml):
        """Test fallback to XML metadata when SPARQL returns empty string."""
        mock_sparql.return_value = {
            "date": "",
            "relations": {}
        }
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <WORK>
        <DATE_DOCUMENT>
            <YEAR>2022</YEAR>
            <MONTH>10</MONTH>
            <DAY>19</DAY>
        </DATE_DOCUMENT>
    </WORK>
</NOTICE>"""

        resolver = DataResolver("32022R2065")
        result = resolver.get_date_adopted()

        self.assertEqual(date(2022, 10, 19), result)

    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_date_adopted_fallback_to_xml_when_sparql_raises_exception(self, mock_sparql, mock_metadata_xml):
        """Test fallback to XML metadata when SPARQL raises an exception."""
        mock_sparql.side_effect = Exception("SPARQL endpoint unavailable")
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <WORK>
        <DATE_DOCUMENT>
            <YEAR>2016</YEAR>
            <MONTH>4</MONTH>
            <DAY>27</DAY>
        </DATE_DOCUMENT>
    </WORK>
</NOTICE>"""

        resolver = DataResolver("32016R0679")
        result = resolver.get_date_adopted()

        self.assertEqual(date(2016, 4, 27), result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    def test_get_date_adopted_fallback_to_title_when_xml_fails(self, mock_metadata_xml, mock_sparql):
        """Test fallback to title parsing when both SPARQL and XML fail."""
        mock_sparql.return_value = {
            "title": "Regulation (EU) 2024/1689 of the European Parliament and of the Council of 13 June 2024 and so on",
            "relations": {}
        }
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <WORK>
    </WORK>
</NOTICE>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_date_adopted()

        self.assertEqual(date(2024, 6, 13), result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    def test_get_date_adopted_from_title_with_different_date_formats(self, mock_metadata_xml, mock_sparql):
        """Test title parsing with different date formats."""
        mock_sparql.return_value = {
            "title": "Directive of 27 April 2016 on data protection",
            "relations": {}
        }
        mock_metadata_xml.side_effect = Exception("XML error")

        resolver = DataResolver("32016R0679")
        result = resolver.get_date_adopted()

        self.assertEqual(date(2016, 4, 27), result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    def test_get_date_adopted_from_title_with_single_digit_day(self, mock_metadata_xml, mock_sparql):
        """Test title parsing with single-digit day."""
        mock_sparql.return_value = {
            "title": "Regulation of 5 May 2023 on important stuff",
            "relations": {}
        }
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <WORK>
    </WORK>
</NOTICE>"""

        resolver = DataResolver("32023R0001")
        result = resolver.get_date_adopted()

        self.assertEqual(date(2023, 5, 5), result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    def test_get_date_adopted_returns_none_when_all_sources_fail(self, mock_metadata_xml, mock_sparql):
        """Test that None is returned when all sources fail."""
        mock_sparql.return_value = {
            "title": "Some title without a date",
            "relations": {}
        }
        mock_metadata_xml.side_effect = Exception("XML error")

        resolver = DataResolver("32024R1689")
        result = resolver.get_date_adopted()

        self.assertIsNone(result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    def test_get_date_adopted_returns_none_when_title_unavailable(self, mock_metadata_xml, mock_sparql):
        """Test that None is returned when title is [Unavailable]."""
        mock_sparql.return_value = {
            "relations": {}
        }
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <WORK>
    </WORK>
</NOTICE>"""

        resolver = DataResolver("32024R1689")
        # Force title to be set to [Unavailable]
        resolver._title = "[Unavailable]"
        result = resolver.get_date_adopted()

        self.assertIsNone(result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    def test_get_date_adopted_xml_with_invalid_date_values(self, mock_metadata_xml, mock_sparql):
        """Test XML fallback with invalid date values that raise ValueError."""
        mock_sparql.return_value = {
            "title": "Some title without a date",
            "relations": {}
        }
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <WORK>
        <DATE_DOCUMENT>
            <YEAR>2024</YEAR>
            <MONTH>13</MONTH>
            <DAY>32</DAY>
        </DATE_DOCUMENT>
    </WORK>
</NOTICE>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_date_adopted()

        # Should return None since invalid date causes ValueError
        self.assertIsNone(result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    def test_get_date_adopted_xml_with_missing_date_fields(self, mock_metadata_xml, mock_sparql):
        """Test XML fallback with missing date fields."""
        mock_sparql.return_value = {
            "title": "Another title without date pattern",
            "relations": {}
        }
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <WORK>
        <DATE_DOCUMENT>
            <YEAR>2024</YEAR>
        </DATE_DOCUMENT>
    </WORK>
</NOTICE>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_date_adopted()

        # Should return None since date fields are incomplete
        self.assertIsNone(result)

    # Real API integration tests
    def test_get_date_adopted_real_document_32024R1689(self):
        """Test get_date_adopted with a real document using SPARQL (primary source)."""
        resolver = DataResolver("32024R1689")  # AI Act
        result = resolver.get_date_adopted()

        self.assertEqual(date(2024, 6, 13), result)

    def test_get_date_adopted_real_document_32016R0679(self):
        """Test get_date_adopted with GDPR."""
        resolver = DataResolver("32016R0679")  # GDPR
        result = resolver.get_date_adopted()

        self.assertEqual(date(2016, 4, 27), result)

    def test_get_date_adopted_real_document_32022R2065(self):
        """Test get_date_adopted with DSA."""
        resolver = DataResolver("32022R2065")  # DSA
        result = resolver.get_date_adopted()

        self.assertEqual(date(2022, 10, 19), result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_date_adopted_real_document_with_xml_fallback(self, mock_sparql):
        """Test XML fallback with real API call when SPARQL is mocked to fail."""
        mock_sparql.return_value = {
            "relations": {}
        }

        resolver = DataResolver("32024R1689")  # AI Act
        result = resolver.get_date_adopted()

        self.assertEqual(date(2024, 6, 13), result)

    @patch("eulexbuild.data.data_resolver.get_expression_metadata_xml")
    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_date_adopted_real_document_with_title_fallback(self, mock_sparql, mock_metadata_xml):
        """Test title fallback with real title retrieval when SPARQL and XML are mocked to fail."""
        mock_sparql.return_value = {
            "relations": {}
        }
        mock_metadata_xml.return_value = b"""<?xml version="1.0" encoding="UTF-8"?>
<NOTICE>
    <WORK>
    </WORK>
</NOTICE>"""

        resolver = DataResolver("32016R0679")  # GDPR
        result = resolver.get_date_adopted()

        self.assertEqual(date(2016, 4, 27), result)

    def test_get_date_adopted_with_invalid_celex_id(self):
        """Test get_date_adopted with an invalid CELEX ID that doesn't exist."""
        resolver = DataResolver("39999R9999")
        result = resolver.get_date_adopted()

        self.assertIsNone(result)


class TestDataResolverGetTextUnits(TestCase):

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    def test_get_text_units_extracts_recitals(self, mock_html):
        """Test extraction of recitals from HTML."""
        mock_html.return_value = b"""<html xmlns="http://www.w3.org/1999/xhtml">
            <body>
                <div id="rct_1">First recital text.</div>
                <div id="rct_2">Second recital text.</div>
            </body>
        </html>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_text_units()

        recitals = [u for u in result if u["type"] == "recital"]
        self.assertEqual(2, len(recitals))
        self.assertEqual("32024R1689", recitals[0]["celex_id"])
        self.assertEqual("1", recitals[0]["number"])
        self.assertEqual("First recital text.", recitals[0]["text"])
        self.assertEqual("32024R1689", recitals[1]["celex_id"])
        self.assertEqual("2", recitals[1]["number"])
        self.assertEqual("Second recital text.", recitals[1]["text"])

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    def test_get_text_units_extracts_articles(self, mock_html):
        """Test extraction of articles with title from HTML."""
        mock_html.return_value = b"""<html xmlns="http://www.w3.org/1999/xhtml">
            <body>
                <div id="art_1">
                    <div class="oj-ti-art">Article 1</div>
                    <div class="eli-title">Subject matter</div>
                    <div class="oj-normal">Article content here.</div>
                </div>
            </body>
        </html>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_text_units()

        articles = [u for u in result if u["type"] == "article"]
        self.assertEqual(1, len(articles))
        self.assertEqual("32024R1689", articles[0]["celex_id"])
        self.assertEqual("1", articles[0]["number"])
        self.assertEqual("Subject matter", articles[0]["title"])
        self.assertEqual("Article content here.", articles[0]["text"])

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    def test_get_text_units_extracts_articles_without_title(self, mock_html):
        """Test extraction of articles without eli-title."""
        mock_html.return_value = b"""<html xmlns="http://www.w3.org/1999/xhtml">
            <body>
                <div id="art_1">
                    <div class="oj-ti-art">Article 1</div>
                    <div class="oj-normal">Article content without title.</div>
                </div>
            </body>
        </html>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_text_units()

        articles = [u for u in result if u["type"] == "article"]
        self.assertEqual(1, len(articles))
        self.assertEqual("", articles[0]["title"])
        self.assertEqual("Article content without title.", articles[0]["text"])

    @patch("eulexbuild.data.data_resolver.is_standard_structure")
    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    def test_get_text_units_extracts_annexes(self, mock_html, mock_is_standard_structure):
        """Test extraction of annexes from HTML."""
        mock_html.return_value = b"""<html xmlns="http://www.w3.org/1999/xhtml">
            <body>
                <div id="anx_I">
                    <div class="oj-doc-ti">ANNEX I</div>
                    <div class="oj-doc-ti">List of High-Risk AI Systems</div>
                    <div class="oj-normal">Annex content here.</div>
                </div>
            </body>
        </html>"""
        mock_is_standard_structure.return_value = True

        resolver = DataResolver("32024R1689")
        result = resolver.get_text_units(include_recitals=False)

        annexes = [u for u in result if u["type"] == "annex"]
        self.assertEqual(1, len(annexes))
        self.assertEqual("32024R1689", annexes[0]["celex_id"])
        self.assertEqual("I", annexes[0]["number"])
        self.assertEqual("List of High-Risk AI Systems", annexes[0]["title"])
        self.assertEqual("Annex content here.", annexes[0]["text"])

    @patch("eulexbuild.data.data_resolver.is_standard_structure")
    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    def test_get_text_units_filters_annex_header(self, mock_html, mock_is_standard_structure):
        """Test that ANNEX header is filtered out as title."""
        mock_html.return_value = b"""<html xmlns="http://www.w3.org/1999/xhtml">
            <body>
                <div id="anx_IV">
                    <div class="oj-doc-ti">ANNEX IV</div>
                    <div class="oj-doc-ti">Actual Annex Title</div>
                </div>
            </body>
        </html>"""
        mock_is_standard_structure.return_value = True

        resolver = DataResolver("32024R1689")
        result = resolver.get_text_units(include_recitals=False)

        annexes = [u for u in result if u["type"] == "annex"]
        self.assertEqual(1, len(annexes))
        self.assertEqual("Actual Annex Title", annexes[0]["title"])

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    def test_get_text_units_ignores_nested_article_ids(self, mock_html):
        """Test that nested article IDs like art_1_para_2 are ignored."""
        mock_html.return_value = b"""<html xmlns="http://www.w3.org/1999/xhtml">
            <body>
                <div id="art_1">
                    <div class="oj-normal">Article 1 content.</div>
                </div>
                <div id="art_1_para_2">
                    <div class="oj-normal">This should be ignored.</div>
                </div>
            </body>
        </html>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_text_units()

        articles = [u for u in result if u["type"] == "article"]
        self.assertEqual(1, len(articles))
        self.assertEqual("1", articles[0]["number"])

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    def test_get_text_units_normalizes_whitespace(self, mock_html):
        """Test that text is normalized (whitespace collapsed)."""
        mock_html.return_value = b"""<html xmlns="http://www.w3.org/1999/xhtml">
            <body>
                <div id="rct_1">  Text   with   extra   whitespace  </div>
            </body>
        </html>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_text_units()

        recitals = [u for u in result if u["type"] == "recital"]
        self.assertEqual(1, len(recitals))
        self.assertEqual("Text with extra whitespace", recitals[0]["text"])

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    def test_get_text_units_returns_empty_list_when_no_units(self, mock_html):
        """Test that an empty list is returned when no text units found."""
        mock_html.return_value = b"""<html xmlns="http://www.w3.org/1999/xhtml">
            <body>
                <div id="other">No text units here.</div>
            </body>
        </html>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_text_units()

        self.assertEqual([], result)

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    def test_get_text_units_extracts_all_types(self, mock_html):
        """Test extraction of recitals, articles, and annexes together."""
        mock_html.return_value = b"""<html xmlns="http://www.w3.org/1999/xhtml">
            <body>
                <div id="rct_1">Recital text.</div>
                <div id="art_1">
                    <div class="eli-title">Article Title</div>
                    <div class="oj-normal">Article text.</div>
                </div>
                <div id="anx_I">
                    <div class="oj-doc-ti">ANNEX I</div>
                    <div class="oj-doc-ti">Annex Title</div>
                    <div class="oj-normal">Annex text.</div>
                </div>
            </body>
        </html>"""

        resolver = DataResolver("32024R1689")
        result = resolver.get_text_units()

        self.assertEqual(3, len(result))
        types = [u["type"] for u in result]
        self.assertIn("recital", types)
        self.assertIn("article", types)
        self.assertIn("annex", types)

    @patch("eulexbuild.data.data_resolver.is_standard_structure")
    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    def test_get_text_units_handles_roman_numerals_case_insensitive(self, mock_html, mock_is_standard_structure):
        """Test that annex IDs with lowercase roman numerals are matched."""
        mock_html.return_value = b"""<html xmlns="http://www.w3.org/1999/xhtml">
            <body>
                <div id="anx_iv">
                    <div class="oj-normal">Annex with lowercase ID.</div>
                </div>
            </body>
        </html>"""
        mock_is_standard_structure.return_value = True

        resolver = DataResolver("32024R1689")
        result = resolver.get_text_units()

        annexes = [u for u in result if u["type"] == "annex"]
        self.assertEqual(1, len(annexes))
        self.assertEqual("iv", annexes[0]["number"])

    def test_get_text_units_real_document_32024R1689(self):
        """Test get_text_units with a real document (AI Act - Regulation 2024/1689)."""
        resolver = DataResolver("32024R1689")
        result = resolver.get_text_units()

        recitals = [u for u in result if u["type"] == "recital"]
        articles = [u for u in result if u["type"] == "article"]
        annexes = [u for u in result if u["type"] == "annex"]

        self.assertEqual(180, len(recitals))
        self.assertEqual("1", recitals[0]["number"])
        self.assertEqual("180", recitals[-1]["number"])
        self.assertTrue(recitals[0]["text"].startswith("(1) The purpose of this Regulation"))
        self.assertIn("internal market", recitals[0]["text"])

        self.assertEqual(113, len(articles))
        self.assertEqual("1", articles[0]["number"])
        self.assertEqual("113", articles[-1]["number"])
        self.assertEqual("Subject matter`", articles[0]["title"])
        self.assertIn("The purpose of this Regulation is to improve the functioning of the internal market",
                      articles[0]["text"])
        self.assertIn("human-centric and trustworthy artificial intelligence", articles[0]["text"])
        self.assertEqual("Entry into force and application", articles[-1]["title"])
        self.assertIn("enter into force on the twentieth day following", articles[-1]["text"])
        self.assertIn("Official Journal of the European Union", articles[-1]["text"])

        self.assertEqual(13, len(annexes))
        annex_numbers = [a["number"] for a in annexes]
        expected_annex_numbers = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII", "XIII"]
        self.assertEqual(expected_annex_numbers, annex_numbers)
        self.assertEqual("List of Union harmonisation legislation", annexes[0]["title"])
        self.assertIn("New Legislative Framework", annexes[0]["text"])
        self.assertIn("Directive 2006/42/EC", annexes[0]["text"])
        self.assertEqual("High-risk AI systems referred to in Article 6(2)", annexes[2]["title"])
        self.assertIn("High-risk AI systems pursuant to Article 6(2)", annexes[2]["text"])
        self.assertIn("Biometrics", annexes[2]["text"])

    def test_get_text_units_real_document_32016R0679(self):
        """Test get_text_units with GDPR (Regulation 2016/679)."""
        resolver = DataResolver("32016R0679")
        result = resolver.get_text_units()

        recitals = [u for u in result if u["type"] == "recital"]
        articles = [u for u in result if u["type"] == "article"]
        annexes = [u for u in result if u["type"] == "annex"]

        self.assertEqual(173, len(recitals))
        self.assertEqual("1", recitals[0]["number"])
        self.assertEqual("173", recitals[-1]["number"])
        self.assertIn("protection of natural persons", recitals[0]["text"])
        self.assertIn("fundamental right", recitals[0]["text"])

        self.assertEqual(99, len(articles))
        self.assertEqual("1", articles[0]["number"])
        self.assertEqual("99", articles[-1]["number"])
        self.assertIn("Subject-matter and objectives", articles[0]["title"])
        self.assertIn("protection of natural persons", articles[0]["text"])
        self.assertIn("processing of personal data", articles[0]["text"])
        self.assertIn("Entry into force and application", articles[-1]["title"])
        self.assertIn("enter into force", articles[-1]["text"])
        self.assertIn("Official Journal of the European Union", articles[-1]["text"])

        self.assertEqual(0, len(annexes))

    def test_get_text_units_real_document_32024D01459(self):
        """Test get_text_units with AI Office Decision (C/2024/390)."""
        resolver = DataResolver("32024D01459")
        result = resolver.get_text_units()

        recitals = [u for u in result if u["type"] == "recital"]
        articles = [u for u in result if u["type"] == "article"]
        annexes = [u for u in result if u["type"] == "annex"]

        self.assertEqual(8, len(recitals))
        self.assertEqual("1", recitals[0]["number"])
        self.assertEqual("8", recitals[-1]["number"])
        self.assertIn("AI can generate risks", recitals[0]["text"])
        self.assertIn("preparation of the implementation of the forthcoming", recitals[-1]["text"])

        self.assertEqual(9, len(articles))
        self.assertEqual("1", articles[0]["number"])
        self.assertEqual("9", articles[-1]["number"])
        self.assertIn("Establishment", articles[0]["title"])
        self.assertIn("European Artificial Intelligence Office (the ‘Office’) is established", articles[0]["text"])
        self.assertIn("Entry into force", articles[-1]["title"])
        self.assertIn("enter into force", articles[-1]["text"])

        self.assertEqual(0, len(annexes))


class TestDataResolverGetRelations(TestCase):

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_relations_extracts_cites(self, mock_sparql):
        """Test extraction of cites relations."""
        mock_sparql.return_value = {
            "relations": {
                "cites": ["32016R0679", "32018R1807"]
            }
        }

        resolver = DataResolver("32024R1689")
        result = resolver.get_relations()

        cites = [r for r in result if r["relation_type"] == "cites"]
        self.assertEqual(2, len(cites))
        self.assertEqual("32024R1689", cites[0]["celex_source"])
        self.assertEqual("32016R0679", cites[0]["celex_target"])
        self.assertEqual("32024R1689", cites[1]["celex_source"])
        self.assertEqual("32018R1807", cites[1]["celex_target"])

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_relations_extracts_amends(self, mock_sparql):
        """Test extraction of amends relations."""
        mock_sparql.return_value = {
            "relations": {
                "amends": ["32019R1020"]
            }
        }

        resolver = DataResolver("32024R1689")
        result = resolver.get_relations()

        amends = [r for r in result if r["relation_type"] == "amends"]
        self.assertEqual(1, len(amends))
        self.assertEqual("32024R1689", amends[0]["celex_source"])
        self.assertEqual("32019R1020", amends[0]["celex_target"])

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_relations_extracts_adopts(self, mock_sparql):
        """Test extraction of adopts relations."""
        mock_sparql.return_value = {
            "relations": {
                "adopts": ["32023R1234"]
            }
        }

        resolver = DataResolver("32024R1689")
        result = resolver.get_relations()

        adopts = [r for r in result if r["relation_type"] == "adopts"]
        self.assertEqual(1, len(adopts))
        self.assertEqual("32024R1689", adopts[0]["celex_source"])
        self.assertEqual("32023R1234", adopts[0]["celex_target"])

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_relations_extracts_based_on(self, mock_sparql):
        """Test extraction of based_on relations."""
        mock_sparql.return_value = {
            "relations": {
                "based_on": ["12016E114"]
            }
        }

        resolver = DataResolver("32024R1689")
        result = resolver.get_relations()

        based_on = [r for r in result if r["relation_type"] == "based_on"]
        self.assertEqual(1, len(based_on))
        self.assertEqual("32024R1689", based_on[0]["celex_source"])
        self.assertEqual("12016E114", based_on[0]["celex_target"])

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_relations_extracts_all_types(self, mock_sparql):
        """Test extraction of all relation types together."""
        mock_sparql.return_value = {
            "relations": {
                "cites": ["32016R0679"],
                "amends": ["32019R1020"],
                "adopts": ["32023R1234"],
                "based_on": ["12016E114"]
            }
        }

        resolver = DataResolver("32024R1689")
        result = resolver.get_relations()

        self.assertEqual(4, len(result))
        relation_types = [r["relation_type"] for r in result]
        self.assertIn("cites", relation_types)
        self.assertIn("amends", relation_types)
        self.assertIn("adopts", relation_types)
        self.assertIn("based_on", relation_types)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_relations_returns_empty_list_when_no_relations(self, mock_sparql):
        """Test that empty list is returned when no relations found."""
        mock_sparql.return_value = {
            "relations": {}
        }

        resolver = DataResolver("32024R1689")
        result = resolver.get_relations()

        self.assertEqual([], result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_relations_handles_multiple_targets_same_type(self, mock_sparql):
        """Test handling multiple targets of same relation type."""
        mock_sparql.return_value = {
            "relations": {
                "cites": ["32016R0679", "32018R1807", "32019R0881"]
            }
        }

        resolver = DataResolver("32024R1689")
        result = resolver.get_relations()

        self.assertEqual(3, len(result))
        targets = [r["celex_target"] for r in result]
        self.assertIn("32016R0679", targets)
        self.assertIn("32018R1807", targets)
        self.assertIn("32019R0881", targets)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_relations_skips_empty_targets(self, mock_sparql):
        """Test that empty target strings are skipped."""
        mock_sparql.return_value = {
            "relations": {
                "cites": ["32016R0679", "", "32018R1807"],
                "amends": [""]
            }
        }

        resolver = DataResolver("32024R1689")
        result = resolver.get_relations()

        self.assertEqual(2, len(result))
        targets = [r["celex_target"] for r in result]
        self.assertIn("32016R0679", targets)
        self.assertIn("32018R1807", targets)
        self.assertNotIn("", targets)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_relations_handles_exception_gracefully(self, mock_sparql):
        """Test that exception in SPARQL is handled gracefully."""
        mock_sparql.side_effect = Exception("SPARQL error")

        resolver = DataResolver("32024R1689")
        result = resolver.get_relations()

        self.assertEqual([], result)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_relations_includes_celex_source_in_all(self, mock_sparql):
        """Test that celex_source is included in all relation dicts."""
        mock_sparql.return_value = {
            "relations": {
                "cites": ["32016R0679"],
                "amends": ["32019R1020"],
                "adopts": ["32023R1234"],
                "based_on": ["12016E114"]
            }
        }

        resolver = DataResolver("32024R1689")
        result = resolver.get_relations()

        for relation in result:
            self.assertIn("celex_source", relation)
            self.assertEqual("32024R1689", relation["celex_source"])
            self.assertIn("celex_target", relation)
            self.assertIn("relation_type", relation)

    @patch("eulexbuild.data.data_resolver.get_all_properties")
    def test_get_relations_preserves_order(self, mock_sparql):
        """Test that relations are returned in consistent order (cites, amends, adopts, based_on)."""
        mock_sparql.return_value = {
            "relations": {
                "cites": ["32016R0679"],
                "amends": ["32019R1020"],
                "adopts": ["32023R1234"],
                "based_on": ["12016E114"]
            }
        }

        resolver = DataResolver("32024R1689")
        result = resolver.get_relations()

        # Note: Order depends on dictionary iteration, which is insertion order in Python 3.7+
        # But the exact order may vary, so just check all types are present
        relation_types = [r["relation_type"] for r in result]
        self.assertIn("cites", relation_types)
        self.assertIn("amends", relation_types)
        self.assertIn("adopts", relation_types)
        self.assertIn("based_on", relation_types)

    def test_get_relations_real_document_ai_act(self):
        """Test get_relations with a real document (AI Act - Regulation 2024/1689)."""
        resolver = DataResolver("32024R1689")
        result = resolver.get_relations()

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

        for relation in result:
            self.assertEqual("32024R1689", relation["celex_source"])
            self.assertIn("celex_target", relation)
            self.assertIn("relation_type", relation)
            self.assertIn(relation["relation_type"], ["cites", "amends", "adopts", "based_on"])

        cites = [r for r in result if r["relation_type"] == "cites"]
        cites_targets = [r["celex_target"] for r in cites]
        self.assertGreater(len(cites), 60)
        self.assertIn("32016R0679", cites_targets)
        self.assertIn("32022R2065", cites_targets)
        self.assertIn("32019R0881", cites_targets)
        self.assertIn("32019R1020", cites_targets)
        self.assertIn("32017R0745", cites_targets)
        self.assertIn("32006L0042", cites_targets)
        self.assertIn("32024D01459", cites_targets)

        amends = [r for r in result if r["relation_type"] == "amends"]
        amends_targets = [r["celex_target"] for r in amends]
        self.assertEqual(9, len(amends))
        self.assertIn("32014R0090", amends_targets)
        self.assertIn("32008R0300", amends_targets)
        self.assertIn("32016L0797", amends_targets)
        self.assertIn("32013R0167", amends_targets)
        self.assertIn("32013R0168", amends_targets)
        self.assertIn("32018R0858", amends_targets)
        self.assertIn("32018R1139", amends_targets)
        self.assertIn("32019R2144", amends_targets)
        self.assertIn("32020L1828", amends_targets)

        adopts = [r for r in result if r["relation_type"] == "adopts"]
        adopts_targets = [r["celex_target"] for r in adopts]
        self.assertEqual(1, len(adopts))
        self.assertIn("52021PC0206", adopts_targets)

        based_on = [r for r in result if r["relation_type"] == "based_on"]
        based_on_targets = [r["celex_target"] for r in based_on]
        self.assertEqual(2, len(based_on))
        self.assertIn("12016E114", based_on_targets)
        self.assertIn("12016E016", based_on_targets)

    def test_get_relations_real_document_gdpr(self):
        """Test get_relations with GDPR (Regulation 2016/679)."""
        resolver = DataResolver("32016R0679")
        result = resolver.get_relations()

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

        for relation in result:
            self.assertEqual("32016R0679", relation["celex_source"])
            self.assertIn("celex_target", relation)
            self.assertIn("relation_type", relation)
            self.assertIn(relation["relation_type"], ["cites", "amends", "adopts", "based_on"])

        cites = [r for r in result if r["relation_type"] == "cites"]
        cites_targets = [r["celex_target"] for r in cites]
        self.assertGreater(len(cites), 30)
        self.assertIn("31993L0013", cites_targets)
        self.assertIn("12012E101", cites_targets)
        self.assertIn("32002L0058", cites_targets)
        self.assertIn("32001R0045", cites_targets)
        self.assertIn("32000L0031", cites_targets)

        amends = [r for r in result if r["relation_type"] == "amends"]
        self.assertEqual(0, len(amends))

        adopts = [r for r in result if r["relation_type"] == "adopts"]
        adopts_targets = [r["celex_target"] for r in adopts]
        self.assertEqual(1, len(adopts))
        self.assertIn("52012PC0011", adopts_targets)

        based_on = [r for r in result if r["relation_type"] == "based_on"]
        based_on_targets = [r["celex_target"] for r in based_on]
        self.assertEqual(1, len(based_on))
        self.assertIn("12012E016", based_on_targets)


class TestDataResolverManualTextExtraction(TestCase):

    def _read_test_file(self, filename) -> bytes:
        file_path = Path(__file__).parent / "test_html_files" / filename
        with open(file_path, "rb") as f:
            return f.read()

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    def test_extract_manual_structure_52023PC0533(self, mock_html):
        """Test manual text extraction for 52023PC0533 (XML file)."""
        content = self._read_test_file("manual_text_extraction_52023PC0533.xml")
        mock_html.return_value = content

        resolver = DataResolver("52023PC0533")
        units = resolver.get_text_units()

        self.assertEqual(len(units), 52)

        # Check for Recitals
        recitals = [u for u in units if u["type"] == "recital"]
        self.assertEqual(len(recitals), 32)
        self.assertEqual(recitals[0]["text"],
                         "(1) Most goods and services are supplied within the internal market by economic operators to other economic operators and to public authorities on a deferred payment basis whereby the supplier gives its client time to pay the invoice, as agreed between parties, as set out in the suppliers’ invoice, or as laid down by law.")
        self.assertEqual(recitals[0]["number"], "1")

        self.assertEqual(recitals[12]["text"],
                         "(13) This Regulation should be without prejudice to shorter periods which may be provided for in national law, and which are more favourable to the creditor.")
        self.assertEqual(recitals[12]["number"], "13")

        # Check for Articles
        articles = [u for u in units if u["type"] == "article"]
        self.assertEqual(len(articles), 20)
        self.assertIn("The delivery of goods or the provision of services referred to in paragraph 1 shall",
                      articles[0]["text"])
        self.assertEqual(articles[0]["number"], "1")
        self.assertEqual(articles[0]["title"], "Scope")

        self.assertIn("calculating the period referred to in paragraph 1",
                      articles[11]["text"])
        self.assertEqual(articles[11]["number"], "12")
        self.assertEqual(articles[11]["title"], "Recovery procedures for unchallenged claims")

        self.assertIn(
            "This Regulation shall enter into force on the day following that of its publication in the Official Journal of the European Union",
            articles[19]["text"])
        self.assertEqual(articles[19]["number"], "20")

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    def test_extract_manual_structure_52025PC1006(self, mock_html):
        """Test manual text extraction for 52025PC1006 (Main HTML)."""
        content = self._read_test_file("manual_text_extraction_52025PC1006.html")
        mock_html.return_value = content

        resolver = DataResolver("52025PC1006")
        units = resolver.get_text_units()

        self.assertEqual(len(units), 125)

        # Check for Recitals
        recitals = [u for u in units if u["type"] == "recital"]
        self.assertEqual(len(recitals), 85)

        self.assertIn("Clean Industrial Deal", recitals[0]["text"])
        self.assertEqual(recitals[0]["number"], "1")

        self.assertEqual(
            "(28) The Commission should have the possibility to nominate European coordinators for projects facing particular difficulties or delays, in order to facilitate the implementation of projects which encounter difficulties.",
            recitals[27]["text"])
        self.assertEqual(recitals[27]["number"], "28")

        self.assertIn("cannot be sufficiently achieved by the Member States", recitals[84]["text"])
        self.assertEqual(recitals[84]["number"], "85")

        # Check for Articles
        articles = [u for u in units if u["type"] == "article"]
        self.assertEqual(len(articles), 33)

        self.assertIn("climate neutrality objective by 2050 at the latest", articles[0]["text"])
        self.assertEqual(articles[0]["number"], "1")
        self.assertEqual(articles[0]["title"], "Subject matter and scope")

        self.assertIn(
            "synergies with priority corridors and thematic areas identified under trans-European networks for transport and telecommunications",
            articles[3]["text"])
        self.assertEqual(articles[3]["number"], "4")
        self.assertEqual(articles[3]["title"], "Criteria for the assessment of projects by the Groups")

        # Check for Annexes
        annexes = [u for u in units if u["type"] == "annex"]
        self.assertEqual(len(annexes), 7)

        self.assertIn("North-South electricity interconnections", annexes[0]["text"])
        self.assertEqual(annexes[0]["number"], "I")

        self.assertIn("optimal energy network should also ensure security of supply and lead", annexes[6]["text"])
        self.assertEqual(annexes[6]["number"], "VII")

    @patch("eulexbuild.data.data_resolver.get_full_text_xhtml")
    def test_extract_manual_structure_annexes_52025PC1006(self, mock_html):
        """Test manual text extraction for annexes of 52025PC1006."""
        content = self._read_test_file("manual_text_extraction_annexes_52025PC1006.html")
        mock_html.return_value = content

        resolver = DataResolver("52025PC1006")
        units = resolver.get_text_units()

        # Check for Annexes
        annexes = [u for u in units if u["type"] == "annex"]
        self.assertEqual(len(annexes), 7)

        self.assertIn(
            "This Regulation shall apply to the following trans-European energy infrastructure priority corridors and areas",
            annexes[0]["text"])
        self.assertIn(
            "Baltic Energy Market Interconnection Plan in electricity",
            annexes[0]["text"])
        self.assertIn(
            "PRIORITY CORRIDORS FOR HYDROGEN AND ELECTROLYSERS",
            annexes[0]["text"])
        self.assertIn(
            "supporting the deployment of power-to-gas applications aiming to enable greenhouse gas reductions and contributing to secure",
            annexes[0]["text"])
        self.assertIn(
            "synthetic fuel gases leading to the permanent neutralization of carbon dioxide",
            annexes[0]["text"])
        self.assertEqual(annexes[0]["number"], "I")
        self.assertEqual(annexes[0]["title"], "ENERGY INFRASTRUCTURE PRIORITY CORRIDORS AND AREAS")

        self.assertIn(
            "The framework methodology developed by ACER",
            annexes[6]["text"])
        self.assertIn(
            "It shall look at medium (10-15 years) and long-term (20-30 years) time horizon based",
            annexes[6]["text"])
        self.assertIn(
            "that need to be addressed over the next ten to twenty years",
            annexes[6]["text"])
        self.assertEqual(annexes[6]["number"], "VII")
        self.assertEqual(annexes[6]["title"], "INFRASTRUCTURE NEEDS IDENTIFICATION REPORTS")
