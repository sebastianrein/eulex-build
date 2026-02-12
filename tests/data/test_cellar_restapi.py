import logging
from unittest import TestCase
from unittest.mock import patch, Mock

import requests

from eulexbuild.data.cellar_restapi import (
    close_session, APIRequestError,
    get_full_text_xhtml, get_full_text_plain_html,
    _parse_300_response, _select_document
)


class TestGetFullText(TestCase):
    @classmethod
    def tearDownClass(cls):
        close_session()

    def test_raises_error_for_invalid_celex_id(self):
        with self.assertRaises(ValueError):
            get_full_text_xhtml("invalid_celex")

    @patch("eulexbuild.data.cellar_restapi.get_session")
    def test_returns_text_for_valid_response(self, mock_get_session):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = "<html>Valid Response</html>"
        mock_get_session.return_value.get.return_value = mock_response

        result = get_full_text_xhtml("32024R1689")
        self.assertEqual(result, "<html>Valid Response</html>")

    @patch("eulexbuild.data.cellar_restapi.get_session")
    def test_raises_error_for_404_response(self, mock_get_session):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get_session.return_value.get.return_value = mock_response

        with self.assertRaises(APIRequestError):
            get_full_text_xhtml("32024R1689")

    @patch("eulexbuild.data.cellar_restapi.get_session")
    def test_raises_error_for_403_response(self, mock_get_session):
        mock_response = Mock()
        mock_response.status_code = 403
        mock_get_session.return_value.get.return_value = mock_response

        with self.assertRaises(APIRequestError):
            get_full_text_xhtml("32024R1689")

    @patch("eulexbuild.data.cellar_restapi.get_session")
    def test_raises_error_for_timeout(self, mock_get_session):
        mock_get_session.return_value.get.side_effect = requests.exceptions.Timeout

        with self.assertRaises(APIRequestError):
            get_full_text_xhtml("32024R1689")

    @patch("eulexbuild.data.cellar_restapi.get_session")
    def test_raises_error_for_connection_error(self, mock_get_session):
        mock_get_session.return_value.get.side_effect = requests.exceptions.ConnectionError

        with self.assertRaises(APIRequestError):
            get_full_text_xhtml("32024R1689")


class TestGetFullTextIntegration(TestCase):
    @classmethod
    def tearDownClass(cls):
        close_session()

    def test_retrieves_real_document_32024R1689(self):
        celex_id = "32024R1689"

        try:
            result = get_full_text_xhtml(celex_id)

            self.assertIsInstance(result, bytes)
            self.assertGreater(len(result), 0)

            self.assertTrue(
                result.strip().startswith(b'<') or b'<?xml' in result,
                "Response should be XML/XHTML content"
            )

        except APIRequestError as e:
            self.fail(f"API request failed: {e}")


class TestParse300Response(TestCase):
    @classmethod
    def tearDownClass(cls):
        close_session()

    def test_parses_valid_300_response(self):
        """Test parsing a valid 300 response HTML."""
        html_content = b"""
        <html><head><title>300 Multiple-Choice Response</title></head><body>
        List of URI's:<ul>
        <li title="manifestation">cellar:63317ca3-db66-11f0-8da2-01aa75ed71a1.0001.03<ul>
        <li title="item"><a href="http://publications.europa.eu/resource/cellar/63317ca3-db66-11f0-8da2-01aa75ed71a1.0001.03/DOC_1">
        <span class="stream_page_physical_first"></span>&nbsp;-&nbsp;<span class="stream_page_physical_last"></span>&nbsp;
        <span class="url">(http://publications.europa.eu/resource/cellar/63317ca3-db66-11f0-8da2-01aa75ed71a1.0001.03/DOC_1)</span></a><ul>
        <li title="stream_name">1_EN_ACT_part1_v12.html</li>
        <li title="stream_label">amd_comnat_COM_2025_0994_FIN.ENG.xhtml_1766058410035.rdf</li>
        <li title="stream_order" id="streamOrder">1</li>
        </ul></li>
        <li title="item"><a href="http://publications.europa.eu/resource/cellar/63317ca3-db66-11f0-8da2-01aa75ed71a1.0001.03/DOC_2">
        <span class="stream_page_physical_first"></span>&nbsp;-&nbsp;<span class="stream_page_physical_last"></span>&nbsp;
        <span class="url">(http://publications.europa.eu/resource/cellar/63317ca3-db66-11f0-8da2-01aa75ed71a1.0001.03/DOC_2)</span></a><ul>
        <li title="stream_name">1_EN_annexe_proposition_part1_v4.html</li>
        <li title="stream_label">amd_comnat_COM_2025_0994_FIN.ENG.xhtml_1766058410035.rdf</li>
        <li title="stream_order" id="streamOrder">2</li>
        </ul></li>
        </ul></li></ul></body></html>
        """

        result = _parse_300_response(html_content)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0],
                         "http://publications.europa.eu/resource/cellar/63317ca3-db66-11f0-8da2-01aa75ed71a1.0001.03/DOC_1")
        self.assertEqual(result[0][1], "1_EN_ACT_part1_v12.html")
        self.assertEqual(result[0][2], 1)
        self.assertEqual(result[1][0],
                         "http://publications.europa.eu/resource/cellar/63317ca3-db66-11f0-8da2-01aa75ed71a1.0001.03/DOC_2")
        self.assertEqual(result[1][1], "1_EN_annexe_proposition_part1_v4.html")
        self.assertEqual(result[1][2], 2)

    def test_handles_missing_stream_order(self):
        """Test parsing when stream_order is missing."""
        html_content = b"""
        <html><body><ul>
        <li title="item"><a href="http://example.com/doc1">Link</a><ul>
        <li title="stream_name">document.html</li>
        </ul></li>
        </ul></body></html>
        """

        result = _parse_300_response(html_content)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][2], 999)  # Default value


class TestSelectDocument(TestCase):
    """Test the _select_document helper function."""

    def test_selects_act_document_over_annexe(self):
        """Test that ACT file is selected over annexe."""
        items = [
            ("http://example.com/doc1", "1_EN_ACT_part1_v12.html", 1),
            ("http://example.com/doc2", "1_EN_annexe_proposition_part1_v4.html", 2),
        ]

        result = _select_document(
            items,
            include_keywords={'ACT'},
            exclude_keywords={'annexe', 'annex', 'cover', 'erratum', 'corrigendum'}
        )

        self.assertEqual(result, "http://example.com/doc1")

    def test_excludes_annexe_files(self):
        """Test that annexe files are excluded."""
        items = [
            ("http://example.com/doc1", "document.html", 2),
            ("http://example.com/doc2", "annexe_document.html", 1),
        ]

        result = _select_document(items, exclude_keywords={'annexe', 'annex'})

        self.assertEqual(result, "http://example.com/doc1")

    def test_excludes_various_supplementary_files(self):
        """Test that various supplementary file types are excluded."""
        items = [
            ("http://example.com/doc1", "main_document.html", 4),
            ("http://example.com/doc2", "annex_data.html", 1),
            ("http://example.com/doc3", "cover_page.html", 2),
            ("http://example.com/doc4", "erratum_text.html", 3),
        ]

        result = _select_document(
            items,
            exclude_keywords={'annex', 'cover', 'erratum'}
        )

        self.assertEqual(result, "http://example.com/doc1")

    def test_prefers_lowest_stream_order(self):
        """Test that lowest stream_order is preferred among valid candidates."""
        items = [
            ("http://example.com/doc1", "document_part2.html", 2),
            ("http://example.com/doc2", "document_part1.html", 1),
            ("http://example.com/doc3", "document_part3.html", 3),
        ]

        result = _select_document(items)

        self.assertEqual(result, "http://example.com/doc2")

    def test_act_file_wins_despite_higher_order(self):
        """Test that ACT file is selected even with higher stream_order."""
        items = [
            ("http://example.com/doc1", "document.html", 1),
            ("http://example.com/doc2", "EN_ACT_document.html", 3),
        ]

        result = _select_document(items, include_keywords={'ACT'})

        self.assertEqual(result, "http://example.com/doc2")

    def test_fallback_to_first_when_only_annexes(self):
        """Test fallback to first document when all are annexes."""
        logger = logging.getLogger("test")
        items = [
            ("http://example.com/doc1", "annexe_1.html", 1),
            ("http://example.com/doc2", "annexe_2.html", 2),
        ]

        with self.assertLogs(logger, level='WARNING') as cm:
            result = _select_document(
                items,
                exclude_keywords={'annexe'},
                logger=logger
            )

        self.assertEqual(result, "http://example.com/doc1")
        self.assertIn("No main document found", cm.output[0])
        self.assertIn("annexe_1.html", cm.output[0])

    def test_raises_error_for_empty_items_list(self):
        """Test that empty items list raises an error."""
        with self.assertRaises(APIRequestError):
            _select_document([])


class TestGetFullTextPlainHtml(TestCase):
    """Test get_full_text_plain_html with 300 response handling."""

    @classmethod
    def tearDownClass(cls):
        close_session()

    @patch("eulexbuild.data.cellar_restapi.get_session")
    def test_handles_300_response_and_selects_main_document(self, mock_get_session):
        """Test that 300 response is handled correctly."""
        # Mock 300 response
        mock_300_response = Mock()
        mock_300_response.status_code = 300
        mock_300_response.content = b"""
        <html><body><ul>
        <li title="item"><a href="http://example.com/doc1">Link</a><ul>
        <li title="stream_name">1_EN_ACT_part1.html</li>
        <li title="stream_order">1</li>
        </ul></li>
        <li title="item"><a href="http://example.com/doc2">Link</a><ul>
        <li title="stream_name">1_EN_annexe.html</li>
        <li title="stream_order">2</li>
        </ul></li>
        </ul></body></html>
        """

        # Mock successful response for selected document
        mock_final_response = Mock()
        mock_final_response.status_code = 200
        mock_final_response.content = b"<html>Main Document Content</html>"

        # Configure mock to return 300 first, then 200
        mock_session = Mock()
        mock_session.get.side_effect = [mock_300_response, mock_final_response]
        mock_get_session.return_value = mock_session

        result = get_full_text_plain_html("32024R1689")

        self.assertEqual(result, b"<html>Main Document Content</html>")
        # Verify that two requests were made
        self.assertEqual(mock_session.get.call_count, 2)
        # Second call should be to the ACT document
        second_call_url = mock_session.get.call_args_list[1][0][0]
        self.assertEqual(second_call_url, "http://example.com/doc1")

    @patch("eulexbuild.data.cellar_restapi.get_session")
    def test_handles_regular_200_response(self, mock_get_session):
        """Test that regular 200 responses work as before."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"<html>Regular Response</html>"
        mock_get_session.return_value.get.return_value = mock_response

        result = get_full_text_plain_html("32024R1689")

        self.assertEqual(result, b"<html>Regular Response</html>")

    @patch("eulexbuild.data.cellar_restapi.get_session")
    def test_raises_error_for_404_after_300_redirect(self, mock_get_session):
        """Test error handling when selected document returns 404."""
        # Mock 300 response
        mock_300_response = Mock()
        mock_300_response.status_code = 300
        mock_300_response.content = b"""
        <html><body><ul>
        <li title="item"><a href="http://example.com/doc1">Link</a><ul>
        <li title="stream_name">document.html</li>
        <li title="stream_order">1</li>
        </ul></li>
        </ul></body></html>
        """

        # Mock 404 response for selected document
        mock_404_response = Mock()
        mock_404_response.status_code = 404

        mock_session = Mock()
        mock_session.get.side_effect = [mock_300_response, mock_404_response]
        mock_get_session.return_value = mock_session

        with self.assertRaises(APIRequestError):
            get_full_text_plain_html("32024R1689")
