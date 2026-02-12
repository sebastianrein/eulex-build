import os
from datetime import date, timedelta
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch, MagicMock

import eulexbuild


class TestValidateConfiguration(TestCase):

    @patch("eulexbuild.config_validation.Path")
    def test_file_does_not_exist(self, mock_path_class):
        mock_instance = MagicMock()
        mock_instance.exists.return_value = False
        mock_path_class.return_value = mock_instance
        self.assertRaises(FileNotFoundError, eulexbuild.validate_configuration, "")

    @patch("eulexbuild.config_validation.Path")
    def test_is_not_a_file(self, mock_path_class):
        mock_instance = MagicMock()
        mock_instance.exists.return_value = True
        mock_instance.is_file.return_value = False
        mock_path_class.return_value = mock_instance
        self.assertRaises(ValueError, eulexbuild.validate_configuration, "")

    def test_simple_valid_config(self):
        test_file = Path(__file__).parent / "test_configurations" / "simple_valid_file.yaml"
        config = eulexbuild.validate_configuration(test_file)

        # Metadata
        self.assertEqual(config.metadata.project_name, "Simple Valid Dataset")
        self.assertEqual(config.metadata.author, "")
        self.assertEqual(config.metadata.description, "A new dataset constructed with EULEX-BUILD.")
        self.assertTrue(date.today() - config.metadata.date_created <= timedelta(days=1))
        self.assertEqual(config.metadata.version, "1.0")

        # Data
        self.assertEqual(config.data.mode, "fixed")
        self.assertIn("32024R1689", config.data.celex_ids)
        self.assertIn("32008R0765", config.data.celex_ids)
        self.assertIn("32019L0790", config.data.celex_ids)

        # Processing
        self.assertEqual(config.processing.enable_parallel_processing, True)
        self.assertEqual(config.processing.max_threads, os.cpu_count() - 1)
        self.assertEqual(config.processing.text_extraction.include_recitals, True)
        self.assertEqual(config.processing.text_extraction.include_articles, True)
        self.assertEqual(config.processing.text_extraction.include_annexes, True)

        # Output
        self.assertIn("csv", config.output.formats)
        self.assertIn("parquet", config.output.formats)
        self.assertEqual(config.output.output_directory, "./output")

    def test_valid_config_with_procedure_numbers(self):
        test_file = Path(__file__).parent / "test_configurations" / "valid_procedure_numbers.yaml"
        config = eulexbuild.validate_configuration(test_file)

        # Metadata - should use defaults
        self.assertEqual(config.metadata.project_name, "EULEX-BUILD Dataset")
        self.assertEqual(config.metadata.author, "")
        self.assertEqual(config.metadata.description, "A new dataset constructed with EULEX-BUILD.")
        self.assertTrue(date.today() - config.metadata.date_created <= timedelta(days=1))
        self.assertEqual(config.metadata.version, "1.0")

        # Data
        self.assertEqual(config.data.mode, "fixed")
        self.assertEqual(len(config.data.celex_ids), 2)
        self.assertIn("32024R1689", config.data.celex_ids)
        self.assertIn("32008R0765", config.data.celex_ids)
        self.assertEqual(len(config.data.procedure_numbers), 2)
        self.assertIn("2023/0202/COD", config.data.procedure_numbers)
        self.assertIn("2023/0323/COD", config.data.procedure_numbers)

        # Processing - should use defaults
        self.assertEqual(config.processing.enable_parallel_processing, True)
        self.assertEqual(config.processing.max_threads, os.cpu_count() - 1)
        self.assertEqual(config.processing.text_extraction.include_recitals, True)
        self.assertEqual(config.processing.text_extraction.include_articles, True)
        self.assertEqual(config.processing.text_extraction.include_annexes, True)

        # Output - should use defaults
        self.assertIn("csv", config.output.formats)
        self.assertIn("parquet", config.output.formats)
        self.assertEqual(config.output.output_directory, "./output")

    def test_valid_config_fully_fixed_configured(self):
        test_file = Path(__file__).parent / "test_configurations" / "valid_fixed.yaml"
        config = eulexbuild.validate_configuration(test_file)

        # Metadata
        self.assertEqual(config.metadata.project_name, "EU Legal Acts Analysis Dataset")
        self.assertEqual(config.metadata.author, "John Doe")
        self.assertEqual(config.metadata.description,
                         "A comprehensive dataset of EU regulations and directives for legal analysis")
        self.assertEqual(config.metadata.date_created, date(2024, 1, 15))
        self.assertEqual(config.metadata.version, "2.0")

        # Data
        self.assertEqual(config.data.mode, "fixed")
        self.assertEqual(len(config.data.celex_ids), 6)
        self.assertIn("32024R1689", config.data.celex_ids)
        self.assertIn("32008R0765", config.data.celex_ids)
        self.assertIn("32019L0790", config.data.celex_ids)
        self.assertIn("32016R0679", config.data.celex_ids)
        self.assertIn("32022R2065", config.data.celex_ids)
        self.assertIn("32024D01459", config.data.celex_ids)

        # Processing
        self.assertEqual(config.processing.enable_parallel_processing, True)
        self.assertEqual(config.processing.max_threads, 15)
        self.assertEqual(config.processing.text_extraction.include_recitals, True)
        self.assertEqual(config.processing.text_extraction.include_articles, True)
        self.assertEqual(config.processing.text_extraction.include_annexes, True)

        # Output
        self.assertIn("csv", config.output.formats)
        self.assertIn("parquet", config.output.formats)
        self.assertEqual(config.output.output_directory, "./custom_output")

    def test_valid_config_descriptive_mode(self):
        test_file = Path(__file__).parent / "test_configurations" / "valid_descriptive.yaml"
        config = eulexbuild.validate_configuration(test_file)

        # Metadata
        self.assertEqual(config.metadata.project_name, "EU Climate and Digital Policy Dataset")
        self.assertEqual(config.metadata.author, "Jane Smith")
        self.assertEqual(config.metadata.description,
                         "Dataset of EU regulations and directives related to climate and digital policies from 2020-2023")
        self.assertEqual(config.metadata.date_created, date(2024, 3, 20))
        self.assertEqual(config.metadata.version, "3.1")

        # Data
        self.assertEqual(config.data.mode, "descriptive")
        self.assertIn("directive", config.data.document_types)
        self.assertIn("regulation", config.data.document_types)
        self.assertEqual(config.data.start_date, date(2020, 1, 1))
        self.assertEqual(config.data.end_date, date(2023, 12, 31))
        self.assertEqual(len(config.data.filter_keywords), 4)
        self.assertIn("climate", config.data.filter_keywords)
        self.assertIn("digital", config.data.filter_keywords)
        self.assertIn("environment", config.data.filter_keywords)
        self.assertIn("cybersecurity", config.data.filter_keywords)
        self.assertEqual(config.data.include_corrigenda, False)
        self.assertEqual(config.data.include_consolidated_texts, False)
        self.assertEqual(config.data.include_national_transpositions, False)

        # Processing
        self.assertEqual(config.processing.enable_parallel_processing, False)
        self.assertEqual(config.processing.max_threads, 1)
        self.assertEqual(config.processing.text_extraction.include_recitals, True)
        self.assertEqual(config.processing.text_extraction.include_articles, True)
        self.assertEqual(config.processing.text_extraction.include_annexes, True)

        # Output
        self.assertIn("parquet", config.output.formats)
        self.assertEqual(config.output.output_directory, "./descriptive_output")

    def test_invalid_missing_data(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_missing_data.yaml"
        with self.assertRaises(Exception):
            eulexbuild.validate_configuration(test_file)

    def test_invalid_mode(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_mode.yaml"
        with self.assertRaises(Exception):
            eulexbuild.validate_configuration(test_file)

    def test_invalid_missing_celex_ids(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_missing_celex_ids.yaml"
        with self.assertRaises(Exception):
            eulexbuild.validate_configuration(test_file)

    def test_invalid_empty_celex_ids(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_empty_celex_ids.yaml"
        with self.assertRaises(Exception):
            eulexbuild.validate_configuration(test_file)

    def test_invalid_missing_start_date(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_missing_start_date.yaml"
        with self.assertRaises(Exception):
            eulexbuild.validate_configuration(test_file)

    def test_invalid_missing_end_date(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_missing_end_date.yaml"
        with self.assertRaises(Exception):
            eulexbuild.validate_configuration(test_file)

    def test_invalid_start_after_end(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_start_after_end.yaml"
        with self.assertRaises(ValueError):
            eulexbuild.validate_configuration(test_file)

    def test_invalid_future_start_date(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_future_start_date.yaml"
        with self.assertRaises(Exception):
            eulexbuild.validate_configuration(test_file)

    def test_invalid_future_end_date(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_future_end_date.yaml"
        with self.assertRaises(Exception):
            eulexbuild.validate_configuration(test_file)

    def test_invalid_document_types(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_document_types.yaml"
        with self.assertRaises(Exception):
            eulexbuild.validate_configuration(test_file)

    def test_invalid_negative_max_threads(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_negative_max_threads.yaml"
        with self.assertRaises(Exception):
            eulexbuild.validate_configuration(test_file)

    def test_invalid_zero_max_threads(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_zero_max_threads.yaml"
        with self.assertRaises(Exception):
            eulexbuild.validate_configuration(test_file)

    def test_invalid_output_formats(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_output_formats.yaml"
        with self.assertRaises(Exception):
            eulexbuild.validate_configuration(test_file)

    def test_invalid_celex_format(self):
        test_file = Path(__file__).parent / "test_configurations" / "invalid_celex_format.yaml"
        with self.assertRaises(ValueError) as context:
            eulexbuild.validate_configuration(test_file)
        self.assertIn("Invalid CELEX ID format", str(context.exception))

    def test_valid_celex_normalization(self):
        test_file = Path(__file__).parent / "test_configurations" / "valid_celex_normalization.yaml"
        config = eulexbuild.validate_configuration(test_file)

        # Data - verify all CELEX IDs are valid after normalization
        self.assertEqual(config.data.mode, "fixed")
        self.assertEqual(len(config.data.celex_ids), 5)
        self.assertIn("32024R1689", config.data.celex_ids)
        self.assertIn("32008R0765", config.data.celex_ids)
        self.assertIn("32019L0790", config.data.celex_ids)
        self.assertIn("31992L0043", config.data.celex_ids)
        self.assertIn("32016R0679", config.data.celex_ids)
