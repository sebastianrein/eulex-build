import os
from datetime import date
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
import yaml

from eulexbuild.EULEXBuildPipeline import EULEXBuildPipeline
from eulexbuild.storage.models import Work, TextUnit, Relation


class TestEULEXBuildPipelineGetFixedData(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_config_path = Path(__file__).parent / "test_configurations" / "valid_fixed.yaml"
        cls.test_db_path = "test_pipeline_fixed_data.db"

        cls.expected_celex_ids = [
            "32024R1689",
            "32008R0765",
            "32019L0790",
            "32016R0679",
            "32022R2065",
            "32024D01459"
        ]

        cls.pipeline = EULEXBuildPipeline(cls.test_config_path, cls.test_db_path)
        cls.pipeline._get_data(cls.pipeline.config.data.celex_ids)

    def tearDown(self):
        """Clean up test artifacts."""
        if hasattr(self.pipeline, 'store') and hasattr(self.pipeline.store, 'session'):
            self.pipeline.store.session.close()

        if self.pipeline.output_dir.exists():
            for file in self.pipeline.output_dir.iterdir():
                file.unlink()
            self.pipeline.output_dir.rmdir()

    def test_get_fixed_data_processes_all_celex_ids(self):
        works = self.pipeline.store.session.query(Work).all()

        self.assertEqual(6, len(works))

        saved_celex_ids = [work.celex_id for work in works]
        self.assertEqual(set(saved_celex_ids), set(self.expected_celex_ids))

    def test_get_fixed_data_saves_correct_structure(self):
        works = self.pipeline.store.session.query(Work).all()

        for work in works:
            self.assertIsNotNone(work.celex_id)
            self.assertIn(work.celex_id, self.expected_celex_ids)

            self.assertIsInstance(work.title, str)
            self.assertGreater(len(work.title), 0)

            if work.date_adopted is not None:
                self.assertIsInstance(work.date_adopted, date)

            self.assertIsInstance(work.document_type, str)
            self.assertGreater(len(work.document_type), 0)

            self.assertEqual(work.language, "eng")

    def test_get_fixed_data_determines_correct_document_types(self):
        works = self.pipeline.store.session.query(Work).all()

        expected_types = {
            "32024R1689": "regulation",
            "32008R0765": "regulation",
            "32019L0790": "directive",
            "32016R0679": "regulation",
            "32022R2065": "regulation",
            "32024D01459": "decision"
        }

        for work in works:
            self.assertIsNotNone(work.document_type)
            self.assertIsInstance(work.document_type, str)
            # noinspection PyTypeChecker
            self.assertIn(expected_types[work.celex_id], work.document_type.lower())

    def test_get_fixed_data_handles_dates_correctly(self):
        works = self.pipeline.store.session.query(Work).all()

        expected_dates = {
            "32024R1689": date(2024, 6, 13),
            "32008R0765": date(2008, 7, 9),
            "32019L0790": date(2019, 4, 17),
            "32016R0679": date(2016, 4, 27),
            "32022R2065": date(2022, 10, 19),
            "32024D01459": date(2024, 1, 24)
        }

        for work in works:
            self.assertIsNotNone(work.date_adopted)
            self.assertIsInstance(work.date_adopted, date)
            # noinspection PyTypeChecker
            self.assertEqual(expected_dates[work.celex_id], work.date_adopted)

    def test_get_fixed_data_fetches_correct_titles(self):
        works = self.pipeline.store.session.query(Work).all()

        expected_titles = {
            "32024R1689": "Regulation (EU) 2024/1689 of the European Parliament and of the Council of 13 June 2024 laying down harmonised rules on artificial intelligence and amending Regulations (EC) No 300/2008, (EU) No 167/2013, (EU) No 168/2013, (EU) 2018/858, (EU) 2018/1139 and (EU) 2019/2144 and Directives 2014/90/EU, (EU) 2016/797 and (EU) 2020/1828 (Artificial Intelligence Act) (Text with EEA relevance)",
            "32008R0765": "Regulation (EC) No 765/2008 of the European Parliament and of the Council of 9 July 2008 setting out the requirements for accreditation and market surveillance relating to the marketing of products and repealing Regulation (EEC) No 339/93 (Text with EEA relevance)",
            "32019L0790": "Directive (EU) 2019/790 of the European Parliament and of the Council of 17 April 2019 on copyright and related rights in the Digital Single Market and amending Directives 96/9/EC and 2001/29/EC (Text with EEA relevance. )",
            "32016R0679": "Regulation (EU) 2016/679 of the European Parliament and of the Council of 27 April 2016 on the protection of natural persons with regard to the processing of personal data and on the free movement of such data, and repealing Directive 95/46/EC (General Data Protection Regulation) (Text with EEA relevance)",
            "32022R2065": "Regulation (EU) 2022/2065 of the European Parliament and of the Council of 19 October 2022 on a Single Market For Digital Services and amending Directive 2000/31/EC (Digital Services Act) (Text with EEA relevance)",
            "32024D01459": "Commission Decision of 24 January 2024 establishing the European Artificial Intelligence Office"
        }

        for work in works:
            self.assertIsNotNone(work.title)
            self.assertIsInstance(work.title, str)
            # noinspection PyTypeChecker
            self.assertEqual(expected_titles[work.celex_id].lower(), work.title.lower())

    def test_get_fixed_data_saves_text_units_for_all_celex_ids(self):
        for celex_id in self.expected_celex_ids:
            text_units = self.pipeline.store.session.query(TextUnit).filter(
                TextUnit.celex_id == celex_id
            ).all()

            self.assertGreater(len(text_units), 0, f"No text units found for {celex_id}")

    def test_get_fixed_data_text_units_linked_to_works(self):
        for celex_id in self.expected_celex_ids:
            work = self.pipeline.store.session.query(Work).filter(Work.celex_id == celex_id).first()

            self.assertIsNotNone(work.text_units)
            self.assertGreater(len(work.text_units), 0, f"No text units linked to work {celex_id}")

            for unit in work.text_units:
                self.assertEqual(unit.celex_id, work.celex_id)

    def test_get_fixed_data_ai_act_articles(self):
        """Test specific articles from the AI Act (32024R1689)"""
        celex_id = "32024R1689"

        # Article 1 - Subject matter
        article_1 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "article",
            TextUnit.number == "1"
        ).first()

        self.assertIsNotNone(article_1)
        self.assertIn("Subject matter", article_1.title)
        self.assertIn("harmonised rules", article_1.text.lower())
        self.assertIn("artificial intelligence", article_1.text.lower())

        # Article 6 - Classification rules for high-risk AI systems
        article_6 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "article",
            TextUnit.number == "6"
        ).first()

        self.assertIsNotNone(article_6)
        self.assertIn("high-risk", article_6.text.lower())

    def test_get_fixed_data_ai_act_recitals(self):
        """Test specific recitals from the AI Act (32024R1689)"""
        celex_id = "32024R1689"

        recital_1 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "recital",
            TextUnit.number == "1"
        ).first()

        self.assertIsNotNone(recital_1)
        self.assertIn("artificial intelligence", recital_1.text.lower())

    def test_get_fixed_data_ai_act_annexes(self):
        """Test annexes from the AI Act (32024R1689)"""
        celex_id = "32024R1689"

        annexes = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "annex"
        ).all()

        self.assertGreater(len(annexes), 0, "AI Act should have annexes")

        annex_numbers = [annex.number for annex in annexes]
        self.assertIn("I", annex_numbers)

    def test_get_fixed_data_gdpr_articles(self):
        """Test specific articles from GDPR (32016R0679)"""
        celex_id = "32016R0679"

        # Article 1 - Subject-matter and objectives
        article_1 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "article",
            TextUnit.number == "1"
        ).first()

        self.assertIsNotNone(article_1)
        self.assertIn("Subject-matter and objectives", article_1.title)
        self.assertIn("personal data", article_1.text.lower())

        # Article 17 - Right to erasure
        article_17 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "article",
            TextUnit.number == "17"
        ).first()

        self.assertIsNotNone(article_17)
        self.assertIn("erasure", article_17.text.lower())

    def test_get_fixed_data_gdpr_recitals(self):
        """Test specific recitals from GDPR (32016R0679)"""
        celex_id = "32016R0679"

        recital_1 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "recital",
            TextUnit.number == "1"
        ).first()

        self.assertIsNotNone(recital_1)
        self.assertIn("personal data", recital_1.text.lower())

    def test_get_fixed_data_dsa_articles(self):
        """Test specific articles from Digital Services Act (32022R2065)"""
        celex_id = "32022R2065"

        # Article 1 - Subject matter
        article_1 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "article",
            TextUnit.number == "1"
        ).first()

        self.assertIsNotNone(article_1)
        self.assertIn("Subject matter", article_1.title)
        self.assertIn("intermediary services", article_1.text.lower())

    def test_get_fixed_data_dsa_recitals(self):
        """Test specific recitals from Digital Services Act (32022R2065)"""
        celex_id = "32022R2065"

        recital_1 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "recital",
            TextUnit.number == "1"
        ).first()

        self.assertIsNotNone(recital_1)
        self.assertIn("digital", recital_1.text.lower())

    def test_get_fixed_data_copyright_directive_articles(self):
        """Test specific articles from Copyright Directive (32019L0790)"""
        celex_id = "32019L0790"

        # Article 1 - Subject matter and scope
        article_1 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "article",
            TextUnit.number == "1"
        ).first()

        self.assertIsNotNone(article_1)
        self.assertIn("Subject matter and scope", article_1.title)
        self.assertIn("copyright", article_1.text.lower())

    def test_get_fixed_data_copyright_directive_recitals(self):
        """Test specific recitals from Copyright Directive (32019L0790)"""
        celex_id = "32019L0790"

        recital_1 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "recital",
            TextUnit.number == "1"
        ).first()

        self.assertIsNotNone(recital_1)
        # Copyright directive recitals discuss internal market
        self.assertTrue(
            "copyright" in recital_1.text.lower() or "internal market" in recital_1.text.lower()
        )

    def test_get_fixed_data_accreditation_regulation_articles(self):
        """Test specific articles from Accreditation Regulation (32008R0765)"""
        celex_id = "32008R0765"

        # Article 1 - Subject matter and scope
        article_1 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "article",
            TextUnit.number == "1"
        ).first()

        self.assertIsNotNone(article_1)
        self.assertIn("Subject matter and scope", article_1.title)
        self.assertIn("accreditation", article_1.text.lower())

    def test_get_fixed_data_accreditation_regulation_recitals(self):
        """Test specific recitals from Accreditation Regulation (32008R0765)"""
        celex_id = "32008R0765"

        recital_1 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "recital",
            TextUnit.number == "1"
        ).first()

        self.assertIsNotNone(recital_1)
        self.assertIn("market", recital_1.text.lower())

    def test_get_fixed_data_ai_office_decision_articles(self):
        """Test specific articles from AI Office Decision (32024D01459)"""
        celex_id = "32024D01459"

        # Article 1 - Establishment
        article_1 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "article",
            TextUnit.number == "1"
        ).first()

        self.assertIsNotNone(article_1)
        self.assertIn("artificial intelligence", article_1.text.lower())

    def test_get_fixed_data_ai_office_decision_recitals(self):
        """Test specific recitals from AI Office Decision (32024D01459)"""
        celex_id = "32024D01459"

        recital_1 = self.pipeline.store.session.query(TextUnit).filter(
            TextUnit.celex_id == celex_id,
            TextUnit.type == "recital",
            TextUnit.number == "1"
        ).first()

        self.assertIsNotNone(recital_1)
        self.assertIn("artificial intelligence", recital_1.text.lower())

    # ==================== RELATION TESTS ====================

    def test_get_fixed_data_ai_act_relations(self):
        """Test relations from the AI Act (32024R1689)"""
        celex_id = "32024R1689"

        relations = self.pipeline.store.session.query(Relation).filter(
            Relation.celex_source == celex_id
        ).all()

        target_celex_ids = [r.celex_target for r in relations]

        expected_targets = [
            "32008R0300",  # Regulation (EC) No 300/2008
            "32013R0167",  # Regulation (EU) No 167/2013
            "32013R0168",  # Regulation (EU) No 168/2013
            "32018R0858",  # Regulation (EU) 2018/858
            "32018R1139",  # Regulation (EU) 2018/1139
            "32019R2144",  # Regulation (EU) 2019/2144
            "32014R0090",  # Directive 2014/90/EU
            "32016L0797",  # Directive (EU) 2016/797
            "32020L1828",  # Directive (EU) 2020/1828
            "32016Q0512(01)",
        ]

        for expected in expected_targets:
            self.assertIn(
                expected, target_celex_ids,
                f"AI Act should reference {expected}. Found targets: {target_celex_ids}"
            )

    def test_get_fixed_data_gdpr_relations(self):
        """Test relations from GDPR (32016R0679)"""
        celex_id = "32016R0679"

        relations = self.pipeline.store.session.query(Relation).filter(
            Relation.celex_source == celex_id
        ).all()

        target_celex_ids = [r.celex_target for r in relations]

        expected_targets = [
            "32002L0058",  # Directive 2002/58/EC - ePrivacy Directive
            "32016L0680",  # Regulation (EU) 2016/680 - Law Enforcement Directive
            "32000L0031",  # Directive 2000/31/EC - E-Commerce Directive
        ]

        for expected in expected_targets:
            self.assertIn(
                expected, target_celex_ids,
                f"GDPR should reference {expected}. Found targets: {target_celex_ids}"
            )

    def test_get_fixed_data_dsa_relations(self):
        """Test relations from Digital Services Act (32022R2065)"""
        celex_id = "32022R2065"

        relations = self.pipeline.store.session.query(Relation).filter(
            Relation.celex_source == celex_id
        ).all()

        target_celex_ids = [r.celex_target for r in relations]

        expected_target = "32000L0031"  # Directive 2000/31/EC

        self.assertIn(
            expected_target, target_celex_ids,
            f"DSA should reference Directive 2000/31/EC. Found targets: {target_celex_ids}"
        )

    def test_get_fixed_data_copyright_directive_relations(self):
        """Test relations from Copyright Directive (32019L0790)"""
        celex_id = "32019L0790"

        relations = self.pipeline.store.session.query(Relation).filter(
            Relation.celex_source == celex_id
        ).all()

        target_celex_ids = [r.celex_target for r in relations]

        expected_targets = [
            "31996L0009",  # Directive 96/9/EC
            "32001L0029",  # Directive 2001/29/EC
        ]

        for expected in expected_targets:
            self.assertIn(
                expected, target_celex_ids,
                f"Copyright Directive should reference {expected}. Found targets: {target_celex_ids}"
            )

    def test_get_fixed_data_accreditation_regulation_relations(self):
        """Test relations from Accreditation Regulation (32008R0765)"""
        celex_id = "32008R0765"

        relations = self.pipeline.store.session.query(Relation).filter(
            Relation.celex_source == celex_id
        ).all()

        target_celex_ids = [r.celex_target for r in relations]

        expected_targets = [
            "31995L0046",  # Directive 95/46/EC - Data Protection Directive
            "32001L0095",  # Directive 2001/95/EC - General Product Safety Directive
            "31998L0034",  # Directive 98/34/EC - Technical Standards Directive
        ]

        for expected in expected_targets:
            self.assertIn(
                expected, target_celex_ids,
                f"Accreditation Regulation should reference {expected}. Found targets: {target_celex_ids}"
            )

    def test_get_fixed_data_ai_office_decision_relations(self):
        """Test relations from AI Office Decision (32024D01459)"""
        celex_id = "32024D01459"

        relations = self.pipeline.store.session.query(Relation).filter(
            Relation.celex_source == celex_id
        ).all()

        target_celex_ids = [r.celex_target for r in relations]

        expected_targets = [
            "32018R1046",
            "52021DC0205",
            "32022R1925",
        ]

        for expected in expected_targets:
            self.assertIn(
                expected, target_celex_ids,
                f"AI Office Decision should reference {expected}. Found targets: {target_celex_ids}"
            )


class TestEULEXBuildPipelineEndToEnd(TestCase):
    def tearDown(self):
        """Clean up test artifacts."""
        if hasattr(self.pipeline, 'store') and hasattr(self.pipeline.store, 'session'):
            self.pipeline.store.session.close()

        if self.pipeline.output_dir.exists():
            for file in self.pipeline.output_dir.iterdir():
                file.unlink()
            self.pipeline.output_dir.rmdir()

        import logging
        logger = logging.getLogger('eulexbuild_pipeline')
        handlers = logger.handlers[:]
        for handler in handlers:
            try:
                handler.close()
            except:
                pass
            logger.removeHandler(handler)

    def test_with_valid_fixed(self):
        test_config_path = Path(__file__).parent / "test_configurations" / "valid_fixed.yaml"
        self.test_db_path = "test_pipeline_end_to_end.db"

        self.pipeline = EULEXBuildPipeline(test_config_path, self.test_db_path)
        self.pipeline.run()

        # Verify output files
        output_dir = self.pipeline.output_dir
        expected_files = [
            output_dir / "works.csv",
            output_dir / "text_units.csv",
            output_dir / "relations.csv",
            output_dir / "works.parquet",
            output_dir / "text_units.parquet",
            output_dir / "relations.parquet",
        ]

        for file_path in expected_files:
            self.assertTrue(file_path.exists(), f"Expected output file {file_path} does not exist.")

        # Load expected celex_ids from configuration file
        with open(test_config_path, 'r') as f:
            config = yaml.safe_load(f)
        expected_celex_ids = set(config['data']['celex_ids'])

        # Verify CSV files contain the expected celex_ids
        works_csv = pd.read_csv(output_dir / "works.csv")
        works_csv_celex_ids = set(works_csv['celex_id'].tolist())
        self.assertEqual(expected_celex_ids, works_csv_celex_ids,
                         f"Works CSV celex_ids mismatch. Expected: {expected_celex_ids}, Got: {works_csv_celex_ids}")

        text_units_csv = pd.read_csv(output_dir / "text_units.csv")
        text_units_csv_celex_ids = set(text_units_csv['celex_id'].tolist())
        self.assertEqual(expected_celex_ids, text_units_csv_celex_ids,
                         f"Text units CSV celex_ids mismatch. Expected: {expected_celex_ids}, Got: {text_units_csv_celex_ids}")

        relations_csv = pd.read_csv(output_dir / "relations.csv")
        relations_csv_source_celex_ids = set(relations_csv['celex_source'].tolist())
        self.assertEqual(expected_celex_ids, relations_csv_source_celex_ids,
                         f"Relations CSV source celex_ids mismatch. Expected: {expected_celex_ids}, Got: {relations_csv_source_celex_ids}")

        # Verify Parquet files contain the expected celex_ids
        works_parquet = pd.read_parquet(output_dir / "works.parquet")
        works_parquet_celex_ids = set(works_parquet['celex_id'].tolist())
        self.assertEqual(expected_celex_ids, works_parquet_celex_ids,
                         f"Works Parquet celex_ids mismatch. Expected: {expected_celex_ids}, Got: {works_parquet_celex_ids}")

        text_units_parquet = pd.read_parquet(output_dir / "text_units.parquet")
        text_units_parquet_celex_ids = set(text_units_parquet['celex_id'].tolist())
        self.assertEqual(expected_celex_ids, text_units_parquet_celex_ids,
                         f"Text units Parquet celex_ids mismatch. Expected: {expected_celex_ids}, Got: {text_units_parquet_celex_ids}")

        relations_parquet = pd.read_parquet(output_dir / "relations.parquet")
        relations_parquet_source_celex_ids = set(relations_parquet['celex_source'].tolist())
        self.assertEqual(expected_celex_ids, relations_parquet_source_celex_ids,
                         f"Relations Parquet source celex_ids mismatch. Expected: {expected_celex_ids}, Got: {relations_parquet_source_celex_ids}")

    def test_with_valid_procedure_numbers(self):
        test_config_path = Path(__file__).parent / "test_configurations" / "valid_procedure_numbers.yaml"
        self.test_db_path = "test_pipeline_procedure_numbers.db"

        self.pipeline = EULEXBuildPipeline(test_config_path, self.test_db_path)
        self.pipeline.run()

        # Verify output files
        output_dir = self.pipeline.output_dir
        expected_files = [
            output_dir / "works.csv",
            output_dir / "text_units.csv",
            output_dir / "relations.csv",
            output_dir / "works.parquet",
            output_dir / "text_units.parquet",
            output_dir / "relations.parquet",
        ]

        for file_path in expected_files:
            self.assertTrue(file_path.exists(), f"Expected output file {file_path} does not exist.")

        # Expected CELEX IDs resolved from procedure numbers:
        # 2023/0202(COD) -> 32025R2518
        # 2023/0323/COD -> 52023PC0533
        expected_celex_ids = {"32025R2518", "52023PC0533", "32024R1689", "32008R0765"}

        # Verify CSV files contain the expected celex_ids
        works_csv = pd.read_csv(output_dir / "works.csv")
        works_csv_celex_ids = set(works_csv['celex_id'].tolist())
        self.assertEqual(expected_celex_ids, works_csv_celex_ids,
                         f"Works CSV celex_ids mismatch. Expected: {expected_celex_ids}, Got: {works_csv_celex_ids}")

        text_units_csv = pd.read_csv(output_dir / "text_units.csv")
        text_units_csv_celex_ids = set(text_units_csv['celex_id'].tolist())
        self.assertEqual(expected_celex_ids, text_units_csv_celex_ids,
                         f"Text units CSV celex_ids mismatch. Expected: {expected_celex_ids}, Got: {text_units_csv_celex_ids}")

        relations_csv = pd.read_csv(output_dir / "relations.csv")
        relations_csv_source_celex_ids = set(relations_csv['celex_source'].tolist())
        self.assertEqual(expected_celex_ids, relations_csv_source_celex_ids,
                         f"Relations CSV source celex_ids mismatch. Expected: {expected_celex_ids}, Got: {relations_csv_source_celex_ids}")

        # Verify Parquet files contain the expected celex_ids
        works_parquet = pd.read_parquet(output_dir / "works.parquet")
        works_parquet_celex_ids = set(works_parquet['celex_id'].tolist())
        self.assertEqual(expected_celex_ids, works_parquet_celex_ids,
                         f"Works Parquet celex_ids mismatch. Expected: {expected_celex_ids}, Got: {works_parquet_celex_ids}")

        text_units_parquet = pd.read_parquet(output_dir / "text_units.parquet")
        text_units_parquet_celex_ids = set(text_units_parquet['celex_id'].tolist())
        self.assertEqual(expected_celex_ids, text_units_parquet_celex_ids,
                         f"Text units Parquet celex_ids mismatch. Expected: {expected_celex_ids}, Got: {text_units_parquet_celex_ids}")

        relations_parquet = pd.read_parquet(output_dir / "relations.parquet")
        relations_parquet_source_celex_ids = set(relations_parquet['celex_source'].tolist())
        self.assertEqual(expected_celex_ids, relations_parquet_source_celex_ids,
                         f"Relations Parquet source celex_ids mismatch. Expected: {expected_celex_ids}, Got: {relations_parquet_source_celex_ids}")

        # Verify document types are correctly determined
        # 32025R2518 is a regulation (R)
        # 52023PC0533 is a proposal (PC)
        expected_types = {
            "32025R2518": "regulation",
            "52023PC0533": "proposal",
            "32024R1689": "regulation",
            "32008R0765": "regulation",
        }

        for _, work_row in works_csv.iterrows():
            celex_id = work_row['celex_id']
            doc_type = work_row['document_type']
            self.assertIn(expected_types[celex_id], doc_type.lower(),
                          f"Document type mismatch for {celex_id}. Expected to contain '{expected_types[celex_id]}', got '{doc_type}'")

    @patch('builtins.input', return_value='')
    def test_with_valid_descriptive(self, mock_input):
        test_config_path = Path(__file__).parent / "test_configurations" / "valid_simple_descriptive.yaml"
        test_db_path = "test_pipeline_end_to_end.db"

        self.pipeline = EULEXBuildPipeline(test_config_path, test_db_path)
        self.pipeline.run()

        # Verify that user input was requested (interactive mode)
        mock_input.assert_called_once()

        # Verify output directory exists
        output_dir = self.pipeline.output_dir
        self.assertTrue(output_dir.exists(), f"Output directory {output_dir} does not exist.")

        # Verify log file is created
        log_file = output_dir / "pipeline.log"
        self.assertTrue(log_file.exists(), f"Log file {log_file} does not exist.")
        # Verify log file has content
        with open(log_file, 'r', encoding='utf-8') as f:
            log_content = f.read()
        self.assertGreater(len(log_content), 0, "Log file is empty.")
        self.assertIn("Starting EULEX-Build Pipeline", log_content, "Log file missing expected start message.")
        self.assertIn("Pipeline completed successfully", log_content, "Log file missing expected completion message.")

        # Verify README file is created
        readme_file = output_dir / "README.md"
        self.assertTrue(readme_file.exists(), f"README file {readme_file} does not exist.")
        # Verify README file has content
        with open(readme_file, 'r', encoding='utf-8') as f:
            readme_content = f.read()
        self.assertGreater(len(readme_content), 0, "README file is empty.")
        self.assertIn("EULEX-Build Pipeline Output", readme_content, "README file missing expected title.")
        self.assertIn("EU Climate Dataset", readme_content, "README file missing project name from config.")
        self.assertIn("Jane Smith", readme_content, "README file missing author from config.")

        # Verify expected output files based on configuration
        with open(test_config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Check for parquet files (as specified in config)
        expected_files = [
            output_dir / "works.parquet",
            output_dir / "text_units.parquet",
            output_dir / "relations.parquet",
        ]

        for file_path in expected_files:
            self.assertTrue(file_path.exists(), f"Expected output file {file_path} does not exist.")

        # Verify the data files contain actual data
        works_parquet = pd.read_parquet(output_dir / "works.parquet")
        self.assertGreater(len(works_parquet), 0, "Works parquet file is empty.")

        text_units_parquet = pd.read_parquet(output_dir / "text_units.parquet")
        self.assertGreater(len(text_units_parquet), 0, "Text units parquet file is empty.")

        relations_parquet = pd.read_parquet(output_dir / "relations.parquet")
        self.assertGreater(len(relations_parquet), 0, "Relations parquet file is empty.")

        # Verify document types match configuration
        document_types_in_data = set(works_parquet['document_type'].unique())
        expected_document_types = set(config['data']['document_types'])
        self.assertTrue(document_types_in_data.issubset(expected_document_types),
                        f"Found unexpected document types. Expected subset of {expected_document_types}, got {document_types_in_data}")

        # Verify EuroVoc labels file was created (for descriptive mode with filter_keywords)
        eurovoc_file = output_dir / "eurovoc_labels.yaml"
        if config['data'].get('filter_keywords'):
            self.assertTrue(eurovoc_file.exists(), f"EuroVoc labels file {eurovoc_file} does not exist.")
            # Verify EuroVoc file has content
            with open(eurovoc_file, 'r', encoding='utf-8') as f:
                eurovoc_content = yaml.safe_load(f)
            self.assertIn('labels', eurovoc_content, "EuroVoc file missing 'labels' section.")
            self.assertIn('instructions', eurovoc_content, "EuroVoc file missing 'instructions' section.")


class TestReviewEurovocLabels(TestCase):
    """Test the _review_eurovoc_labels method of EULEXBuildPipeline."""

    def setUp(self):
        """Set up common test fixtures."""
        self.test_config_path = Path(__file__).parent / "test_configurations" / "valid_descriptive.yaml"
        self.test_db_path = "test_review_eurovoc.db"

        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)

        self.pipeline = EULEXBuildPipeline(self.test_config_path, self.test_db_path)
        # Clean up any review files
        review_file = self.pipeline.output_dir / "eurovoc_labels.yaml"
        if review_file.exists():
            review_file.unlink()

        # Sample EuroVoc labels for testing (keyword -> concept_uri -> set(labels))
        self.sample_labels = {
            "climate": {
                "http://eurovoc.europa.eu/2585": ["climate change"],
                "http://eurovoc.europa.eu/3249": ["climate policy"],
                "http://eurovoc.europa.eu/1042": ["climatic conditions"]
            },
            "digital": {
                "http://eurovoc.europa.eu/8468": ["digital economy"],
                "http://eurovoc.europa.eu/8469": ["digital single market"],
                "http://eurovoc.europa.eu/8470": ["digitalisation"]
            }
        }
        self.sample_labels_returned = {
            'http://eurovoc.europa.eu/2585',
            'http://eurovoc.europa.eu/3249',
            'http://eurovoc.europa.eu/1042',
            'http://eurovoc.europa.eu/8468',
            'http://eurovoc.europa.eu/8469',
            'http://eurovoc.europa.eu/8470'
        }

    def tearDown(self):
        """Clean up test artifacts."""
        if hasattr(self.pipeline, 'store') and hasattr(self.pipeline.store, 'session'):
            self.pipeline.store.session.close()

        if self.pipeline.output_dir.exists():
            for file in self.pipeline.output_dir.iterdir():
                file.unlink()
            self.pipeline.output_dir.rmdir()

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_creates_file(self, mock_input, mock_get_eurovoc):
        """Test that _review_eurovoc_labels creates a YAML file with EuroVoc labels."""
        mock_get_eurovoc.return_value = self.sample_labels

        result = self.pipeline._review_eurovoc_labels()

        # Verify file was created
        review_file = self.pipeline.output_dir / "eurovoc_labels.yaml"
        self.assertTrue(review_file.exists(), "Review file should be created")

        # Verify file contents
        with open(review_file, 'r', encoding='utf-8') as f:
            file_contents = yaml.safe_load(f)

        self.assertIn('instructions', file_contents)
        self.assertIn('labels', file_contents)
        self.assertEqual(file_contents['labels'], self.sample_labels)

        # Verify return value
        self.assertEqual(result, self.sample_labels_returned)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_waits_for_user_input(self, mock_input, mock_get_eurovoc):
        """Test that _review_eurovoc_labels waits for user to press Enter."""
        mock_get_eurovoc.return_value = self.sample_labels

        self.pipeline._review_eurovoc_labels()

        # Verify input was called (user interaction)
        mock_input.assert_called_once()

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_loads_modified_labels(self, mock_input, mock_get_eurovoc):
        """Test that _review_eurovoc_labels loads user-modified labels."""
        mock_get_eurovoc.return_value = self.sample_labels

        # Modify the labels after file creation but before input
        def simulate_user_edit(*args, **kwargs):
            review_file = self.pipeline.output_dir / "eurovoc_labels.yaml"
            modified_labels = {
                "climate": {
                    "http://eurovoc.europa.eu/2585": ["climate change"],
                    # Removed other climate labels
                },
                "digital": {
                    "http://eurovoc.europa.eu/8468": ["digital economy"],
                    "http://eurovoc.europa.eu/9999": ["new label"]  # Added new label
                }
            }
            with open(review_file, 'w', encoding='utf-8') as f:
                yaml.dump({'instructions': 'Review...', 'labels': modified_labels}, f)
            return ''

        mock_input.side_effect = simulate_user_edit

        result = self.pipeline._review_eurovoc_labels()

        # Verify modified labels were loaded
        self.assertEqual(len(result), 3)
        self.assertIn('http://eurovoc.europa.eu/2585', result)
        self.assertIn('http://eurovoc.europa.eu/8468', result)
        self.assertIn('http://eurovoc.europa.eu/9999', result)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_handles_removed_keywords(self, mock_input, mock_get_eurovoc):
        """Test that user can remove entire keyword sections."""
        mock_get_eurovoc.return_value = self.sample_labels

        def remove_keyword_section(*args, **kwargs):
            review_file = self.pipeline.output_dir / "eurovoc_labels.yaml"
            sample_labels = self.sample_labels
            modified_labels = {
                "climate": sample_labels["climate"]
                # Removed "digital" keyword entirely
            }
            with open(review_file, 'w', encoding='utf-8') as f:
                yaml.dump({'instructions': 'Review...', 'labels': modified_labels}, f)
            return ''

        mock_input.side_effect = remove_keyword_section

        result = self.pipeline._review_eurovoc_labels()

        # Verify only climate labels remain
        self.assertIn('http://eurovoc.europa.eu/2585', result)
        self.assertNotIn('http://eurovoc.europa.eu/8468', result)
        self.assertEqual(3, len(result))

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_handles_empty_labels(self, mock_input, mock_get_eurovoc):
        """Test that _review_eurovoc_labels handles case with no initial labels."""
        mock_get_eurovoc.return_value = {}

        result = self.pipeline._review_eurovoc_labels()

        # Verify empty dict is handled correctly
        self.assertEqual(set(), result)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_handles_user_added_keywords(self, mock_input, mock_get_eurovoc):
        """Test that user can add new keywords not in original fetch."""
        mock_get_eurovoc.return_value = self.sample_labels

        def add_new_keyword(*args, **kwargs):
            review_file = self.pipeline.output_dir / "eurovoc_labels.yaml"
            modified_labels = self.sample_labels.copy()
            modified_labels["environment"] = {
                "http://eurovoc.europa.eu/2825": ["environmental protection"],
                "http://eurovoc.europa.eu/2826": ["environmental policy"]
            }
            with open(review_file, 'w', encoding='utf-8') as f:
                yaml.dump({'instructions': 'Review...', 'labels': modified_labels}, f)
            return ''

        mock_input.side_effect = add_new_keyword

        result = self.pipeline._review_eurovoc_labels()

        # Verify new keyword section was added
        self.assertIn('http://eurovoc.europa.eu/2825', result)
        self.assertIn('http://eurovoc.europa.eu/2826', result)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_logs_correctly(self, mock_input, mock_get_eurovoc):
        """Test that _review_eurovoc_labels logs appropriate messages."""
        mock_get_eurovoc.return_value = self.sample_labels

        with self.assertLogs('eulexbuild_pipeline', level='DEBUG') as log_context:
            self.pipeline._review_eurovoc_labels()

        # Verify expected log messages
        log_output = ' '.join(log_context.output)
        self.assertIn('Fetching EuroVoc labels', log_output)
        self.assertIn('keywords', log_output)
        self.assertIn('Saving', log_output)
        self.assertIn('EuroVoc labels', log_output)
        self.assertIn('REVIEW REQUIRED', log_output)
        self.assertIn('Loaded', log_output)
        self.assertIn('reviewed EuroVoc labels', log_output)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_file_path_correct(self, mock_input, mock_get_eurovoc):
        """Test that the review file is created in the correct output directory."""
        mock_get_eurovoc.return_value = self.sample_labels

        self.pipeline._review_eurovoc_labels()

        # Verify file path
        expected_path = self.pipeline.output_dir / "eurovoc_labels.yaml"
        self.assertTrue(expected_path.exists())
        self.assertEqual(expected_path.parent, self.pipeline.output_dir)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_yaml_format(self, mock_input, mock_get_eurovoc):
        """Test that the YAML file is properly formatted."""
        mock_get_eurovoc.return_value = self.sample_labels

        self.pipeline._review_eurovoc_labels()

        review_file = self.pipeline.output_dir / "eurovoc_labels.yaml"

        # Verify YAML is valid and well-formed
        with open(review_file, 'r', encoding='utf-8') as f:
            contents = yaml.safe_load(f)

        self.assertIsInstance(contents, dict)
        self.assertIn('instructions', contents)
        self.assertIn('labels', contents)
        self.assertIsInstance(contents['instructions'], str)
        self.assertIsInstance(contents['labels'], dict)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_instructions_present(self, mock_input, mock_get_eurovoc):
        """Test that instructions are included in the review file."""
        mock_get_eurovoc.return_value = self.sample_labels

        self.pipeline._review_eurovoc_labels()

        review_file = self.pipeline.output_dir / "eurovoc_labels.yaml"
        with open(review_file, 'r', encoding='utf-8') as f:
            contents = yaml.safe_load(f)

        instructions = contents['instructions']
        self.assertIn('Review', instructions)
        self.assertIn('EuroVoc labels', instructions)
        self.assertGreater(len(instructions), 10)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_handles_malformed_yaml(self, mock_input, mock_get_eurovoc):
        """Test error handling when user creates malformed YAML."""
        mock_get_eurovoc.return_value = self.sample_labels

        def create_malformed_yaml(*args, **kwargs):
            review_file = self.pipeline.output_dir / "eurovoc_labels.yaml"
            # Write malformed YAML
            with open(review_file, 'w', encoding='utf-8') as f:
                f.write("labels: [\n  invalid yaml structure")
            return ''

        mock_input.side_effect = create_malformed_yaml

        # Should raise an error when trying to load malformed YAML
        with self.assertRaises(yaml.YAMLError):
            self.pipeline._review_eurovoc_labels()

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_handles_missing_labels_key(self, mock_input, mock_get_eurovoc):
        """Test handling when user removes the 'labels' key."""
        mock_get_eurovoc.return_value = self.sample_labels

        def remove_labels_key(*args, **kwargs):
            review_file = self.pipeline.output_dir / "eurovoc_labels.yaml"
            with open(review_file, 'w', encoding='utf-8') as f:
                yaml.dump({'instructions': 'Review...'}, f)  # No 'labels' key
            return ''

        mock_input.side_effect = remove_labels_key

        result = self.pipeline._review_eurovoc_labels()

        # Should return empty dict when labels key is missing
        self.assertEqual(set(), result)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_handles_all_labels_removed(self, mock_input, mock_get_eurovoc):
        """Test when user removes all labels from all keywords."""
        mock_get_eurovoc.return_value = self.sample_labels

        def remove_all_labels(*args, **kwargs):
            review_file = self.pipeline.output_dir / "eurovoc_labels.yaml"
            with open(review_file, 'w', encoding='utf-8') as f:
                yaml.dump({'instructions': 'Review...', 'labels': {}}, f)
            return ''

        mock_input.side_effect = remove_all_labels

        result = self.pipeline._review_eurovoc_labels()

        # Should handle empty labels gracefully
        self.assertEqual(set(), result)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    @patch('builtins.input', return_value='')
    def test_review_eurovoc_labels_count_matches(self, mock_input, mock_get_eurovoc):
        """Test that logged counts match actual label counts."""
        mock_get_eurovoc.return_value = self.sample_labels

        with self.assertLogs('eulexbuild_pipeline', level='DEBUG') as log_context:
            result = self.pipeline._review_eurovoc_labels()

        # Calculate expected count (count concept URIs, not labels)
        total_concepts = sum(len(concepts) for concepts in self.sample_labels.values())

        log_output = ' '.join(log_context.output)
        self.assertIn(f'Saving {total_concepts}', log_output)
        self.assertIn(f'Loaded {len(result)}', log_output)


class TestReviewEurovocLabelsAutomatedMode(TestCase):
    """Test the _review_eurovoc_labels method in automated mode for CI/CD environments."""

    def setUp(self):
        """Set up common test fixtures."""
        self.test_config_path = Path(__file__).parent / "test_configurations" / "valid_descriptive.yaml"
        self.test_db_name = "test_review_eurovoc_automated.db"

        test_db_path = Path(__file__).parent / "descriptive_output" / self.test_db_name
        if os.path.exists(test_db_path):
            os.remove(test_db_path)

        self.pipeline = EULEXBuildPipeline(self.test_config_path, self.test_db_name)
        # Enable automated mode
        self.pipeline.config.processing.automated_mode = True

        # Clean up any review files
        review_file = self.pipeline.output_dir / "eurovoc_labels.yaml"
        if review_file.exists():
            review_file.unlink()

        # Sample EuroVoc labels for testing (keyword -> concept_uri -> set(labels))
        self.sample_labels = {
            "climate": {
                "http://eurovoc.europa.eu/2585": ["climate change"],
                "http://eurovoc.europa.eu/3249": ["climate policy"],
                "http://eurovoc.europa.eu/1042": ["climatic conditions"]
            },
            "digital": {
                "http://eurovoc.europa.eu/8468": ["digital economy"],
                "http://eurovoc.europa.eu/8469": ["digital single market"],
                "http://eurovoc.europa.eu/8470": ["digitalisation"]
            }
        }
        self.sample_labels_returned = {
            'http://eurovoc.europa.eu/2585',
            'http://eurovoc.europa.eu/3249',
            'http://eurovoc.europa.eu/1042',
            'http://eurovoc.europa.eu/8468',
            'http://eurovoc.europa.eu/8469',
            'http://eurovoc.europa.eu/8470'
        }

    def tearDown(self):
        """Clean up test artifacts."""
        if hasattr(self.pipeline, 'store') and hasattr(self.pipeline.store, 'session'):
            self.pipeline.store.session.close()

        if self.pipeline.output_dir.exists():
            for file in self.pipeline.output_dir.iterdir():
                file.unlink()
            self.pipeline.output_dir.rmdir()

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    def test_automated_mode_skips_user_input(self, mock_get_eurovoc):
        """Test that automated mode skips waiting for user input."""
        mock_get_eurovoc.return_value = self.sample_labels

        # Should not call input() in automated mode
        with patch('builtins.input') as mock_input:
            result = self.pipeline._review_eurovoc_labels()
            mock_input.assert_not_called()

        # Verify all labels are returned
        self.assertEqual(result, self.sample_labels_returned)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    def test_automated_mode_creates_file(self, mock_get_eurovoc):
        """Test that automated mode still creates the review file for record keeping."""
        mock_get_eurovoc.return_value = self.sample_labels

        result = self.pipeline._review_eurovoc_labels()

        # Verify file was created even in automated mode
        review_file = self.pipeline.output_dir / "eurovoc_labels.yaml"
        self.assertTrue(review_file.exists(), "Review file should be created in automated mode")

        # Verify file contents
        with open(review_file, 'r', encoding='utf-8') as f:
            file_contents = yaml.safe_load(f)

        self.assertIn('instructions', file_contents)
        self.assertIn('labels', file_contents)
        self.assertEqual(file_contents['labels'], self.sample_labels)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    def test_automated_mode_returns_all_labels(self, mock_get_eurovoc):
        """Test that automated mode returns all fetched labels without modification."""
        mock_get_eurovoc.return_value = self.sample_labels

        result = self.pipeline._review_eurovoc_labels()

        # Verify all labels are present
        self.assertEqual(len(result), 6)
        self.assertEqual(result, self.sample_labels_returned)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    def test_automated_mode_logs_correctly(self, mock_get_eurovoc):
        """Test that automated mode logs appropriate messages."""
        mock_get_eurovoc.return_value = self.sample_labels

        with self.assertLogs('eulexbuild_pipeline', level='INFO') as log_context:
            self.pipeline._review_eurovoc_labels()

        log_output = ' '.join(log_context.output)
        self.assertIn('Automated mode enabled', log_output)
        self.assertIn('skipping interactive EuroVoc review', log_output)
        self.assertIn('Using all', log_output)
        self.assertIn('fetched EuroVoc labels automatically', log_output)

        # Should NOT contain interactive review messages
        self.assertNotIn('REVIEW REQUIRED', log_output)
        self.assertNotIn('Press Enter', log_output)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    def test_automated_mode_handles_empty_labels(self, mock_get_eurovoc):
        """Test that automated mode handles empty labels correctly."""
        mock_get_eurovoc.return_value = {}

        result = self.pipeline._review_eurovoc_labels()

        self.assertEqual(set(), result)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    def test_automated_mode_with_single_keyword(self, mock_get_eurovoc):
        """Test automated mode with only one keyword."""
        single_keyword_labels = {
            "climate": {
                "http://eurovoc.europa.eu/2585": ["climate change"]
            }
        }
        mock_get_eurovoc.return_value = single_keyword_labels

        result = self.pipeline._review_eurovoc_labels()

        self.assertEqual(len(result), 1)
        self.assertIn('http://eurovoc.europa.eu/2585', result)

    @patch('eulexbuild.EULEXBuildPipeline.get_eurovoc_labels_for_keywords')
    def test_automated_mode_execution_speed(self, mock_get_eurovoc):
        """Test that automated mode executes quickly without waiting for input."""
        import time
        mock_get_eurovoc.return_value = self.sample_labels

        start_time = time.time()
        self.pipeline._review_eurovoc_labels()
        execution_time = time.time() - start_time

        # Should complete in less than 1 second (no user interaction)
        self.assertLess(execution_time, 1.0, "Automated mode should execute quickly")
