import csv
import os
from datetime import date
from unittest import TestCase

import pyarrow.parquet as pq

from eulexbuild.storage.models import Work, TextUnit, Relation
from eulexbuild.storage.storageManager import create_store


class TestStorageManager(TestCase):
    work_data = {
        "celex_id": "32022R2065",
        "document_type": "regulation",
        "title": "Digital Services Act",
        "date_adopted": date(2022, 10, 19),
        "language": "en",
        "full_text_html": None
    }

    units = [
        {"celex_id": "32022R2065", "type": "article", "number": "1", "text": "This Regulation establishes..."},
        {"celex_id": "32022R2065", "type": "recital", "number": "12", "text": "Recognising the importance..."}
    ]

    relations = [
        {"celex_source": "32022R2065", "celex_target": "32010R1060", "relation_type": "repeals"}
    ]

    def setUp(self):
        self.store = create_store("sqlite:///test_db.db")
        self.session = self.store.session

    def tearDown(self):
        if os.path.exists("test_db.db"):
            os.remove("test_db.db")

    def test_store_manager(self):
        self.store.save_work(self.work_data)
        self.store.save_text_units(self.units)
        self.store.save_relations(self.relations)

        # Verify work data
        work = self.session.query(Work).filter_by(celex_id="32022R2065").first()
        self.assertIsNotNone(work)
        self.assertEqual(work.title, "Digital Services Act")
        self.assertEqual(work.document_type, "regulation")

        # Verify both text units fully
        units = self.session.query(TextUnit).filter_by(celex_id="32022R2065").all()
        self.assertEqual(len(units), 2)

        article = next((u for u in units if u.type == "article"), None)
        self.assertIsNotNone(article)
        self.assertEqual(article.number, "1")
        self.assertEqual(article.text, "This Regulation establishes...")

        recital = next((u for u in units if u.type == "recital"), None)
        self.assertIsNotNone(recital)
        self.assertEqual(recital.number, "12")
        self.assertEqual(recital.text, "Recognising the importance...")

        # Verify relations
        relations = self.session.query(Relation).filter_by(celex_source="32022R2065").all()
        self.assertEqual(len(relations), 1)
        self.assertEqual(relations[0].celex_target, "32010R1060")
        self.assertEqual(relations[0].relation_type, "repeals")

    def test_export_works_to_csv(self):
        # Save some test data
        self.store.save_work(self.work_data)
        self.store.save_text_units(self.units)
        self.store.save_relations(self.relations)

        # Export to CSV
        csv_file = "test_export_works.csv"
        result = self.store.export_works_to_csv(csv_file)

        # Verify the export was successful
        self.assertTrue(os.path.exists(csv_file))

        # Verify CSV content
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row['celex_id'], "32022R2065")
        self.assertEqual(row['document_type'], "regulation")
        self.assertEqual(row['title'], "Digital Services Act")
        self.assertEqual(row['language'], "en")

        # Cleanup
        if os.path.exists(csv_file):
            os.remove(csv_file)

    def test_export_text_units_to_csv(self):
        # Save some test data
        self.store.save_work(self.work_data)
        self.store.save_text_units(self.units)

        # Export to CSV
        csv_file = "test_export_units.csv"
        result = self.store.export_text_units_to_csv(csv_file)

        # Verify the export was successful
        self.assertTrue(os.path.exists(csv_file))

        # Verify CSV content
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        self.assertEqual(len(rows), 2)

        # Check article
        article_row = next((r for r in rows if r['type'] == 'article'), None)
        self.assertIsNotNone(article_row)
        self.assertEqual(article_row['celex_id'], "32022R2065")
        self.assertEqual(article_row['number'], "1")
        self.assertEqual(article_row['text'], "This Regulation establishes...")

        # Check recital
        recital_row = next((r for r in rows if r['type'] == 'recital'), None)
        self.assertIsNotNone(recital_row)
        self.assertEqual(recital_row['celex_id'], "32022R2065")
        self.assertEqual(recital_row['number'], "12")
        self.assertEqual(recital_row['text'], "Recognising the importance...")

        # Cleanup
        if os.path.exists(csv_file):
            os.remove(csv_file)

    def test_export_relations_to_csv(self):
        # Save some test data
        self.store.save_work(self.work_data)
        self.store.save_relations(self.relations)

        # Export to CSV
        csv_file = "test_export_relations.csv"
        result = self.store.export_relations_to_csv(csv_file)

        # Verify the export was successful
        self.assertTrue(os.path.exists(csv_file))

        # Verify CSV content
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row['celex_source'], "32022R2065")
        self.assertEqual(row['celex_target'], "32010R1060")
        self.assertEqual(row['relation_type'], "repeals")

        # Cleanup
        if os.path.exists(csv_file):
            os.remove(csv_file)

    def test_export_works_to_parquet(self):
        # Save some test data
        self.store.save_work(self.work_data)
        self.store.save_text_units(self.units)
        self.store.save_relations(self.relations)

        # Export to Parquet
        parquet_file = "test_export_works.parquet"
        result = self.store.export_works_to_parquet(parquet_file)

        # Verify the export was successful
        self.assertTrue(os.path.exists(parquet_file))

        # Verify Parquet content
        table = pq.read_table(parquet_file)
        self.assertEqual(len(table), 1)

        row = table.to_pydict()
        self.assertEqual(row['celex_id'][0], "32022R2065")
        self.assertEqual(row['document_type'][0], "regulation")
        self.assertEqual(row['title'][0], "Digital Services Act")
        self.assertEqual(row['language'][0], "en")

        # Cleanup
        if os.path.exists(parquet_file):
            os.remove(parquet_file)

    def test_export_text_units_to_parquet(self):
        # Save some test data
        self.store.save_work(self.work_data)
        self.store.save_text_units(self.units)

        # Export to Parquet
        parquet_file = "test_export_units.parquet"
        result = self.store.export_text_units_to_parquet(parquet_file)

        # Verify the export was successful
        self.assertTrue(os.path.exists(parquet_file))

        # Verify Parquet content
        table = pq.read_table(parquet_file)
        self.assertEqual(len(table), 2)

        # Check that both article and recital are present
        data = table.to_pydict()
        unit_types = set(data['type'])
        self.assertIn('article', unit_types)
        self.assertIn('recital', unit_types)

        # Cleanup
        if os.path.exists(parquet_file):
            os.remove(parquet_file)

    def test_export_relations_to_parquet(self):
        # Save some test data
        self.store.save_work(self.work_data)
        self.store.save_relations(self.relations)

        # Export to Parquet
        parquet_file = "test_export_relations.parquet"
        result = self.store.export_relations_to_parquet(parquet_file)

        # Verify the export was successful
        self.assertTrue(os.path.exists(parquet_file))

        # Verify Parquet content
        table = pq.read_table(parquet_file)
        self.assertEqual(len(table), 1)

        data = table.to_pydict()
        self.assertEqual(data['celex_source'][0], "32022R2065")
        self.assertEqual(data['celex_target'][0], "32010R1060")
        self.assertEqual(data['relation_type'][0], "repeals")

        # Cleanup
        if os.path.exists(parquet_file):
            os.remove(parquet_file)
