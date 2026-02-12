import csv
import logging

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from .database import get_session, init_engine
from .models import Work, TextUnit, Relation


class StorageManager:

    def __init__(self, session: Session, logger: logging.Logger = logging.getLogger(__name__)):
        self.session = session
        self.logger = logger

    # Mapper functions for converting database objects to dictionaries
    @staticmethod
    def _map_work(work, include_full_text_html=False):
        data = {
            'celex_id': work.celex_id,
            'document_type': work.document_type,
            'title': work.title,
            'date_adopted': work.date_adopted.isoformat() if work.date_adopted else None,
            'language': work.language
        }
        if include_full_text_html:
            data['full_text_html'] = work.full_text_html
        return data

    @staticmethod
    def _map_text_unit(unit):
        return {
            'id': unit.id,
            'celex_id': unit.celex_id,
            'type': unit.type,
            'number': unit.number,
            'title': unit.title,
            'text': unit.text
        }

    @staticmethod
    def _map_relation(relation):
        return {
            'id': relation.id,
            'celex_source': relation.celex_source,
            'celex_target': relation.celex_target,
            'relation_type': relation.relation_type
        }

    def count_works(self):
        return self.session.query(Work).count()

    def count_text_units(self):
        return self.session.query(TextUnit).count()

    def count_relations(self):
        return self.session.query(Relation).count()

    def save_work(self, work_data: dict | list[dict]):
        try:
            if isinstance(work_data, dict):
                work_data = [work_data]
            if not work_data:
                return
            self.session.execute(insert(Work), work_data)
            self.session.commit()
            self.logger.debug(f"Successfully saved {len(work_data)} work record(s) to database.")
        except SQLAlchemyError as e:
            self.session.rollback()
            raise e

    def save_text_units(self, text_units: list):
        try:
            if not text_units:
                return
            self.session.execute(insert(TextUnit), text_units)
            self.session.commit()
            self.logger.debug(f"Successfully saved {len(text_units)} text unit(s) to database.")
        except SQLAlchemyError as e:
            self.session.rollback()
            raise e

    def save_relations(self, relations: list):
        try:
            if not relations:
                return
            self.session.execute(insert(Relation), relations)
            self.session.commit()
            self.logger.debug(f"Successfully saved {len(relations)} relation(s) to database.")
        except SQLAlchemyError as e:
            self.session.rollback()
            raise e

    def _export_to_csv(self, file_path: str, query_result, fieldnames, row_mapper):
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for item in query_result:
                    writer.writerow(row_mapper(item))
        except IOError as e:
            self.logger.error(f"Error writing to file {file_path}: {str(e)}")
            raise e

    def export_works_to_csv(self, file_path, include_raw_full_text=False):
        try:
            chunk_size = 100 if include_raw_full_text else 1000
            works = self.session.query(Work).yield_per(chunk_size)
            fieldnames = ['celex_id', 'document_type', 'title', 'date_adopted', 'language']
            if include_raw_full_text:
                fieldnames.append('full_text_html')
            row_mapper = lambda work: self._map_work(work, include_full_text_html=include_raw_full_text)
            self._export_to_csv(file_path, works, fieldnames, row_mapper)
            self.logger.debug(f"Successfully exported work records to {file_path} as csv.")
        except SQLAlchemyError as e:
            raise e

    def export_text_units_to_csv(self, file_path):
        try:
            text_units = self.session.query(TextUnit).yield_per(1000)
            fieldnames = ['id', 'celex_id', 'type', 'number', 'title', 'text']
            self._export_to_csv(file_path, text_units, fieldnames, self._map_text_unit)
            self.logger.debug(f"Successfully exported text units to {file_path} as csv.")
        except SQLAlchemyError as e:
            raise e

    def export_relations_to_csv(self, file_path):
        try:
            relations = self.session.query(Relation).yield_per(1000)
            fieldnames = ['id', 'celex_source', 'celex_target', 'relation_type']
            self._export_to_csv(file_path, relations, fieldnames, self._map_relation)
            self.logger.debug(f"Successfully exported relations to {file_path} as csv.")
        except SQLAlchemyError as e:
            raise e

    def _export_to_parquet(self, file_path, query, row_mapper, chunk_size=1000, schema=None):
        try:
            with pq.ParquetWriter(file_path, schema=schema) as writer:
                chunk_data = []
                for item in query.yield_per(chunk_size):
                    chunk_data.append(row_mapper(item))

                    if len(chunk_data) >= chunk_size:
                        batch = pa.RecordBatch.from_pylist(chunk_data, schema=schema)
                        writer.write_batch(batch)
                        chunk_data = []

                if chunk_data:
                    batch = pa.RecordBatch.from_pylist(chunk_data, schema=schema)
                    writer.write_batch(batch)

        except Exception as e:
            self.logger.error(f"Error writing to file {file_path}: {str(e)}")
            raise e

    def export_works_to_parquet(self, file_path, include_raw_full_text=False):
        try:
            works_query = self.session.query(Work)
            row_mapper = lambda work: self._map_work(work, include_full_text_html=include_raw_full_text)

            fields = [
                pa.field('celex_id', pa.string()),
                pa.field('document_type', pa.string()),
                pa.field('title', pa.string()),
                pa.field('date_adopted', pa.string()),
                pa.field('language', pa.string())
            ]
            if include_raw_full_text:
                fields.append(pa.field('full_text_html', pa.string()))
            schema = pa.schema(fields)

            chunk_size = 100 if include_raw_full_text else 1000

            self._export_to_parquet(file_path, works_query, row_mapper, chunk_size=chunk_size, schema=schema)
            self.logger.debug(f"Successfully exported work records to {file_path} as parquet.")
        except SQLAlchemyError as e:
            raise e

    def export_text_units_to_parquet(self, file_path):
        try:
            text_units_query = self.session.query(TextUnit)

            schema = pa.schema([
                pa.field('id', pa.int64()),
                pa.field('celex_id', pa.string()),
                pa.field('type', pa.string()),
                pa.field('number', pa.string()),
                pa.field('title', pa.string()),
                pa.field('text', pa.string())
            ])

            self._export_to_parquet(file_path, text_units_query, self._map_text_unit, schema=schema)
            self.logger.debug(f"Successfully exported text units to {file_path} as parquet.")
        except SQLAlchemyError as e:
            raise e

    def export_relations_to_parquet(self, file_path):
        try:
            relations_query = self.session.query(Relation)

            schema = pa.schema([
                pa.field('id', pa.int64()),
                pa.field('celex_source', pa.string()),
                pa.field('celex_target', pa.string()),
                pa.field('relation_type', pa.string())
            ])

            self._export_to_parquet(file_path, relations_query, self._map_relation, schema=schema)
            self.logger.debug(f"Successfully exported relations to {file_path} as parquet.")
        except SQLAlchemyError as e:
            raise e


def create_store(db_url="sqlite:///eulex_build.db",
                 logger: logging.Logger = logging.getLogger(__name__)) -> StorageManager:
    engine = init_engine(db_url)
    session = get_session(engine)
    return StorageManager(session, logger)
