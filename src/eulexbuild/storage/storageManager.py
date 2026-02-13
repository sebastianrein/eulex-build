import logging

import polars as pl
from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from .database import get_session, init_engine
from .models import Work, TextUnit, Relation


class StorageManager:

    def __init__(self, session: Session, db_url: str, logger: logging.Logger = logging.getLogger(__name__)):
        self.session = session
        self.db_url = db_url
        self.logger = logger

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

    def _export_to_formats(self, query, output_dir, file_base_name, formats: set[str]):
        try:
            sql_query = str(query.statement.compile(
                dialect=self.session.bind.dialect,
                compile_kwargs={"literal_binds": True}
            ))

            df = pl.read_database_uri(
                query=sql_query,
                uri=self.db_url,
                engine="connectorx"
            )

            if 'csv' in formats:
                csv_path = output_dir / f"{file_base_name}.csv"
                df.write_csv(csv_path)
                self.logger.debug(f"Successfully exported {file_base_name} to {csv_path} as csv.")

            if 'parquet' in formats:
                parquet_path = output_dir / f"{file_base_name}.parquet"
                df.write_parquet(
                    parquet_path,
                    compression='snappy',
                    statistics=True,
                    use_pyarrow=False
                )
                self.logger.debug(f"Successfully exported {file_base_name} to {parquet_path} as parquet.")

        except (SQLAlchemyError, Exception) as e:
            self.logger.error(f"Error exporting {file_base_name}: {str(e)}")
            raise e

    def export_works(self, output_dir, formats: set[str], include_raw_full_text=False):
        if include_raw_full_text:
            works_query = self.session.query(Work)
        else:
            works_query = self.session.query(
                Work.celex_id,
                Work.document_type,
                Work.title,
                Work.date_adopted,
                Work.language
            )
        self._export_to_formats(
            query=works_query,
            output_dir=output_dir,
            file_base_name='works',
            formats=formats
        )

    def export_text_units(self, output_dir, formats: set[str]):
        text_units_query = self.session.query(TextUnit)
        self._export_to_formats(
            query=text_units_query,
            output_dir=output_dir,
            file_base_name='text_units',
            formats=formats
        )

    def export_relations(self, output_dir, formats: set[str]):
        relations_query = self.session.query(Relation)
        self._export_to_formats(
            query=relations_query,
            output_dir=output_dir,
            file_base_name='relations',
            formats=formats
        )


def create_store(db_url="sqlite:///eulex_build.db",
                 logger: logging.Logger = logging.getLogger(__name__)) -> StorageManager:
    engine = init_engine(db_url)
    session = get_session(engine)
    return StorageManager(session, db_url, logger)
