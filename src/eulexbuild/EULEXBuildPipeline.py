import logging
import multiprocessing
import sys
import time
from datetime import datetime
from functools import partial
from importlib.metadata import version, PackageNotFoundError
from logging import Logger
from logging.handlers import QueueListener, QueueHandler
from math import ceil
from multiprocessing import Pool, current_process, Queue
from pathlib import Path

import yaml
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm, _TqdmLoggingHandler

from eulexbuild.config_validation import Config, validate_configuration
from eulexbuild.data import DataResolver
from eulexbuild.data.cellar_sparql import get_eurovoc_labels_for_keywords, get_descriptive_celex_ids, \
    get_procedure_celex_ids
from eulexbuild.storage.storageManager import StorageManager, create_store


def _process_celex_document(celex_id: str, include_recitals: bool, include_articles: bool,
                            include_annexes: bool, include_relations: bool = True,
                            include_original_act_relations_for_consolidated_texts: bool = False,
                            logger: Logger = None) -> dict:
    if logger is None:
        logger = logging.getLogger(_get_worker_logger_name())

    resolver = DataResolver(celex_id, logger)
    return {
        "work": dict(
            celex_id=celex_id,
            title=resolver.get_title(),
            date_adopted=resolver.get_date_adopted(),
            document_type=resolver.get_document_type(),
            language="eng",
            full_text_html=resolver.get_full_text_html()
        ),
        "text_units": resolver.get_text_units(include_recitals, include_articles, include_annexes),
        "relations": resolver.get_relations(include_relations, include_original_act_relations_for_consolidated_texts)
    }


def _get_worker_logger_name() -> str:
    proc = current_process()
    proc_name = proc.name or ""
    if "-" in proc_name:
        suffix = proc_name.rsplit("-", 1)[-1]
    else:
        suffix = str(proc.pid) if getattr(proc, "pid", None) is not None else "unknown"
    return f"eulexbuild_worker_{suffix}"


def _worker_init(log_queue: Queue):
    qh = QueueHandler(log_queue)
    worker_logger = logging.getLogger(_get_worker_logger_name())
    worker_logger.setLevel(logging.DEBUG)
    worker_logger.addHandler(qh)


def _calculate_optimal_sizes(num_items: int, num_workers: int) -> tuple[int, int]:
    """
    Calculate optimal chunksize and batch_size.

    Returns: (chunksize, batch_size)
    """
    # Chunksize: balance IPC overhead vs load distribution
    # Rule: aim for ~4-8 chunks per worker for good load balancing
    chunks_per_worker = 4
    chunksize = max(1, ceil(num_items / (num_workers * chunks_per_worker)))
    chunksize = min(chunksize, 100)

    # Batch size: balance DB transaction overhead vs memory
    # Rule: ~2-4 chunks worth of data per DB write
    batch_size = max(50, chunksize * 2)
    batch_size = min(batch_size, 500)  # Cap for memory safety

    return chunksize, batch_size


class EULEXBuildPipeline:

    def __init__(self, config_path: str | Path, db_name="eulex_build.db"):
        try:
            current_version = version("eulexbuild")
        except PackageNotFoundError:
            current_version = "unknown"

        self.config: Config = validate_configuration(config_path)
        self.output_dir: Path = (Path.cwd() / self.config.output.output_directory).resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger: logging.Logger = self._setup_logger()

        self.logger.debug(f"Running on Python {sys.version} on {sys.platform}")
        self.logger.debug(f"Running with eulexbuild version {current_version} ")
        self.logger.debug("-" * 60)

        self.logger.info(f"Initializing EULEX-Build Pipeline with config: {config_path}")
        self.logger.info(f"Created output directory: {self.output_dir}")

        self._db_url = f"sqlite:///{self.output_dir}/{db_name}"
        self.store: StorageManager = create_store(self._db_url, self.logger)
        self.logger.debug(f"Storage manager with '{db_name}' database initialized successfully in output directory")

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger("eulexbuild_pipeline")
        logger.setLevel(logging.DEBUG)

        # File Handler
        log_file = self.output_dir / "pipeline.log"
        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        file_handler.setFormatter(file_formatter)

        # Queue for Multiprocessing logging
        self.log_queue = multiprocessing.Manager().Queue()
        self.log_listener = QueueListener(self.log_queue, file_handler, respect_handler_level=True)
        self.log_listener.start()

        queue_handler = QueueHandler(self.log_queue)
        queue_handler.setLevel(logging.DEBUG)
        logger.addHandler(queue_handler)

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        return logger

    def run(self):
        try:
            self.logger.info("=" * 60)
            self.logger.info("Starting EULEX-Build Pipeline")
            self.logger.info("=" * 60)

            # Log configuration details
            self.logger.info(f"Mode: {self.config.data.mode}")
            if self.config.data.mode == "fixed":
                self.logger.info(f"Total CELEX IDs to process: {len(self.config.data.celex_ids)}")
                self.logger.info(f"CELEX IDs: {', '.join(sorted(self.config.data.celex_ids))}")
                self.logger.info(f"Total Procedure numbers to process: {len(self.config.data.procedure_numbers)}")
                self.logger.info(f"Procedure numbers: {', '.join(sorted(self.config.data.procedure_numbers))}")
            elif self.config.data.mode == "descriptive":
                self.logger.info(f"Document types: {', '.join(self.config.data.document_types)}")
                self.logger.info(f"Date range: {self.config.data.start_date} to {self.config.data.end_date}")
                if self.config.data.filter_keywords:
                    self.logger.info(f"Filter keywords: {', '.join(self.config.data.filter_keywords)}")

            self.logger.debug(f"Parallel processing: {self.config.processing.enable_parallel_processing}")
            if self.config.processing.enable_parallel_processing:
                self.logger.debug(f"Max threads: {self.config.processing.max_threads}")

            self.logger.debug(
                f"Text extraction - Recitals: {self.config.processing.text_extraction.include_recitals}, "
                f"Articles: {self.config.processing.text_extraction.include_articles}, "
                f"Annexes: {self.config.processing.text_extraction.include_annexes}")

            self.logger.debug(
                f"Relations extraction - Include relations: {self.config.processing.relations_extraction.include_relations}, "
                f"Include original act relations for consolidated texts: {self.config.processing.relations_extraction.include_original_act_relations_for_consolidated_texts}")

            # Process data based on mode
            self.logger.info("-" * 60)
            celex_ids = set()
            if self.config.data.mode == "fixed":
                self.logger.info("Starting data collection in 'fixed' mode")
                celex_ids = self.config.data.celex_ids
                procedure_celex_ids = get_procedure_celex_ids(self.config.data.procedure_numbers, self.logger)
                celex_ids.update(procedure_celex_ids)
            elif self.config.data.mode == "descriptive":
                self.logger.info("Starting data collection in 'descriptive' mode")
                eurovoc_uris = self._review_eurovoc_labels()
                celex_ids = get_descriptive_celex_ids(
                    start_date=self.config.data.start_date,
                    end_date=self.config.data.end_date,
                    eurovoc_uris=eurovoc_uris,
                    include_decisions=("decision" in self.config.data.document_types),
                    include_directives=("directive" in self.config.data.document_types),
                    include_regulations=("regulation" in self.config.data.document_types),
                    include_proposals=("proposal" in self.config.data.document_types),
                    include_corrigenda=self.config.data.include_corrigenda,
                    include_consolidated_texts=self.config.data.include_consolidated_texts,
                    include_national_transpositions=self.config.data.include_national_transpositions,
                    logger=self.logger
                )
            else:
                raise ValueError(f"Unknown mode: {self.config.data.mode}")

            # Total = #CelexIDs + 3 export steps (works, text_units, relations) + Readme export
            total = len(celex_ids) + 3 + 1
            with tqdm(total=total, unit="step", desc="Pipeline Progress") as pbar, logging_redirect_tqdm(
                    loggers=[self.logger]):

                # Fix for tqdm not preserving logging level
                for handler in self.logger.handlers:
                    try:
                        if isinstance(handler, _TqdmLoggingHandler):
                            handler.setLevel(logging.INFO)
                    except TypeError:
                        if handler.level == logging.NOTSET:
                            handler.setLevel(logging.INFO)

                if self.config.processing.enable_parallel_processing:
                    self._get_data_parallel(celex_ids, pbar)
                else:
                    self._get_data(celex_ids, pbar)

                self.logger.info("-" * 60)
                self.logger.debug("Data collection completed. Starting export process")
                self._export_results(pbar)

                self._export_readme()
                pbar.update()

                self.logger.info("=" * 60)
                self.logger.info("Pipeline completed successfully")
                self.logger.info("=" * 60)

                pbar.close()

            # Cleanup
            self.log_listener.stop()

        except Exception as e:
            self.logger.error("=" * 60)
            self.logger.error(f"Error occurred during pipeline execution: {e}")
            self.logger.exception("Full traceback:")
            self.logger.error("=" * 60)
            raise

    def _review_eurovoc_labels(self) -> set[str]:
        """
        Fetch EuroVoc labels and allow user review before proceeding (or skip if automated).
        Returns: Set of EuroVoc URIs
        """
        review_file = self.output_dir / "eurovoc_labels.yaml"

        # Fetch initial labels
        self.logger.debug(f"Fetching EuroVoc labels for keywords: {self.config.data.filter_keywords}")
        labels = get_eurovoc_labels_for_keywords(set(self.config.data.filter_keywords), self.logger)

        total_labels = sum(
            len(uri_dict)
            for uri_dict in labels.values()
        )
        self.logger.debug(f"Saving {total_labels} unique EuroVoc concepts to {review_file} for review")

        # Convert to more readable format: show all labels for each URI
        formatted_labels = {}
        for keyword, uri_dict in labels.items():
            formatted_labels[keyword] = {
                uri: list(label_set)
                for uri, label_set in uri_dict.items()
            }

        with open(review_file, 'w', encoding='utf-8') as f:
            yaml.safe_dump({
                'instructions': 'Review the EuroVoc labels below. Remove unwanted entries or add new ones by removing or editing the URI (Unique Resource Identifier) of the corresponding EuroVoc. Save this file and continue by pressing enter in the terminal.',
                'labels': formatted_labels
            }, f, allow_unicode=True, sort_keys=False, width=None)

        # Check if automated mode is enabled
        if self.config.processing.automated_mode:
            self.logger.info("Automated mode enabled - skipping interactive EuroVoc review")
            reviewed_labels = set(
                uri
                for uri_dict in labels.values()
                for uri in uri_dict.keys()
            )
            self.logger.info(f"Using all {len(reviewed_labels)} fetched EuroVoc labels automatically")
            return reviewed_labels

        # Interactive mode - pause for user review
        self.logger.info("=" * 60)
        self.logger.info(f"REVIEW REQUIRED: Please review {review_file}")
        self.logger.info("Edit the file to add/remove/modify EuroVoc labels. Save the changes.")
        self.logger.info("=" * 60)
        time.sleep(0.1)
        input("Press Enter to continue after saving the changes in the file...")

        # Load reviewed labels
        with open(review_file, 'r', encoding='utf-8') as f:
            reviewed = yaml.safe_load(f)

        reviewed_labels = set(
            uri
            for label_dict in reviewed.get('labels', {}).values()
            for uri in label_dict.keys()
        )
        self.logger.info(
            f"Loaded {len(reviewed_labels)} reviewed EuroVoc labels")

        return reviewed_labels

    def _get_data(self, celex_ids: set[str], pbar: tqdm = None):
        total = len(celex_ids)
        self.logger.info(f"Processing {total} documents sequentially")

        processed = 0
        failed = 0

        for idx, celex_id in enumerate(celex_ids, 1):
            try:
                self.logger.debug(f"[{idx}/{total}] Processing document: {celex_id}")

                result = _process_celex_document(
                    celex_id,
                    include_recitals=self.config.processing.text_extraction.include_recitals,
                    include_articles=self.config.processing.text_extraction.include_articles,
                    include_annexes=self.config.processing.text_extraction.include_annexes,
                    include_relations=self.config.processing.relations_extraction.include_relations,
                    include_original_act_relations_for_consolidated_texts=self.config.processing.relations_extraction.include_original_act_relations_for_consolidated_texts,
                    logger=self.logger
                )

                self.logger.debug(f"[{idx}/{total}] Saving work metadata for {celex_id}")
                self.store.save_work(result["work"])

                self.logger.debug(f"[{idx}/{total}] Saving {len(result['text_units'])} text units for {celex_id}")
                self.store.save_text_units(result["text_units"])

                self.logger.debug(f"[{idx}/{total}] Saving {len(result['relations'])} relations for {celex_id}")
                self.store.save_relations(result["relations"])

                processed += 1
                pbar.update() if pbar else None
                self.logger.info(f"[{idx}/{total}] Successfully processed {celex_id}")

            except Exception as e:
                failed += 1
                self.logger.error(f"[{idx}/{total}] Failed to process {celex_id}: {e}")
                self.logger.exception(f"[{idx}/{total}] Traceback for {celex_id}:")

        self.logger.info(
            f"Sequential processing complete: {processed} successful, {failed} failed out of {total} total")

    def _get_data_parallel(self, celex_ids: set[str], pbar: tqdm = None):
        workers = self.config.processing.max_threads
        total = len(celex_ids)

        chunksize, batch_size = _calculate_optimal_sizes(total, workers)

        self.logger.info(f"Processing {total} documents in parallel with {workers} workers")
        self.logger.debug(f"Parallel processing parameters - Chunksize: {chunksize}, Batch size: {batch_size}")

        func = partial(
            _process_celex_document,
            include_recitals=self.config.processing.text_extraction.include_recitals,
            include_articles=self.config.processing.text_extraction.include_articles,
            include_annexes=self.config.processing.text_extraction.include_annexes,
            include_relations=self.config.processing.relations_extraction.include_relations,
            include_original_act_relations_for_consolidated_texts=self.config.processing.relations_extraction.include_original_act_relations_for_consolidated_texts
        )

        processed = 0
        failed = 0
        batch_count = 0

        interrupted = False

        with Pool(processes=workers, initializer=_worker_init, initargs=(self.log_queue,)) as pool:
            self.logger.debug("Worker pool created, starting document processing")
            results_iter = pool.imap_unordered(func, celex_ids, chunksize=chunksize)

            works, text_units, relations = [], [], []

            try:
                for result in results_iter:
                    try:
                        works.append(result["work"])
                        text_units.extend(result["text_units"])
                        relations.extend(result["relations"])
                        processed += 1
                        pbar.update() if pbar else None

                        # Log progress periodically
                        if processed % 10 == 0 or processed == total:
                            self.logger.debug(
                                f"Progress: {processed}/{total} documents processed ({processed / total * 100:.1f}%)")

                        if len(works) >= batch_size:
                            batch_count += 1
                            self.logger.debug(f"Batch {batch_count}: Saving {len(works)} works, "
                                              f"{len(text_units)} text units, {len(relations)} relations")

                            self.store.save_work(works)
                            self.store.save_text_units(text_units)
                            self.store.save_relations(relations)

                            self.logger.debug(f"Batch {batch_count} saved successfully")
                            works, text_units, relations = [], [], []

                    except Exception as e:
                        failed += 1
                        self.logger.error(f"Failed to process a document in parallel mode: {e}")
                        self.logger.exception("Traceback:")

            except KeyboardInterrupt:
                interrupted = True
                self.logger.warning(
                    "KeyboardInterrupt received - terminating worker pool and flushing pending batches")
                try:
                    pool.terminate()
                    pool.join()
                except Exception:
                    pass

            # Flush remaining
            if works:
                batch_count += 1
                self.logger.debug(f"Final batch {batch_count}: Saving {len(works)} works, "
                                  f"{len(text_units)} text units, {len(relations)} relations")
                self.store.save_work(works)
            if text_units:
                self.store.save_text_units(text_units)
            if relations:
                self.store.save_relations(relations)

        if interrupted:
            raise KeyboardInterrupt

        self.logger.info(f"Parallel processing complete: {processed} successful, {failed} failed")
        self.logger.debug(f"Total batches saved: {batch_count}")

    def _export_results(self, pbar: tqdm = None) -> None:
        formats_str = ', '.join([f.upper() for f in self.config.output.formats])
        self.logger.info(f"Exporting data to {len(self.config.output.formats)} format(s): {formats_str}")

        try:
            # Export works
            self.logger.debug(f"Exporting works to {formats_str}")
            self.store.export_works(
                self.output_dir,
                self.config.output.formats,
                include_raw_full_text=self.config.output.include_raw_full_text
            )
            pbar.update() if pbar else None

            # Export text units
            self.logger.debug(f"Exporting text units to {formats_str}")
            self.store.export_text_units(
                self.output_dir,
                self.config.output.formats
            )
            pbar.update() if pbar else None

            # Export relations
            self.logger.debug(f"Exporting relations to {formats_str}")
            self.store.export_relations(
                self.output_dir,
                self.config.output.formats
            )
            pbar.update() if pbar else None

            self.logger.info("All exports completed successfully")

        except Exception as e:
            self.logger.error(f"Failed to export data: {e}")
            self.logger.exception(f"Traceback for export:")
            raise


    def _export_readme(self) -> None:
        """Export a comprehensive README file explaining the pipeline output."""
        readme_path = self.output_dir / "README.md"
        self.logger.debug(f"Generating README file at {readme_path}")

        try:
            # Get statistics from the store
            work_count = self.store.count_works()
            text_unit_count = self.store.count_text_units()
            relation_count = self.store.count_relations()
            avg_text_units = text_unit_count / work_count if work_count > 0 else 0
            avg_relations = relation_count / work_count if work_count > 0 else 0

            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            with open(readme_path, 'w', encoding='utf-8') as f:
                # Header
                f.write("# EULEX-Build Pipeline Output\n\n")
                f.write(f"*Generated: {timestamp}*\n\n")

                # What is this dataset
                f.write("## What This Dataset Contains\n\n")
                f.write("This dataset contains structured legal documents from EUR-Lex, the official repository ")
                f.write("of European Union law. The EULEX-Build pipeline has extracted, parsed, and organized ")
                f.write("these documents into machine-readable formats suitable for legal research, natural ")
                f.write("language processing, network analysis or other research.\n\n")

                # Metadata section
                f.write("### Dataset Metadata\n\n")
                f.write(f"**Project Name:** {self.config.metadata.project_name}\n\n")
                if self.config.metadata.author:
                    f.write(f"**Author:** {self.config.metadata.author}\n\n")
                f.write(f"**Description:** {self.config.metadata.description}\n\n")
                f.write(f"**Date Created:** {self.config.metadata.date_created}\n\n")
                f.write(f"**Version:** {self.config.metadata.version}\n\n")

                # Data selection method
                f.write("### Document Selection\n\n")
                if self.config.data.mode == "fixed":
                    f.write(f"**Method:** Fixed List of {len(self.config.data.celex_ids)} CELEX IDs\n\n")
                    f.write("This dataset contains only those documents.\n\n")
                    f.write("**Documents included:**\n")
                    for celex_id in sorted(self.config.data.celex_ids)[:10]:
                        f.write(f"- `{celex_id}`\n")
                    if len(self.config.data.celex_ids) > 10:
                        f.write(f"- ... and {len(self.config.data.celex_ids) - 10} more (see configuration file)\n")
                    f.write("\n")
                else:  # descriptive mode
                    f.write("**Method:** Descriptive Query (Automatic Document Discovery)\n\n")
                    doc_types = [dt.replace('_', ' ').title() + 's' for dt in self.config.data.document_types]
                    f.write(f"Documents were automatically discovered and retrieved based on these criteria:\n\n")
                    f.write(f"- **Document Types:** {', '.join(doc_types)}\n")
                    f.write(f"- **Time Period:** {self.config.data.start_date} to {self.config.data.end_date}\n")

                    if self.config.data.filter_keywords:
                        f.write(f"- **Filtered by Keywords:** {', '.join(self.config.data.filter_keywords)}\n")
                        f.write("  *(Documents must be tagged with at least one of these EuroVoc concepts)*\n")

                    f.write(f"- **Corrigenda:** {'Included' if self.config.data.include_corrigenda else 'Excluded'}\n")
                    f.write(
                        f"- **Consolidated Texts:** {'Included' if self.config.data.include_consolidated_texts else 'Excluded'}\n")
                    f.write(
                        f"- **National Transpositions:** {'Included' if self.config.data.include_national_transpositions else 'Excluded'}\n\n")

                    eurovoc_file = self.output_dir / "eurovoc_labels.yaml"
                    if eurovoc_file.exists():
                        f.write("\n #### EuroVoc Concepts Used\n\n")
                        f.write(
                            "The following EuroVoc concepts were retrieved from the keywords and used to filter documents.\n\n")
                        if self.config.processing.automated_mode:
                            f.write(
                                "The labels were automatically generated from the given keywords and not checked again.\n\n")
                        else:
                            f.write(
                                "The labels were automatically generated from the given keywords and reviewed by the user.\n\n")

                        with open(eurovoc_file, 'r', encoding='utf-8') as ef:
                            eurovoc_data = yaml.safe_load(ef)
                            labels = eurovoc_data.get('labels', {})

                            for keyword in sorted(self.config.data.filter_keywords):
                                if keyword in labels:
                                    f.write(f"**{keyword}:**\n")
                                    concepts = labels[keyword]
                                    # Show first 5 concepts as preview
                                    for i, (label, uri) in enumerate(list(concepts.items())[:5], 1):
                                        f.write(f"- {label} (`{uri}`)\n")
                                    if len(concepts) > 5:
                                        f.write(f"- *...and {len(concepts) - 5} more concepts*\n")
                                    f.write("\n")

                # Dataset statistics
                f.write("### Dataset Statistics\n\n")
                f.write(f"📊 **{work_count:,}** legal documents processed  \n")
                f.write(
                    f"📄 **{text_unit_count:,}** text segments extracted (avg. {avg_text_units:.1f} per document)  \n")
                f.write(
                    f"🔗 **{relation_count:,}** document relationships identified (avg. {avg_relations:.1f} per document)\n\n")

                # Technical details
                f.write("## Technical Details\n\n")

                # Pipeline configuration
                f.write("### Pipeline Configuration\n\n")
                f.write(f"**Mode:** {self.config.data.mode.title()}\n\n")
                f.write(
                    f"**Parallel Processing:** {'Enabled' if self.config.processing.enable_parallel_processing else 'Disabled'}\n\n")
                if self.config.processing.enable_parallel_processing:
                    f.write(f"**Worker Threads:** {self.config.processing.max_threads}\n\n")
                f.write(f"**Automated Mode:** {'Enabled' if self.config.processing.automated_mode else 'Disabled'}\n\n")

                # Text extraction settings
                f.write("\n### Text Extraction Settings\n\n")
                f.write("**Included Sections:**\n")
                f.write(f"- Recitals: {'✓' if self.config.processing.text_extraction.include_recitals else '✗'}\n")
                f.write(f"- Articles: {'✓' if self.config.processing.text_extraction.include_articles else '✗'}\n")
                f.write(f"- Annexes: {'✓' if self.config.processing.text_extraction.include_annexes else '✗'}\n")

                # Relations extraction settings
                f.write("\n### Relations Extraction Settings\n\n")
                f.write(
                    f"**Include Relations:** {'✓' if self.config.processing.relations_extraction.include_relations else '✗'}\n")
                if self.config.processing.relations_extraction.include_relations:
                    f.write(
                        f"**Include Original Act Relations for Consolidated Texts:** {'✓' if self.config.processing.relations_extraction.include_original_act_relations_for_consolidated_texts else '✗'}\n")

                # Data source details
                f.write("\n### Data Source\n\n")
                f.write("**Source:** EUR-Lex CELLAR Repository (SPARQL endpoint and REST API)\n\n")
                f.write(
                    f"**Language:** {self.config.data.language if hasattr(self.config.data, 'language') else 'English (eng)'}\n\n")
                if self.config.data.mode == "descriptive":
                    f.write(f"**Query Date Range:** {self.config.data.start_date} to {self.config.data.end_date}\n\n")
                    f.write(f"**Include Corrigenda:** {'✓' if self.config.data.include_corrigenda else '✗'}\n\n")
                    f.write(
                        f"**Include Consolidated Texts:** {'✓' if self.config.data.include_consolidated_texts else '✗'}\n\n")
                    f.write(
                        f"**Include National Transpositions:** {'✓' if self.config.data.include_national_transpositions else '✗'}\n\n")

                # Export configuration
                f.write("\n### Export Configuration\n\n")
                f.write(f"**Output Formats:** {', '.join([fmt.upper() for fmt in self.config.output.formats])}\n\n")
                f.write(f"**Output Directory:** `{self.output_dir.name}`\n\n")
                f.write(
                    f"**Include Raw Full Text HTML:** {'✓' if self.config.output.include_raw_full_text else '✗'}\n\n")

                # Storage details
                db_path = Path(self._db_url.replace("sqlite:///", ""))
                f.write("\n### Storage Backend\n\n")
                f.write(f"**Database:** SQLite\n\n")
                f.write(f"**Database File:** `{db_path.name}`\n\n")
                f.write(
                    f"**Database Size:** {db_path.stat().st_size / (1024 * 1024):.2f} MB\n" if db_path.exists() else "")

                # Processing statistics
                f.write("\n### Processing Summary\n\n")
                f.write(f"**Generation Time:** {timestamp}\n\n")
                f.write(f"**Total Documents Processed:** {work_count:,}\n\n")
                f.write(f"**Total Text Units Extracted:** {text_unit_count:,}\n\n")
                f.write(f"**Total Relations Identified:** {relation_count:,}\n\n")
                f.write(f"**Average Text Units per Document:** {avg_text_units:.2f}\n\n")
                f.write(f"**Average Relations per Document:** {avg_relations:.2f}\n\n")
                f.write(f"**Log File:** `pipeline.log`\n\n")

                # Additional files
                f.write("### 📋 Additional Files\n\n")
                if self.config.data.mode == "descriptive" and self.config.data.filter_keywords:
                    f.write("- `eurovoc_labels.yaml` - EuroVoc concept mappings used for document filtering\n\n")
                f.write("- `pipeline.log` - Full log of pipeline run\n\n")

                # Footer
                f.write("---\n\n")
                f.write("*Generated by EULEX-Build Pipeline*  \n")

            self.logger.debug(f"README file generated successfully at {readme_path}")

        except Exception as e:
            self.logger.error(f"Failed to generate README: {e}")
            self.logger.exception("Traceback:")
            raise
