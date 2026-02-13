import datetime
import logging
import re

from lxml import etree, html

from eulexbuild.data.cellar_restapi import get_full_text_xhtml, get_expression_metadata_xml, get_full_text_plain_html, \
    get_annex_xhtml, get_annex_plain_html
from eulexbuild.data.cellar_sparql import get_all_properties
from eulexbuild.utils import normalize_string, is_consolidated_celex, convert_consolidated_celex_to_original

NSMAP = {
    "xhtml": "http://www.w3.org/1999/xhtml",
    "re": "http://exslt.org/regular-expressions"
}


def _flatten_content_divs(html_bytes: bytes, celex_id: str, logger: logging.Logger = None) -> bytes:
    """Flatten div elements with class='content' by moving their children to the parent."""
    try:
        tree = etree.fromstring(html_bytes)
        content_divs = tree.xpath(".//*[local-name()='div' and @class='content']")

        for div in content_divs:
            parent = div.getparent()
            if parent is not None:
                index = parent.index(div)
                for child in reversed(list(div)):
                    parent.insert(index, child)
                parent.remove(div)

        return etree.tostring(tree, encoding='utf-8')
    except Exception as e:
        if logger:
            logger.warning(
                f"Failed to flatten content divs for {celex_id} as xml, retry as html: {e}")
        try:
            tree = html.fromstring(html_bytes)
            content_divs = tree.xpath(".//*[local-name()='div' and @class='content']")

            for div in content_divs:
                parent = div.getparent()
                if parent is not None:
                    index = parent.index(div)
                    for child in reversed(list(div)):
                        parent.insert(index, child)
                    parent.remove(div)

            return html.tostring(tree, encoding='utf-8')
        except Exception as e2:
            if logger:
                logger.warning(
                    f"Failed to flatten content divs for {celex_id} as xml,  will probably result in broken text_units: {e2}")
            return html_bytes


def is_standard_structure(tree) -> bool:
    return bool(tree.xpath(".//*[local-name()='div' and (starts-with(@id, 'rct_') or starts-with(@id, 'art_'))]"))


def is_manual_structure(tree) -> bool:
    return bool(tree.xpath(
        ".//*[local-name()='p' and (@class='li ManualConsidrant' or @class='Titrearticle' or @class='Annexetitre')]"))


def is_text_only_structure(tree) -> bool:
    return bool(tree.xpath(".//*[local-name()='div' and @id='TexteOnly']"))


class DataResolver:

    def __init__(self, celex_id: str, logger: logging.Logger = logging.getLogger()):
        self.celex_id: str = celex_id
        self._original_celex = None
        self.logger: logging.Logger = logger
        self._original_data_resolver_for_consolidated = None
        self._raw_full_text_xhtml = None
        self._raw_full_text_plain_html = None
        self._expression_metadata_xml = None
        self._sparql_query_results = None
        self._title = None
        self._document_type = None

    @property
    def raw_full_text_xhtml(self):
        if self._raw_full_text_xhtml is None:
            xhtml = get_full_text_xhtml(self.celex_id, logger=self.logger)
            self._raw_full_text_xhtml = _flatten_content_divs(xhtml, self.celex_id, self.logger)
        return self._raw_full_text_xhtml

    @property
    def raw_full_text_plain_html(self):
        if self._raw_full_text_plain_html is None:
            plain_html = get_full_text_plain_html(self.celex_id, logger=self.logger)
            self._raw_full_text_plain_html = _flatten_content_divs(plain_html, self.celex_id, self.logger)
        return self._raw_full_text_plain_html

    @property
    def expression_metadata_xml(self):
        if self._expression_metadata_xml is None:
            raw_metadata = get_expression_metadata_xml(self.celex_id, logger=self.logger)
            self._expression_metadata_xml = etree.fromstring(raw_metadata)
        return self._expression_metadata_xml

    @property
    def sparql_query_result(self):
        if self._sparql_query_results is None:
            self._sparql_query_results = get_all_properties(self.celex_id, logger=self.logger)
        return self._sparql_query_results

    @property
    def original_celex(self):
        if self._original_celex is None:
            if is_consolidated_celex(self.celex_id):
                self._original_celex = convert_consolidated_celex_to_original(self.celex_id)
            else:
                self._original_celex = self.celex_id
        return self._original_celex

    def get_original_data_resolver_for_consolidated(self) -> DataResolver:
        if is_consolidated_celex(self.celex_id):
            if self._original_data_resolver_for_consolidated is None:
                self._original_data_resolver_for_consolidated = DataResolver(self.original_celex, logger=self.logger)
            return self._original_data_resolver_for_consolidated
        else:
            return self

    def get_full_text_html(self) -> str | None:
        """Get the full text HTML as a string."""
        try:
            return self.raw_full_text_xhtml.decode('utf-8')
        except Exception as e:
            self.logger.error(f"Failed to retrieve full text XHTML for {self.celex_id}, trying plain HTML next: {e}")

        try:
            return self.raw_full_text_plain_html.decode('utf-8')
        except Exception as e:
            self.logger.error(f"Failed to retrieve full text plain HTML for {self.celex_id}: {e}")
            return None

    def get_title(self) -> str:
        if self._title:
            return self._title

        # Primary source: SPARQL query
        try:
            title = self.sparql_query_result["title"]
            if title:
                self.logger.debug(f"Title retrieved from SPARQL for {self.celex_id}")
                title = normalize_string(title)
                self._title = title
                return title
        except Exception as e:
            self.logger.warning(f"Failed to retrieve title from SPARQL for {self.celex_id}: {e}")

        # Fallback 1: Extract from metadata XML
        try:
            title = self.expression_metadata_xml.findtext(".//EXPRESSION/EXPRESSION_TITLE/VALUE")
            if title:
                self.logger.debug(f"Title retrieved from XML metadata for {self.celex_id}")
                title = normalize_string(title)
                self._title = title
                return title
        except (IndexError, AttributeError, TypeError) as e:
            self.logger.warning(f"Failed to extract title from XML metadata for {self.celex_id}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error parsing XML metadata for {self.celex_id}: {e}")

        # Fallback 2: Extract from HTML full text
        try:
            tree = etree.fromstring(self.raw_full_text_xhtml)
            title_nodes = tree.xpath(".//*[local-name()='div' and @class='eli-main-title']")
            div_title = title_nodes[0] if title_nodes else None
            title = " ".join(normalize_string(text) for text in div_title.itertext() if
                             text.strip()) if div_title is not None else ""
            if title:
                self.logger.debug(f"Title extracted from HTML for {self.celex_id}")
                self._title = title
                return title
        except Exception as e:
            self.logger.warning(f"Failed to extract title from HTML for {self.celex_id}: {e}")

        # Error handling if title unavailable
        self.logger.error(f"Unable to retrieve title for {self.celex_id} from any source")
        self._title = "[Unavailable]"
        return "[Unavailable]"

    def get_document_type(self) -> str:
        if self._document_type:
            return self._document_type

        try:
            type_id = self.celex_id[5:7].rstrip('0123456789')
            first_digit = self.celex_id[0]
        except IndexError as e:
            self.logger.error(f"Failed to extract type ID from celex_id '{self.celex_id}': {e}")
            self._document_type = "Unknown"
            return "Unknown"

        doc_type_map = {
            'L': 'directive',
            'R': 'regulation',
            'D': 'decision',
            'PC': 'proposal',
            'DC': 'other preparatory document'
        }

        try:
            type = doc_type_map[type_id]
            if first_digit == '0':
                return f"consolidated {type}"
            self._document_type = type
            return type
        except KeyError as e:
            self.logger.error(f"Unknown document type ID '{type_id}' in celex_id '{self.celex_id}': {e}")
            self._document_type = "Unknown"
            return "Unknown"

    def get_date_adopted(self) -> datetime.date | None:
        if is_consolidated_celex(self.celex_id):
            resolver = self.get_original_data_resolver_for_consolidated()
        else:
            resolver = self

        # Primary source: SPARQL query
        try:
            date_adopted = resolver.sparql_query_result["date"]
            if date_adopted:
                self.logger.debug(f"Date adopted retrieved from SPARQL for {self.celex_id}")
                if isinstance(date_adopted, datetime.date):
                    return date_adopted
                return datetime.datetime.strptime(date_adopted, "%Y-%m-%d").date()
        except Exception as e:
            self.logger.warning(f"Failed to retrieve date adopted from SPARQL for {self.celex_id}: {e}")

        # Fallback 1: Extract from metadata XML
        try:
            date = resolver.expression_metadata_xml.find(".//WORK/DATE_DOCUMENT")
            date_adopted = datetime.date(int(date.findtext("YEAR")), int(date.findtext("MONTH")),
                                         int(date.findtext("DAY")))
            if date_adopted:
                self.logger.debug(f"Date adopted retrieved from XML metadata for {self.celex_id}")
                return date_adopted
        except (IndexError, AttributeError, TypeError, ValueError) as e:
            self.logger.warning(f"Failed to extract date adopted from XML metadata for {self.celex_id}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error parsing XML metadata for date adopted for {self.celex_id}: {e}")

        # Fallback 2: Extract from title
        try:
            title = self.get_title()
            if title != "[Unavailable]":
                date_match = re.search(r'(\d{1,2})\s([A-Za-z]{3,9})\s(\d{4})', title)
                if date_match:
                    day = int(date_match.group(1))
                    month_str = date_match.group(2)
                    year = int(date_match.group(3))
                    month = datetime.datetime.strptime(month_str, "%B").month
                    date_adopted = datetime.date(year, month, day)
                    self.logger.debug(f"Date adopted extracted from title for {self.celex_id}")
                    return date_adopted
                else:
                    raise ValueError("Title unavailable.")
        except Exception as e:
            self.logger.warning(f"Failed to extract date adopted from HTML for {self.celex_id}: {e}")

        # Error handling if date unavailable
        self.logger.error(f"Unable to retrieve date adopted for {self.celex_id} from any source")
        return None

    def _extract_text(self, element) -> str:
        text = " ".join(t for t in element.itertext() if t.strip())
        return normalize_string(text)

    def _extract_standard_structure_recitals(self, tree) -> list[dict]:
        """Extract recitals from documents using standard structure (Official Journal format)."""
        units = []
        try:
            recitals = tree.xpath(".//*[local-name()='div' and starts-with(@id, 'rct_')]")
            self.logger.debug(f"Found {len(recitals)} recitals for {self.celex_id}")
            for recital in recitals:
                recital_id = recital.attrib.get("id", "")
                recital_text = self._extract_text(recital)
                units.append({
                    "celex_id": self.celex_id,
                    "type": "recital",
                    "number": recital_id[4:] if recital_id.startswith("rct_") else recital_id,
                    "text": recital_text
                })
        except Exception as e:
            self.logger.warning(f"Failed to extract recitals for {self.celex_id}: {e}")

        return units

    def _extract_standard_structure_articles(self, tree) -> list[dict]:
        """Extract articles from documents using standard structure (Official Journal format)."""
        units = []
        try:
            articles = tree.xpath(".//*[local-name()='div' and re:test(@id, '^art_\\d+[a-z]*$')]", namespaces=NSMAP)
            self.logger.debug(f"Found {len(articles)} articles for {self.celex_id}")
            for article in articles:
                article_id = article.attrib.get("id", "")
                title_nodes = article.xpath(".//*[local-name()='div' and @class='eli-title']")
                div_title = title_nodes[0] if title_nodes else None
                title = self._extract_text(div_title) if div_title is not None else ""

                text_parts = []
                for child in article.iterchildren():
                    child_class = child.attrib.get("class", "")
                    if child_class not in ("eli-title", "oj-ti-art"):
                        text_parts.append(self._extract_text(child))

                units.append({
                    "celex_id": self.celex_id,
                    "type": "article",
                    "number": article_id[4:] if article_id.startswith("art_") else article_id,
                    "title": title,
                    "text": " ".join(text_parts).strip()
                })
        except Exception as e:
            self.logger.warning(f"Failed to extract articles for {self.celex_id}: {e}")

        return units

    def _extract_standard_structure_annexes(self, tree) -> list[dict]:
        """Extract annexes from documents using standard structure (Official Journal format)."""
        units = []
        try:
            annexes = tree.xpath(
                ".//*[local-name()='div' and re:test(@id, '^anx_[IVXLCDMivxlcdm0-9]+$')]",
                namespaces=NSMAP)
            self.logger.debug(f"Found {len(annexes)} annexes for {self.celex_id}")
            annex_header_pattern = re.compile(r"^ANNEX\s*[IVXLCDM]*", re.IGNORECASE)
            for annex in annexes:
                annex_id = annex.attrib.get("id", "")
                title = ""
                text_parts = []

                for child in annex.iterchildren():
                    child_class = child.attrib.get("class", "")
                    if child_class == "oj-doc-ti":
                        child_text = self._extract_text(child)
                        if not annex_header_pattern.match(child_text):
                            title = child_text
                    else:
                        text_parts.append(self._extract_text(child))

                units.append({
                    "celex_id": self.celex_id,
                    "type": "annex",
                    "number": annex_id[4:] if annex_id.startswith("anx_") else annex_id,
                    "title": title,
                    "text": " ".join(text_parts).strip()
                })
        except Exception as e:
            self.logger.warning(f"Failed to extract annexes for {self.celex_id}: {e}")

        return units

    def _extract_manual_structure_recitals(self, tree) -> list[dict]:
        """Extract recitals from documents using Manual CSS class structure."""
        units = []
        try:
            recitals = tree.xpath(".//*[local-name()='p' and @class='li ManualConsidrant']")
            self.logger.debug(f"Found {len(recitals)} recitals (Manual structure) for {self.celex_id}")

            for recital in recitals:
                # Extract recital number from span.num
                num_nodes = recital.xpath(".//*[local-name()='span' and @class='num']")
                num_span = num_nodes[0] if num_nodes else None
                recital_number = ""
                if num_span is not None:
                    num_text = self._extract_text(num_span)
                    # Extract number from format like "(29)"
                    match = re.search(r'\((\d+)\)', num_text)
                    if match:
                        recital_number = match.group(1)

                # Extract full text of the recital
                recital_text = self._extract_text(recital)
                units.append({
                    "celex_id": self.celex_id,
                    "type": "recital",
                    "number": recital_number,
                    "text": normalize_string(recital_text)
                })
        except Exception as e:
            self.logger.warning(f"Failed to extract recitals (Manual structure) for {self.celex_id}: {e}")

        return units

    def _extract_manual_structure_articles(self, tree) -> list[dict]:
        """Extract articles from documents using Manual CSS class structure."""
        units = []
        try:
            # Find all article title elements
            article_titles = tree.xpath(".//*[local-name()='p' and @class='Titrearticle']")
            self.logger.debug(f"Found {len(article_titles)} articles (Manual structure) for {self.celex_id}")

            for article_title_elem in article_titles:
                # Extract article number and title from span elements
                article_number = ""
                article_title = ""

                # Find the br tag to determine text before/after it
                br_nodes = article_title_elem.xpath(".//*[local-name()='br']")
                br_tag = br_nodes[0] if br_nodes else None

                if br_tag is not None:
                    # Extract text before and after the br tag
                    text_before_br = []
                    text_after_br = []
                    br_found = False

                    # Walk through all elements in document order
                    for elem in article_title_elem.iter():
                        if elem == br_tag:
                            br_found = True
                            # Get the tail of the br tag (text immediately after <br/>)
                            if elem.tail:
                                text_after_br.append(elem.tail)
                            continue

                        # Skip the root element itself
                        if elem == article_title_elem:
                            if elem.text:
                                text_before_br.append(elem.text)
                            continue

                        # Add text and tail based on whether we've found br
                        if not br_found:
                            if elem.text:
                                text_before_br.append(elem.text)
                            if elem.tail:
                                text_before_br.append(elem.tail)
                        else:
                            if elem.text:
                                text_after_br.append(elem.text)
                            if elem.tail:
                                text_after_br.append(elem.tail)

                    # Extract article number from text before br
                    number_text = " ".join(text_before_br)
                    number_text_normalized = re.sub(r'(\d)\s+(\d)', r'\1\2', number_text)
                    match = re.search(r'Article\s+(\d+)', number_text_normalized, re.IGNORECASE)
                    if match:
                        article_number = match.group(1)

                    # Extract title from text after br
                    article_title = " ".join(text_after_br).strip()
                else:
                    # No br tag found, try to extract from spans
                    spans = article_title_elem.xpath(".//*[local-name()='span']")
                    if spans:
                        # First span should contain the article number
                        number_text = self._extract_text(spans[0])
                        number_text_normalized = re.sub(r'(\d)\s+(\d)', r'\1\2', number_text)
                        match = re.search(r'Article\s+(\d+)', number_text_normalized, re.IGNORECASE)
                        if match:
                            article_number = match.group(1)

                # Collect all content until the next article, annex, or signature
                text_parts = []
                next_elem = article_title_elem.getnext()

                # If the article title is not in the same paragraph as the article number, parse here
                if not article_title and next_elem.attrib.get("class", "") == "Titrearticle":
                    title_parts = [self._extract_text(element) for element in next_elem]
                    article_title = " ".join(title_parts).strip()
                    next_elem = next_elem.getnext()

                while next_elem is not None:
                    elem_class = next_elem.attrib.get("class", "")

                    # Stop at the next article, annex, signature, or financial statement
                    if elem_class in ("Titrearticle", "Annexetitre", "Fait", "Fichefinanciretitre"):
                        break

                    text_parts.append(self._extract_text(next_elem))

                    next_elem = next_elem.getnext()

                if article_number:
                    units.append({
                        "celex_id": self.celex_id,
                        "type": "article",
                        "number": article_number,
                        "title": normalize_string(article_title),
                        "text": normalize_string("".join(text_parts).strip())
                    })
        except Exception as e:
            self.logger.warning(f"Failed to extract articles (Manual structure) for {self.celex_id}: {e}")

        return units

    def _extract_manual_structure_annexes(self, tree) -> list[dict]:
        """Extract annexes from documents using Manual CSS class structure."""
        units = []
        try:
            # Find all annex title elements
            annex_titles = tree.xpath(".//*[local-name()='p' and @class='Annexetitre']")
            self.logger.debug(f"Found {len(annex_titles)} annexes (Manual structure) for {self.celex_id}")

            for annex_title_elem in annex_titles:
                # Extract annex title
                title_text = self._extract_text(annex_title_elem)

                # Parse annex number (e.g., "ANNEX I", "ANNEX II")
                annex_number = ""
                annex_title = ""
                match = re.search(r'ANNEX\s+([IVXLCDMivxlcdm]+)', title_text, re.IGNORECASE)
                if match:
                    annex_number = match.group(1)
                    # Everything after the annex number is the title
                    annex_title = title_text[match.end():].strip()

                # Collect all content until the next annex, signature, or end
                text_parts = []
                next_elem = annex_title_elem.getnext()

                # If the title is not in the same paragraph as the article number, parse here
                if not annex_title and next_elem.attrib.get("class", "") == "NormalCentered":
                    title_parts = [self._extract_text(element) for element in next_elem]
                    annex_title = " ".join(title_parts).strip()
                    next_elem = next_elem.getnext()

                while next_elem is not None:
                    elem_class = next_elem.attrib.get("class", "")

                    # Stop at the next annex, signature, or financial statement
                    if elem_class in ("Titrearticle", "Annexetitre", "Fait", "Fichefinanciretitre"):
                        break

                    text_parts.append(self._extract_text(next_elem))

                    next_elem = next_elem.getnext()

                units.append({
                    "celex_id": self.celex_id,
                    "type": "annex",
                    "number": annex_number,
                    "title": annex_title,
                    "text": " ".join(text_parts).strip()
                })
        except Exception as e:
            self.logger.warning(f"Failed to extract annexes (Manual structure) for {self.celex_id}: {e}")

        return units

    def _extract_text_only_units(self, tree, include_recitals=False, include_articles=False, include_annexes=False) -> \
            list[dict]:
        """Extract text units from documents using Text Only structure.

        This method handles extraction of recitals, articles, and annexes from documents
        that use the TexteOnly structure:
        - Recitals: identified by paragraphs starting with 'Whereas'
        - Articles: identified by paragraphs starting with 'Article' followed by a number
        - Annexes: identified by paragraphs starting with 'ANNEX' followed by a number/roman numeral
        """
        if not include_articles and not include_annexes and not include_recitals:
            return []

        requested_types = []
        if include_recitals:
            requested_types.append("recitals")
        if include_articles:
            requested_types.append("articles")
        if include_annexes:
            requested_types.append("annexes")
        requested_str = ", ".join(requested_types) if requested_types else "text units"

        units = []
        try:
            # Find all paragraphs within the TexteOnly div
            text_only_nodes = tree.xpath(".//*[local-name()='div' and @id='TexteOnly']")
            text_only_div = text_only_nodes[0] if text_only_nodes else None
            if text_only_div is None:
                self.logger.warning(
                    f"Failed to extract {requested_str} (Text Only structure) for {self.celex_id}: No TexteOnly div found")
                return units

            paragraphs = text_only_div.xpath(".//*[local-name()='p']")
            self.logger.debug(f"Processing {len(paragraphs)} paragraphs in Text Only structure for {self.celex_id}")

            article_pattern = re.compile(r'^Article\s+(\d+)', re.IGNORECASE)
            annex_pattern = re.compile(r'^ANNEX\s*([IVXLCDMivxlcdm0-9]*)(.*)', re.IGNORECASE)

            recital_count = 0
            i = 0

            while i < len(paragraphs):
                p = paragraphs[i]
                p_text = self._extract_text(p).strip()

                # Check for recitals (Whereas)
                if include_recitals and p_text.startswith("Whereas"):
                    recital_count += 1
                    units.append({
                        "celex_id": self.celex_id,
                        "type": "recital",
                        "number": str(recital_count),
                        "text": normalize_string(p_text)
                    })
                    i += 1
                    continue

                # Check for articles
                if include_articles:
                    article_match = re.match(article_pattern, p_text)
                    if article_match:
                        article_number = article_match.group(1)
                        article_title = ""
                        article_text_parts = []

                        # Move to next paragraphs and collect text until next article/annex
                        i += 1
                        while i < len(paragraphs):
                            next_p = paragraphs[i]
                            next_text = self._extract_text(next_p).strip()

                            # Stop if we hit another article or annex
                            if (re.match(article_pattern, next_text) or
                                    re.match(annex_pattern, next_text)):
                                break

                            article_text_parts.append(next_text)
                            i += 1

                        full_text = " ".join(article_text_parts).strip()

                        units.append({
                            "celex_id": self.celex_id,
                            "type": "article",
                            "number": article_number,
                            "title": article_title,
                            "text": normalize_string(full_text)
                        })
                        continue

                # Check for annexes
                if include_annexes:
                    annex_match = re.match(annex_pattern, p_text)
                    if annex_match:
                        annex_number = annex_match.group(1)
                        annex_title = annex_match.group(2).strip()
                        annex_text_parts = []

                        # Move to next paragraphs and collect text until next annex or end
                        i += 1
                        while i < len(paragraphs):
                            next_p = paragraphs[i]
                            next_text = self._extract_text(next_p).strip()

                            # Stop if we hit another annex
                            if re.match(annex_pattern, next_text):
                                break

                            annex_text_parts.append(next_text)
                            i += 1

                        full_text = " ".join(annex_text_parts).strip()

                        units.append({
                            "celex_id": self.celex_id,
                            "type": "annex",
                            "number": annex_number,
                            "title": normalize_string(annex_title),
                            "text": normalize_string(full_text)
                        })
                        continue

                # Move to next paragraph if no match found
                i += 1

            self.logger.debug(f"Extracted {len(units)} {requested_str} (Text Only structure) for {self.celex_id}")
        except Exception as e:
            self.logger.warning(f"Failed to extract {requested_str} (Text Only structure) for {self.celex_id}: {e}")

        return units

    def get_text_units(self, include_recitals: bool = True, include_articles: bool = True,
                       include_annexes: bool = True) -> list[dict]:
        units = []

        if not include_articles and not include_annexes and not include_recitals:
            self.logger.warning(f"All text extraction settings disabled. Skipping all text units for {self.celex_id}")
            return units

        # Try to parse XHTML first, fallback to plain HTML if XHTML fails
        tree = None
        try:
            tree = etree.fromstring(self.raw_full_text_xhtml)
            if not is_standard_structure(tree) and not is_manual_structure(tree) and not is_text_only_structure(tree):
                raise Exception(f"Invalid structure for {self.celex_id}, retry with plain html.")
            self.logger.debug(f"Successfully parsed XHTML for {self.celex_id}")
        except Exception as e:
            self.logger.warning(f"Failed to parse full text XHTML for {self.celex_id}: {e}")
            try:
                tree = html.fromstring(self.raw_full_text_plain_html)
                self.logger.info(f"Successfully parsed plain HTML for {self.celex_id}")
            except Exception as e2:
                self.logger.error(f"Failed to parse plain HTML for {self.celex_id}: {e2}")
                return units

        recitals_tree = tree
        if include_recitals and is_consolidated_celex(self.celex_id):
            self.logger.debug(f"Consolidated CELEX detected, using original CELEX {self.original_celex} for recitals")
            recitals_data_resolver = self.get_original_data_resolver_for_consolidated()

            try:
                recitals_tree = etree.fromstring(recitals_data_resolver.raw_full_text_xhtml)
                if not is_standard_structure(recitals_tree) and not is_manual_structure(
                        recitals_tree) and not is_text_only_structure(
                    recitals_tree):
                    raise Exception(f"Invalid structure for {self.celex_id}, retry with plain html.")
                self.logger.debug(
                    f"Successfully parsed recitals XHTML for original celex of consolidated {self.celex_id}")
            except Exception as e:
                self.logger.warning(
                    f"Failed to parse recitals full text XHTML for original celex of consolidated {self.celex_id}: {e}")
                try:
                    recitals_tree = html.fromstring(recitals_data_resolver.raw_full_text_plain_html)
                    self.logger.info(
                        f"Successfully parsed recitals plain HTML for original celex of consolidated {self.celex_id}")
                except Exception as e2:
                    self.logger.error(
                        f"Failed to parse recitals plain HTML for original celex of consolidated {self.celex_id}: {e2}")

        annexes_tree = tree
        if include_annexes and self.get_document_type() == "proposal":
            try:
                tree_bytes = _flatten_content_divs(get_annex_xhtml(self.celex_id, logger=self.logger),
                                                   f"annexes of {self.celex_id}", self.logger)
                annexes_tree = etree.fromstring(tree_bytes)
                if not is_standard_structure(annexes_tree) and not is_manual_structure(
                        annexes_tree) and not is_text_only_structure(
                    annexes_tree):
                    raise Exception(f"Invalid structure for {self.celex_id}, retry with plain html.")
                self.logger.debug(
                    f"Successfully parsed annexes XHTML for proposal {self.celex_id}")
            except Exception as e:
                self.logger.warning(
                    f"Failed to parse annexes full text XHTML for proposal {self.celex_id}: {e}")
                try:
                    tree_bytes = _flatten_content_divs(get_annex_plain_html(self.celex_id, logger=self.logger),
                                                       f"annexes of {self.celex_id}", self.logger)
                    annexes_tree = html.fromstring(tree_bytes)
                    self.logger.info(
                        f"Successfully parsed annexes plain HTML for proposal {self.celex_id}")
                except Exception as e2:
                    self.logger.error(
                        f"Failed to parse annexes plain HTML for proposal {self.celex_id}: {e2}")

        if tree is None:
            self.logger.error(f"No valid HTML tree available for {self.celex_id}")
            return units

        if include_recitals:
            if is_standard_structure(recitals_tree):
                self.logger.info(f"Using Standard structure parsing for recitals of {self.celex_id}")
                units.extend(self._extract_standard_structure_recitals(recitals_tree))
            elif is_manual_structure(recitals_tree):
                self.logger.info(f"Using Manual structure parsing for recitals of {self.celex_id}")
                units.extend(self._extract_manual_structure_recitals(recitals_tree))
            elif is_text_only_structure(recitals_tree):
                self.logger.info(f"Using Text Only structure parsing for recitals of {self.celex_id}")
                units.extend(self._extract_text_only_units(recitals_tree, include_recitals=True))
            else:
                self.logger.error(f"No valid structure found for recitals parsing of {self.celex_id}")
        else:
            self.logger.debug(f"Skipping recitals for {self.celex_id}")

        if include_articles:
            if is_standard_structure(tree):
                self.logger.info(f"Using Standard structure parsing for articles of {self.celex_id}")
                units.extend(self._extract_standard_structure_articles(tree))
            elif is_manual_structure(tree):
                self.logger.info(f"Using Manual structure parsing for articles of {self.celex_id}")
                units.extend(self._extract_manual_structure_articles(tree))
            elif is_text_only_structure(tree):
                self.logger.info(f"Using Text Only structure parsing for articles of {self.celex_id}")
                units.extend(self._extract_text_only_units(tree, include_articles=True))
            else:
                self.logger.error(f"No valid structure found for articles parsing of {self.celex_id}")
        else:
            self.logger.debug(f"Skipping articles for {self.celex_id}")

        if include_annexes:
            if is_standard_structure(annexes_tree):
                self.logger.info(f"Using Standard structure parsing for annexes of {self.celex_id}")
                units.extend(self._extract_standard_structure_annexes(annexes_tree))
            elif is_manual_structure(annexes_tree):
                self.logger.info(f"Using Manual structure parsing for annexes of {self.celex_id}")
                units.extend(self._extract_manual_structure_annexes(annexes_tree))
            elif is_text_only_structure(annexes_tree):
                self.logger.info(f"Using Text Only structure parsing for annexes of {self.celex_id}")
                units.extend(self._extract_text_only_units(annexes_tree, include_annexes=True))
            else:
                self.logger.error(f"No valid structure found for annexes parsing of {self.celex_id}")
        else:
            self.logger.debug(f"Skipping annexes for {self.celex_id}")

        self.logger.debug(f"Extracted {len(units)} text units for {self.celex_id}")
        return units

    def get_relations(self, include_relations: bool = True,
                      include_original_act_relations_for_consolidated_texts: bool = False) -> list[dict]:
        if not include_relations:
            self.logger.debug(f"Relations extraction disabled. Skipping relations extraction for {self.celex_id}")
            return []

        relations = []
        try:
            sparql_result = self.sparql_query_result["relations"]
            for relation_type in sparql_result:
                self.logger.debug(
                    f"Found {len(sparql_result[relation_type])} {relation_type} relations for {self.celex_id}")
                for target in sparql_result[relation_type]:
                    if target:
                        relations.append({
                            "celex_source": self.celex_id,
                            "celex_target": target,
                            "relation_type": relation_type
                        })

            if include_original_act_relations_for_consolidated_texts and is_consolidated_celex(self.celex_id):
                self.logger.debug(
                    f"Document {self.celex_id} is a consolidated text, checking for original act relations")
                self.logger.debug(f"Fetching relations for original act {self.original_celex}")

                try:
                    original_data_resolver = self.get_original_data_resolver_for_consolidated()
                    original_relations = original_data_resolver.get_relations()
                    relations.extend(original_relations)
                except Exception as e:
                    self.logger.warning(f"Failed to extract original act relations for {self.original_celex}: {e}")

        except Exception as e:
            self.logger.warning(f"Failed to extract relations for {self.celex_id}: {e}")
        self.logger.debug(f"Extracted {len(relations)} relations for {self.celex_id}")
        return relations
