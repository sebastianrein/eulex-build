import logging
from datetime import datetime, date
from urllib.parse import quote

from SPARQLWrapper import SPARQLWrapper

from eulexbuild.utils import normalize_string


def get_sparql_request(query: str, logger: logging.Logger = None) -> dict:
    sparql = SPARQLWrapper("https://publications.europa.eu/webapi/rdf/sparql")
    sparql.setQuery(query)
    sparql.setReturnFormat("json")
    query_result = sparql.query()
    if logger:
        logger.debug(f"SPARQL query executed: '{normalize_string(query)}'")
    return query_result.convert()


def _parse_value(value_obj: dict) -> str | int | float | bool | date | datetime:
    value_string = value_obj.get("value", "")
    value_type = value_obj.get("type", "uri")
    datatype = value_obj.get("datatype", "")

    if value_type == "literal" and datatype:
        if "boolean" in datatype:
            return value_string.lower() == "true" or value_string == "1"
        elif "integer" in datatype or "positiveInteger" in datatype:
            return int(value_string)
        elif "decimal" in datatype or "double" in datatype or "float" in datatype:
            return float(value_string)
        elif "date" in datatype and "dateTime" not in datatype:
            # Parse ISO date format (YYYY-MM-DD)
            return datetime.strptime(value_string, "%Y-%m-%d").date()
        elif "dateTime" in datatype:
            # Parse ISO datetime format
            try:
                return datetime.fromisoformat(value_string.replace("Z", "+00:00"))
            except ValueError:
                return value_string
        elif "gYear" in datatype:
            # Parse year only
            return int(value_string)

    return value_string


def get_all_properties(celex_id: str, logger: logging.Logger = None) -> dict:
    query = f"""
        PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        
        SELECT DISTINCT ?data_type ?value
        WHERE {{
            ?work owl:sameAs <http://publications.europa.eu/resource/celex/{quote(celex_id, safe='')}> .
            
            # titles
            {{
                ?expression cdm:expression_belongs_to_work ?work .
                ?expression cdm:expression_title ?value .
                
                FILTER EXISTS {{
                    ?expression cdm:expression_uses_language ?lang .
                    FILTER(STRENDS(STR(?lang), "ENG"))
                }}
                
                BIND("title" AS ?data_type)
            }}
            UNION
            
            # document date
            {{
                ?work cdm:work_date_document ?value .
                BIND("date" AS ?data_type)
            }}
            UNION
            
            # cites
            {{
                ?work cdm:work_cites_work ?w .
                ?w cdm:resource_legal_id_celex ?value .
                BIND("cites" AS ?data_type)
            }}
            UNION
            
            # amends
            {{
                ?work cdm:resource_legal_amends_resource_legal ?w .
                ?w cdm:resource_legal_id_celex ?value .
                BIND("amends" AS ?data_type)
            }}
            UNION
            
            # adopts
            {{
                ?work cdm:resource_legal_adopts_resource_legal ?w .
                ?w cdm:resource_legal_id_celex ?value .
                BIND("adopts" AS ?data_type)
            }}
            UNION
            
            # based on
            {{
                ?work cdm:resource_legal_based_on_resource_legal ?w .
                ?w cdm:resource_legal_id_celex ?value .
                BIND("based_on" AS ?data_type)
            }}
            UNION
            
            # proposes to amend
            {{
                ?work cdm:resource_legal_proposes_to_amend_resource_legal ?w .
                ?w cdm:resource_legal_id_celex ?value .
                BIND("proposes_to_amend" AS ?data_type)
            }}
            UNION
            
            # consolidates
            {{
                ?work cdm:act_consolidated_consolidates_resource_legal ?w .
                ?w cdm:resource_legal_id_celex ?value .
                BIND("consolidates" AS ?data_type)
            }}
        }}
        """

    results = get_sparql_request(query, logger)
    bindings = results.get("results", {}).get("bindings", [])

    # Transform bindings into dict
    relation_typeset = {"cites", "amends", "adopts", "based_on", "proposes_to_amend", "consolidates"}

    properties = {"relations": {}}
    for binding in bindings:
        data_type = _parse_value(binding.get("data_type", {}))
        value = _parse_value(binding.get("value", {}))

        if data_type in relation_typeset:
            if data_type not in properties["relations"]:
                properties["relations"][data_type] = []
            properties["relations"][data_type].append(value)
        else:
            properties[data_type] = value

    return properties


def get_procedure_celex_ids(procedure_ids: set[str], logger: logging.Logger = None) -> set[str]:
    query = f"""
        PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        
        SELECT DISTINCT ?procedure ?proposalCelex ?availableWorkCelex
        WHERE {{
            VALUES ?procedure {{
                {" ".join(f'"{pid}"' for pid in procedure_ids)}
        }}
        
        ?dossier cdm:procedure_code_interinstitutional_reference_procedure ?ref .
        ?proposal cdm:work_part_of_dossier ?dossier .
        ?proposal cdm:resource_legal_id_celex ?proposalCelex .
        
        OPTIONAL {{
            ?work cdm:resource_legal_adopts_resource_legal ?proposal .
            ?work cdm:resource_legal_id_celex ?workCelex .
        }}
        
        FILTER(CONTAINS(STR(?ref), ?procedure))
        FILTER(CONTAINS(STR(?proposalCelex), "PC"))
        FILTER(!CONTAINS(STR(?proposalCelex), "("))
        
        BIND(COALESCE(?workCelex, "") AS ?availableWorkCelex)
        }}
    """

    results = get_sparql_request(query, logger)
    bindings = results.get("results", {}).get("bindings", [])

    # Parse results into a dictionary with procedure as key
    procedure_data = set()
    for binding in bindings:
        procedure = _parse_value(binding.get("procedure", {}))
        proposal_celex = _parse_value(binding.get("proposalCelex", {}))
        available_work_celex = _parse_value(binding.get("availableWorkCelex", {}))

        if available_work_celex:
            procedure_data.add(available_work_celex)
            if logger:
                logger.debug(f"Procedure '{procedure}' resolved to adopted legislation '{available_work_celex}'.")
        elif proposal_celex:
            procedure_data.add(proposal_celex)
            if logger:
                logger.debug(f"Procedure '{procedure}' resolved to proposal '{proposal_celex}'.")
        else:
            if logger:
                logger.warning(f"Couldn't resolve '{procedure}'. Skipping.")

    return procedure_data


def get_descriptive_celex_ids(start_date: date | None = None, end_date: date | None = None,
                              eurovoc_uris: set[str] | None = None,
                              include_regulations: bool = True, include_directives: bool = True,
                              include_decisions: bool = True, include_proposals: bool = False,
                              include_corrigenda: bool = False,
                              include_consolidated_texts: bool = False, include_national_transpositions: bool = False,
                              logger: logging.Logger = None
                              ) -> set[str]:
    # Build date filters conditionally
    date_filters = ""
    if start_date is not None:
        date_filters += f'FILTER(?date > "{start_date.strftime("%Y-%m-%d")}"^^xsd:date)'
    if end_date is not None:
        date_filters += f'FILTER(?date < "{end_date.strftime("%Y-%m-%d")}"^^xsd:date)'

    # Build eurovoc filters conditionally
    eurovoc_filter = ""
    if eurovoc_uris:
        eurovoc_values = " ".join(f'<{uri}>' for uri in eurovoc_uris)
        eurovoc_filter += f"""
            ?work cdm:work_is_about_concept_eurovoc ?eurovoc . 
            VALUES ?eurovoc {{ {eurovoc_values} }}
        """

    # Build document type filters conditionally
    type_conditions = []
    if include_regulations:
        type_conditions.append('?type = "R"^^xsd:string')
    if include_directives:
        type_conditions.append('?type = "L"^^xsd:string')
    if include_decisions:
        type_conditions.append('?type = "D"^^xsd:string')
    if include_proposals:
        type_conditions.append('?type = "PC"^^xsd:string')
    type_filter = ""
    if type_conditions:
        type_filter = f'FILTER({" || ".join(type_conditions)})'

    # Build celex sector type filters conditionally
    sector_conditions = ['?sector = "3"^^xsd:string']
    if include_proposals:
        sector_conditions.append('?sector = "5"^^xsd:string')
    if include_consolidated_texts:
        sector_conditions.append('?sector = "0"^^xsd:string')
    if include_national_transpositions:
        sector_conditions.append('?sector = "7"^^xsd:string')
    sector_filter = f'FILTER({" || ".join(sector_conditions)})'

    corrigenda_filter = '' if include_corrigenda else 'FILTER(!REGEX(STR(?celex), "\\\\([0-9]{2}\\\\)$"))'

    query = f"""
        PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        
        SELECT DISTINCT ?celex
        WHERE {{
            ?work cdm:work_date_document ?date .
            ?work cdm:resource_legal_type ?type .
            ?work cdm:resource_legal_id_celex ?celex .
            ?work cdm:resource_legal_id_sector ?sector .
            {eurovoc_filter}
            {date_filters}
            {type_filter}
            {sector_filter}
            {corrigenda_filter}
        }} 
    """
    results = get_sparql_request(query, logger)
    bindings = results.get("results", {}).get("bindings", [])
    return set(_parse_value(b.get("celex", {})) for b in bindings if b.get("celex"))


def get_eurovoc_labels_for_keywords(keywords: set[str], logger: logging.Logger = None) -> dict[
    str, dict[str, set[str]]]:
    preferred_labels = _get_eurovoc_labels(keywords, "prefLabel", logger)
    alternative_labels = _get_eurovoc_labels(keywords, "altLabel", logger)

    result = {}
    for keyword, concepts in preferred_labels.items():
        result[keyword] = {concept: set(labels) for concept, labels in concepts.items()}

    for keyword, concepts in alternative_labels.items():
        if keyword in result:
            for concept, labels in concepts.items():
                if concept in result[keyword]:
                    result[keyword][concept].update(labels)
                else:
                    result[keyword][concept] = set(labels)
        else:
            result[keyword] = {concept: set(labels) for concept, labels in concepts.items()}

    return result


def _get_eurovoc_labels(keywords: set[str], label_type: str, logger: logging.Logger = None) -> dict[
    str, dict[str, set[str]]]:
    if not keywords or not label_type or label_type not in {"prefLabel", "altLabel"}:
        return {}

    query = f"""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX eurovoc: <http://eurovoc.europa.eu/>

        SELECT DISTINCT ?concept ?label ?keyword
        WHERE {{
            VALUES ?keyword {{
            {" ".join(f'"{kw}"' for kw in keywords)}
          }}

          ?concept skos:inScheme eurovoc:100141 .
          ?concept skos:{label_type} ?label .

          FILTER(
            LANGMATCHES(LANG(?label), "en") &&
            CONTAINS(LCASE(STR(?label)), LCASE(?keyword))
          )
        }}
        ORDER BY ?keyword ?label
    """
    results = get_sparql_request(query, logger)
    bindings = results.get("results", {}).get("bindings", [])

    # Build nested dictionary: keyword -> {concept -> set(labels)}
    result = {}
    for b in bindings:
        if not (b.get("keyword") and b.get("label") and b.get("concept")):
            continue

        keyword = _parse_value(b.get("keyword", {}))
        label = _parse_value(b.get("label", {}))
        concept = _parse_value(b.get("concept", {}))

        if keyword not in result:
            result[keyword] = {}
        if concept not in result[keyword]:
            result[keyword][concept] = set()
        result[keyword][concept].add(label)

    return result
