--- This Query creates relational tables with PK publication_number (excluding the citation map)
--- CREATED: NOV 22, 2025
--- LAST UPDATED: NOV 22, 2025


--create pub_general
CREATE TABLE
  sc-patent-networks.sc_chapter3.pub_general AS (
  SELECT
    DISTINCT main.publication_number,
    main.family_id,
    main.priority_date,
    main.filing_date,
    main.grant_date,
  FROM
    `bigquery-public-data.patents.publications` main   LEFT JOIN
    UNNEST(ipc) AS ip
  WHERE
    REGEXP_CONTAINS(ip.code, r'^(B81B|B81C|B82B|C23C|C30B|G01N|G01R|G02B|G02F|G03F|H01J|H01L|H01R|H01S|H03F|H03K|H04B|H04L|H05K|H10)') );


---create pub_ipc

CREATE TABLE
  sc-patent-networks.sc_chapter3.pub_ipc AS (
  SELECT
    main.publication_number,
    ip.code AS ipc
  FROM
    `bigquery-public-data.patents.publications` main   LEFT JOIN
    UNNEST(ipc) AS ip
  WHERE
    REGEXP_CONTAINS(ip.code, r'^(B81B|B81C|B82B|C23C|C30B|G01N|G01R|G02B|G02F|G03F|H01J|H01L|H01R|H01S|H03F|H03K|H04B|H04L|H05K|H10)') );


---create pub_assignee
CREATE TABLE
  sc-patent-networks.sc_chapter3.pub_assignee AS (
  SELECT
    DISTINCT main.publication_number,
          assig.name AS assignee_name,
          assig.country_code AS assignee_country  
  FROM
    `bigquery-public-data.patents.publications` main   LEFT JOIN
    UNNEST(ipc) AS ip  LEFT JOIN
    UNNEST(assignee_harmonized) AS assig 
  WHERE
    REGEXP_CONTAINS(ip.code, r'^(B81B|B81C|B82B|C23C|C30B|G01N|G01R|G02B|G02F|G03F|H01J|H01L|H01R|H01S|H03F|H03K|H04B|H04L|H05K|H10)') );

---create pub_inventor

CREATE TABLE
  sc-patent-networks.sc_chapter3.pub_inventor AS (
  SELECT
    DISTINCT main.publication_number,
          inv.name AS inventor_name,
          inv.country_code AS inventor_country  
  FROM
    `bigquery-public-data.patents.publications` main   LEFT JOIN
    UNNEST(ipc) AS ip  LEFT JOIN
    UNNEST(inventor_harmonized) AS inv 
  WHERE
    REGEXP_CONTAINS(ip.code, r'^(B81B|B81C|B82B|C23C|C30B|G01N|G01R|G02B|G02F|G03F|H01J|H01L|H01R|H01S|H03F|H03K|H04B|H04L|H05K|H10)') );

---create pub_citations
CREATE TABLE
  sc-patent-networks.sc_chapter3.pub_citations AS (
  SELECT
    DISTINCT main.publication_number,
        cite.publication_number as out_citations_pubs 
  FROM
    `bigquery-public-data.patents.publications` main   LEFT JOIN
    UNNEST(ipc) AS ip  LEFT JOIN
    UNNEST(citation) AS cite  
  WHERE
    REGEXP_CONTAINS(ip.code, r'^(B81B|B81C|B82B|C23C|C30B|G01N|G01R|G02B|G02F|G03F|H01J|H01L|H01R|H01S|H03F|H03K|H04B|H04L|H05K|H10)') );


--- create pub_title - and cleaning title to be 1:1
CREATE TABLE
  sc-patent-networks.sc_chapter3.pub_title AS (
WITH ranked AS (
  SELECT
    main.publication_number,
    titl.text as title_text,
    titl.language as title_language,
    titl.truncated as title_truncated,
    ROW_NUMBER() OVER (
      PARTITION BY main.publication_number
      ORDER BY
        -- 1) Prefer titles where the language is english if available
        CASE WHEN titl.language = 'en' THEN 1 ELSE 0 END DESC,

        -- 2) Then prefer the longest string 
        LENGTH(titl.text) DESC
    ) AS selection_criteria
  FROM
    `bigquery-public-data.patents.publications` main   LEFT JOIN
    UNNEST(ipc) AS ip  LEFT JOIN
    UNNEST(title_localized) AS titl  
  WHERE
    REGEXP_CONTAINS(ip.code, r'^(B81B|B81C|B82B|C23C|C30B|G01N|G01R|G02B|G02F|G03F|H01J|H01L|H01R|H01S|H03F|H03K|H04B|H04L|H05K|H10)') 
)
SELECT
  ranked.publication_number,
  title_text,
  title_language,
  title_truncated,
FROM ranked
--selection_criteria=1 is the preferred text. If there are ties (both english and same length, take the first entry)
QUALIFY ROW_NUMBER() OVER (PARTITION BY publication_number ORDER BY selection_criteria ASC) = 1 
);


--- create pub_abstract - and cleaning abstract to be 1:1
CREATE TABLE
  sc-patent-networks.sc_chapter3.pub_abstract AS (
WITH ranked AS (
  SELECT
    main.publication_number,
    abstr.text as abstract_text,
    abstr.language as abstract_language,
    abstr.truncated as abstract_truncated,
    ROW_NUMBER() OVER (
      PARTITION BY main.publication_number
      ORDER BY
        -- 1) Prefer titles where the language is english if available
        CASE WHEN abstr.language = 'en' THEN 1 ELSE 0 END DESC,

        -- 2) Then prefer the longest string 
        LENGTH(abstr.text) DESC
    ) AS selection_criteria
  FROM
    `bigquery-public-data.patents.publications` main   LEFT JOIN
    UNNEST(ipc) AS ip  LEFT JOIN
    UNNEST(abstract_localized) AS abstr  
  WHERE
    REGEXP_CONTAINS(ip.code, r'^(B81B|B81C|B82B|C23C|C30B|G01N|G01R|G02B|G02F|G03F|H01J|H01L|H01R|H01S|H03F|H03K|H04B|H04L|H05K|H10)') 
)
SELECT
  ranked.publication_number,
  abstract_text,
  abstract_language,
  abstract_truncated,
FROM ranked
--selection_criteria=1 is the preferred text. If there are ties (both english and same length, take the first entry)
QUALIFY ROW_NUMBER() OVER (PARTITION BY publication_number ORDER BY selection_criteria ASC) = 1 
);


