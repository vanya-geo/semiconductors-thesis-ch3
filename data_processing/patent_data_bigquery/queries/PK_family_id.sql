--- THIS QUERY CREATES ALL TABLES WITH PK FAMILY_ID
--- NB: this file is to be executed after PK_publication_number
--- CREATED: NOV 22, 2025
--- LAST UPDATED: NOV 22, 2025


---CREATE fam_general
CREATE TABLE sc-patent-networks.sc_chapter3.fam_general AS(
SELECT
  family_id,
  MIN(priority_date) AS priority_date,
  MAX(filing_date) AS last_filing_date,
  MIN(grant_date) AS first_grant_date,
  MAX(grant_date) AS last_grant_date,
  CASE WHEN MAX(grant_date)!=0 THEN 1 ELSE 0 END AS granted_anywhere
FROM `sc-patent-networks`.`sc_chapter3`.`pub_general`
GROUP BY family_id
);


-- create fam_country_dates
CREATE TABLE sc-patent-networks.sc_chapter3.fam_country_date AS(
SELECT
  family_id,
  SUBSTR(publication_number, 1, 2) AS country_code,
  grant_date
FROM sc-patent-networks.sc_chapter3.pub_general
);


-- create fam_ipc

CREATE TABLE sc-patent-networks.sc_chapter3.fam_ipc AS(
SELECT
  DISTINCT family_id,
  ipc
FROM sc-patent-networks.sc_chapter3.pub_general gen 
left join sc-patent-networks.sc_chapter3.pub_ipc ip 
on gen.publication_number=ip.publication_number 
);


-- create fam_citations_p - family id citing publication_number

CREATE TABLE sc-patent-networks.sc_chapter3.fam_citations_p AS(
SELECT
  DISTINCT family_id,
  cite.out_citations_pubs AS out_citations_pubs
FROM sc-patent-networks.sc_chapter3.pub_general gen 
left join sc-patent-networks.sc_chapter3.pub_citations cite 
on gen.publication_number=cite.publication_number 
);


-- create fam_assignee

CREATE TABLE sc-patent-networks.sc_chapter3.fam_assignee AS(
SELECT
  DISTINCT family_id,
  assig.assignee_name AS assignee_name,
  assig.assignee_country AS assignee_country
FROM sc-patent-networks.sc_chapter3.pub_general gen 
left join sc-patent-networks.sc_chapter3.pub_assignee assig 
on gen.publication_number=assig.publication_number 
);


-- create fam_assignee

CREATE TABLE sc-patent-networks.sc_chapter3.fam_inventor AS(
SELECT
  DISTINCT family_id,
  inv.inventor_name AS inventor_name,
  inv.inventor_country AS inventor_country
FROM sc-patent-networks.sc_chapter3.pub_general gen 
left join sc-patent-networks.sc_chapter3.pub_inventor inv 
on gen.publication_number=inv.publication_number 
);


-- create fam_inventor

CREATE TABLE sc-patent-networks.sc_chapter3.fam_inventor AS(
SELECT
  DISTINCT family_id,
  inv.inventor_name AS inventor_name,
  inv.inventor_country AS inventor_country
FROM sc-patent-networks.sc_chapter3.pub_general gen 
left join sc-patent-networks.sc_chapter3.pub_inventor inv 
on gen.publication_number=inv.publication_number 
);



--- create fam_title - and cleaning title to be 1:1
CREATE TABLE
  sc-patent-networks.sc_chapter3.fam_title AS (
WITH ranked AS (
  SELECT
    DISTINCT
    main.family_id,
    titl.text as title_text,
    titl.language as title_language,
    titl.truncated as title_truncated,
    ROW_NUMBER() OVER (
      PARTITION BY main.family_id
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
SELECT DISTINCT
  ranked.family_id,
  title_text,
  title_language,
  title_truncated,
FROM ranked
--selection_criteria=1 is the preferred text. If there are ties (both english and same length, take the first entry)
QUALIFY ROW_NUMBER() OVER (PARTITION BY family_id ORDER BY selection_criteria ASC) = 1 
);



--- create fam_abstract - and cleaning abstract to be 1:1
CREATE TABLE
  sc-patent-networks.sc_chapter3.fam_abstract AS (
WITH ranked AS (
  SELECT DISTINCT
    main.family_id,
    abstr.text as abstract_text,
    abstr.language as abstract_language,
    abstr.truncated as abstract_truncated,
    ROW_NUMBER() OVER (
      PARTITION BY main.family_id
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
SELECT DISTINCT
  ranked.family_id,
  abstract_text,
  abstract_language,
  abstract_truncated,
FROM ranked
--selection_criteria=1 is the preferred text. If there are ties (both english and same length, take the first entry)
QUALIFY ROW_NUMBER() OVER (PARTITION BY family_id ORDER BY selection_criteria ASC) = 1 
);
