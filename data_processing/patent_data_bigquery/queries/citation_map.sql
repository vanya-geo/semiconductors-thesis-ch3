CREATE TABLE sc-patent-networks.sc_chapter3.citation_map_pub AS(
WITH all_citations AS (
  select
  main.publication_number as citing_pub_number,
  cite.publication_number as receiving_pub_number
  from bigquery-public-data.patents.publications main left join unnest(citation) as cite
)
select distinct
  all_cite.citing_pub_number,
  all_cite.receiving_pub_number
  FROM all_citations AS all_cite
  INNER JOIN sc-patent-networks.sc_chapter3.pub_general AS sc_cite
  ON all_cite.citing_pub_number = sc_cite.publication_number

  UNION ALL

  SELECT distinct
    all_cite.citing_pub_number,
    all_cite.receiving_pub_number
  FROM all_citations AS all_cite
  INNER JOIN sc-patent-networks.sc_chapter3.pub_general AS sc_cite 
  ON  all_cite.receiving_pub_number = sc_cite.publication_number
);



CREATE TABLE sc-patent-networks.sc_chapter3.citation_map_fam AS(
WITH all_citations AS (
  select distinct
  main.family_id as citing_family_id,
  receiving.family_id as receiving_family_id
  from bigquery-public-data.patents.publications main left join unnest(citation) as cite
  left join bigquery-public-data.patents.publications receiving on cite.publication_number=receiving.publication_number
)
select distinct
  all_cite.citing_family_id,
  all_cite.receiving_family_id
  FROM all_citations AS all_cite
  INNER JOIN sc-patent-networks.sc_chapter3.pub_general AS sc_cite
  ON all_cite.citing_family_id = sc_cite.family_id
  WHERE citing_family_id!="-1" and receiving_family_id!="-1" --some publications have family_id="-1"

  UNION ALL

  SELECT distinct
  all_cite.citing_family_id,
  all_cite.receiving_family_id
  FROM all_citations AS all_cite
  INNER JOIN sc-patent-networks.sc_chapter3.pub_general AS sc_cite 
  ON  all_cite.receiving_family_id = sc_cite.family_id
  WHERE citing_family_id!="-1" and receiving_family_id!="-1"
);

