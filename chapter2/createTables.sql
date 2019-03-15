 
-- creates the table to hold doubletons
CREATE TABLE fc_project_tags_pairs (
  tag1 varchar(255) NOT NULL,
  tag2 varchar(255) NOT NULL,
  num_projs int(11) NOT NULL
);

-- creates the table to hold tripletons
create table fc_project_tag_triples (
  tag1 varchar(255) NOT NULL,
  tag2 varchar(255) NOT NULL,
  tag3 varchar(255) NOT NULL,
  num_projs int(11) NOT NULL
) ;

