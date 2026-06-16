include ../../../common.mk

# --- src_songs_rock
TABLE_NAME=src_rock_songs
SCRIPT_PRE_KEBAB=dev-usw2-songs
SCRIPT_POST_KEBAB=$(subst _,-,$(TABLE_NAME))

DDL_STATEMENT=$(SCRIPT_PRE_KEBAB)-ddl-$(SCRIPT_POST_KEBAB)
DROP_DDL_STATEMENT=$(SCRIPT_PRE_KEBAB)-drop-$(SCRIPT_POST_KEBAB)
DDL_SCRIPT=./sql-scripts/ddl.$(TABLE_NAME).sql
DML_STATEMENT=$(SCRIPT_PRE_KEBAB)-dml-$(SCRIPT_POST_KEBAB)
DML_SCRIPT=./sql-scripts/dml.$(TABLE_NAME).sql
DML_TESTS_SCRIPT=./tests/dml.$(TABLE_NAME).sql

# --- Entry point
create_src_songs_rock: create_flink_ddl  create_flink_dml

create_raw_rock_songs:
	$(call create_flink_statement, $(SCRIPT_PRE_KEBAB)-ddl-raw-rock-songs, ./tests/ddl.raw_rock_songs.sql) 
	$(call delete_flink_statement, $(SCRIPT_PRE_KEBAB)-ddl-raw-rock-songs )

insert_raw_rock_songs:
	$(call create_flink_statement, $(SCRIPT_PRE_KEBAB)-insert-raw-rock-songs, ./tests/insert_raw_rock_songs.sql) 
	$(call delete_flink_statement, $(SCRIPT_PRE_KEBAB)-insert-raw-rock-songs )
	

# --- Topic used as source
list_record_data_from_topic:
	$(call list_topic_content, $(TABLE_NAME))

# --- DDL 
create_flink_ddl: 
	$(call create_flink_statement, $(DDL_STATEMENT), $(DDL_SCRIPT)) 
	$(call delete_flink_statement, $(DDL_STATEMENT) )


describe_flink_ddl:
	$(call describe_flink_statement,  $(DDL_STATEMENT))

delete_flink_ddl:
	$(call delete_flink_statement, $(DDL_STATEMENT))

drop_table: 
	$(call drop_table, $(DROP_DDL_STATEMENT), $(TABLE_NAME))
	$(call delete_flink_statement, $(DROP_DDL_STATEMENT))

# --- DML
describe_flink_dml:
	$(call describe_flink_statement,  $(DML_STATEMENT))

create_flink_dml:
	$(call create_flink_statement, $(DML_STATEMENT), $(DML_SCRIPT)) 

create_flink_dev:
	$(call create_flink_statement, $(DML_STATEMENT), $(DML_TESTS_SCRIPT)) 

delete_flink_dml:
	$(call delete_flink_statement, $(DML_STATEMENT))

pause_flink_dml:
	$(call pause_flink_statement $(DML_STATEMENT))

resume_flink_dml:
	$(call resume_flink_statement $(DML_STATEMENT))

delete_schemas:
	$(call delete_schema, $(TABLE_NAME)-value)
	$(call delete_schema, $(TABLE_NAME)-key)


delete_flink_statements: delete_flink_ddl delete_flink_dml

delete_data: delete_schemas