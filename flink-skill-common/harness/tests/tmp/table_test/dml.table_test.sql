insert into table_test
    select
        id,
        name,
        product_table.product,
        product_table.product_name
    from source_table
    left join product_table on source_table.product_id = product_table.id;