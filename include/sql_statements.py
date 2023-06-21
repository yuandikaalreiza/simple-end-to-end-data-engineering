create_invoice_table = """
    CREATE TABLE IF NOT EXIST invoice (
        invoice_id int,
        invoice_date text,
        country_of_origin text,
        seller text,
        distribution_area text,
        total_price int
    );
"""

create_invoice_detail_table = """
    CREATE TABLE IF NOT EXIST invoice_detail (
        invoice_id int,
        brand text,
        type_ text,
        price int,
        quantity int
    );
"""