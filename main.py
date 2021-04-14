from src.rbk_parser import RBKParser
import logging
import sqlite3

logger = logging.getLogger(__name__)

con = sqlite3.connect('data/rbk_inflation.db')

with con:
    con.execute("""create table if not exists News
                   (id integer primary key, date text, title text, url text, content text)
                """)

parser = RBKParser('инфляция', con)
parser.start_parsing()
