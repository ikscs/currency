import datetime
from sqlalchemy import create_engine
import traceback

LOG_FNAME = "currency_log.txt"

try:
    from core.credentials import DB
except Exception:
    from credentials import DB

dbms = DB.get('dbms', 'mysql')
if dbms == 'mysql':
    engine = create_engine(f'mysql+pymysql://{DB["user"]}:{DB["password"]}@{DB["host"]}/{DB["database"]}')
else:
    #sqlite only
    engine = create_engine(f'sqlite:///{DB["file"]}', echo=False)

def write2database(data, table):
    with engine.connect() as conn:
        for d in data:
            query_string = f'INSERT INTO {table} VALUES({d[0]}, DATE(\"{str(d[1])}\"), \"{d[2]}\", {d[3]})'
            try:
                conn.execute(query_string)
            except Exception as err:
                print(str(err))
                if 'Duplicate' in str(err):
                    pass
                log_error(f'Problem with upload to {table}')

def log_error(s):
     _log(s, is_error = True)

def log_info(s):
     _log(s, is_error = False)

def _log(s, is_error):
    with open(LOG_FNAME, 'a', encoding='utf-8') as f:
        f.write('\n' + str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + '\n')
        f.write(s + '\n')
        if is_error:
            traceback.print_exc(file=f)
