  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
mysql.connector.errors.IntegrityError: 1062 (23000): Duplicate entry '5-6-55789' for key 'candidatos.uk_candidato'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/transformers/normalizer.py", line 319, in ensure_candidato
    self.loader.execute_query(
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/loaders/mysql_loader.py", line 19, in execute_query
    result = conn.execute(stmt, params or {})
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1415, in execute
    return meth(
           ^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 523, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1637, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1842, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1982, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2351, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
sqlalchemy.exc.IntegrityError: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-55789' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 308, 'cid': 6, 'pid': 14, 'nr': '55789', 'nm': 'ROMULO SOARES'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
2026-03-15 00:01:27,027 - ERROR - ensure_candidato: erro ao processar NR_VOTAVEL=44400, NM_VOTAVEL=RAFAELA CRISTINA: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-44400' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 311, 'cid': 6, 'pid': 1, 'nr': '44400', 'nm': 'RAFAELA CRISTINA'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
mysql.connector.errors.IntegrityError: 1062 (23000): Duplicate entry '5-6-44400' for key 'candidatos.uk_candidato'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/transformers/normalizer.py", line 319, in ensure_candidato
    self.loader.execute_query(
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/loaders/mysql_loader.py", line 19, in execute_query
    result = conn.execute(stmt, params or {})
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1415, in execute
    return meth(
           ^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 523, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1637, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1842, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1982, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2351, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
sqlalchemy.exc.IntegrityError: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-44400' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 311, 'cid': 6, 'pid': 1, 'nr': '44400', 'nm': 'RAFAELA CRISTINA'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
2026-03-15 00:01:27,030 - ERROR - ensure_candidato: erro ao processar NR_VOTAVEL=11024, NM_VOTAVEL=JADY: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-11024' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 277, 'cid': 6, 'pid': 16, 'nr': '11024', 'nm': 'JADY'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
mysql.connector.errors.IntegrityError: 1062 (23000): Duplicate entry '5-6-11024' for key 'candidatos.uk_candidato'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/transformers/normalizer.py", line 319, in ensure_candidato
    self.loader.execute_query(
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/loaders/mysql_loader.py", line 19, in execute_query
    result = conn.execute(stmt, params or {})
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1415, in execute
    return meth(
           ^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 523, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1637, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1842, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1982, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2351, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
sqlalchemy.exc.IntegrityError: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-11024' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 277, 'cid': 6, 'pid': 16, 'nr': '11024', 'nm': 'JADY'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
2026-03-15 00:01:27,033 - ERROR - ensure_candidato: erro ao processar NR_VOTAVEL=77888, NM_VOTAVEL=HUMBERTO SILVA: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-77888' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 154, 'cid': 6, 'pid': 22, 'nr': '77888', 'nm': 'HUMBERTO SILVA'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
mysql.connector.errors.IntegrityError: 1062 (23000): Duplicate entry '5-6-77888' for key 'candidatos.uk_candidato'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/transformers/normalizer.py", line 319, in ensure_candidato
    self.loader.execute_query(
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/loaders/mysql_loader.py", line 19, in execute_query
    result = conn.execute(stmt, params or {})
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1415, in execute
    return meth(
           ^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 523, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1637, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1842, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1982, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2351, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
sqlalchemy.exc.IntegrityError: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-77888' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 154, 'cid': 6, 'pid': 22, 'nr': '77888', 'nm': 'HUMBERTO SILVA'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
2026-03-15 00:01:27,036 - ERROR - ensure_candidato: erro ao processar NR_VOTAVEL=55233, NM_VOTAVEL=PASTORA ESMERALDA: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-55233' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 190, 'cid': 6, 'pid': 14, 'nr': '55233', 'nm': 'PASTORA ESMERALDA'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
mysql.connector.errors.IntegrityError: 1062 (23000): Duplicate entry '5-6-55233' for key 'candidatos.uk_candidato'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/transformers/normalizer.py", line 319, in ensure_candidato
    self.loader.execute_query(
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/loaders/mysql_loader.py", line 19, in execute_query
    result = conn.execute(stmt, params or {})
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1415, in execute
    return meth(
           ^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 523, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1637, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1842, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1982, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2351, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
sqlalchemy.exc.IntegrityError: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-55233' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 190, 'cid': 6, 'pid': 14, 'nr': '55233', 'nm': 'PASTORA ESMERALDA'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
2026-03-15 00:01:27,040 - ERROR - ensure_candidato: erro ao processar NR_VOTAVEL=70123, NM_VOTAVEL=ISAC JUNIOR: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-70123' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 161, 'cid': 6, 'pid': 23, 'nr': '70123', 'nm': 'ISAC JUNIOR'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
mysql.connector.errors.IntegrityError: 1062 (23000): Duplicate entry '5-6-70123' for key 'candidatos.uk_candidato'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/transformers/normalizer.py", line 319, in ensure_candidato
    self.loader.execute_query(
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/loaders/mysql_loader.py", line 19, in execute_query
    result = conn.execute(stmt, params or {})
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1415, in execute
    return meth(
           ^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 523, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1637, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1842, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1982, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2351, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
sqlalchemy.exc.IntegrityError: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-70123' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 161, 'cid': 6, 'pid': 23, 'nr': '70123', 'nm': 'ISAC JUNIOR'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
2026-03-15 00:01:27,043 - ERROR - ensure_candidato: erro ao processar NR_VOTAVEL=70963, NM_VOTAVEL=JEREMIAS SILVA: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-70963' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 161, 'cid': 6, 'pid': 23, 'nr': '70963', 'nm': 'JEREMIAS SILVA'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
mysql.connector.errors.IntegrityError: 1062 (23000): Duplicate entry '5-6-70963' for key 'candidatos.uk_candidato'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/transformers/normalizer.py", line 319, in ensure_candidato
    self.loader.execute_query(
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/loaders/mysql_loader.py", line 19, in execute_query
    result = conn.execute(stmt, params or {})
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1415, in execute
    return meth(
           ^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 523, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1637, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1842, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1982, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2351, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
sqlalchemy.exc.IntegrityError: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-70963' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 161, 'cid': 6, 'pid': 23, 'nr': '70963', 'nm': 'JEREMIAS SILVA'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
2026-03-15 00:01:27,049 - ERROR - ensure_candidato: erro ao processar NR_VOTAVEL=10010, NM_VOTAVEL=ZIZÂNIA DA CRESCER: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-10010' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 227, 'cid': 6, 'pid': 20, 'nr': '10010', 'nm': 'ZIZÂNIA DA CRESCER'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
mysql.connector.errors.IntegrityError: 1062 (23000): Duplicate entry '5-6-10010' for key 'candidatos.uk_candidato'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/transformers/normalizer.py", line 319, in ensure_candidato
    self.loader.execute_query(
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/loaders/mysql_loader.py", line 19, in execute_query
    result = conn.execute(stmt, params or {})
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1415, in execute
    return meth(
           ^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 523, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1637, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1842, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1982, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2351, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
sqlalchemy.exc.IntegrityError: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-10010' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 227, 'cid': 6, 'pid': 20, 'nr': '10010', 'nm': 'ZIZÂNIA DA CRESCER'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
2026-03-15 00:01:27,055 - ERROR - ensure_candidato: erro ao processar NR_VOTAVEL=12333, NM_VOTAVEL=DONIZETH PIQUIZEIRO: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-12333' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 306, 'cid': 6, 'pid': 3, 'nr': '12333', 'nm': 'DONIZETH PIQUIZEIRO'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
mysql.connector.errors.IntegrityError: 1062 (23000): Duplicate entry '5-6-12333' for key 'candidatos.uk_candidato'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/transformers/normalizer.py", line 319, in ensure_candidato
    self.loader.execute_query(
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/loaders/mysql_loader.py", line 19, in execute_query
    result = conn.execute(stmt, params or {})
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1415, in execute
    return meth(
           ^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 523, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1637, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1842, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1982, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2351, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
sqlalchemy.exc.IntegrityError: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-12333' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 306, 'cid': 6, 'pid': 3, 'nr': '12333', 'nm': 'DONIZETH PIQUIZEIRO'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
2026-03-15 00:01:27,059 - ERROR - ensure_candidato: erro ao processar NR_VOTAVEL=55555, NM_VOTAVEL=HENRIQUE SILVA: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-55555' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 282, 'cid': 6, 'pid': 14, 'nr': '55555', 'nm': 'HENRIQUE SILVA'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
mysql.connector.errors.IntegrityError: 1062 (23000): Duplicate entry '5-6-55555' for key 'candidatos.uk_candidato'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/transformers/normalizer.py", line 319, in ensure_candidato
    self.loader.execute_query(
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/loaders/mysql_loader.py", line 19, in execute_query
    result = conn.execute(stmt, params or {})
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1415, in execute
    return meth(
           ^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 523, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1637, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1842, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1982, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2351, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
sqlalchemy.exc.IntegrityError: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-55555' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 282, 'cid': 6, 'pid': 14, 'nr': '55555', 'nm': 'HENRIQUE SILVA'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
2026-03-15 00:01:27,062 - ERROR - ensure_candidato: erro ao processar NR_VOTAVEL=25123, NM_VOTAVEL=THIAGO CARVALHAES: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-25123' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 134, 'cid': 6, 'pid': 33, 'nr': '25123', 'nm': 'THIAGO CARVALHAES'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
mysql.connector.errors.IntegrityError: 1062 (23000): Duplicate entry '5-6-25123' for key 'candidatos.uk_candidato'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/transformers/normalizer.py", line 319, in ensure_candidato
    self.loader.execute_query(
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/loaders/mysql_loader.py", line 19, in execute_query
    result = conn.execute(stmt, params or {})
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1415, in execute
    return meth(
           ^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 523, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1637, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1842, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1982, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2351, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
sqlalchemy.exc.IntegrityError: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-25123' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 134, 'cid': 6, 'pid': 33, 'nr': '25123', 'nm': 'THIAGO CARVALHAES'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
Processing Chunks: 4it [01:50, 27.56s/it]
Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
mysql.connector.errors.IntegrityError: 1062 (23000): Duplicate entry '5-6-22000' for key 'candidatos.uk_candidato'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/loaders/mysql_loader.py", line 19, in execute_query
    result = conn.execute(stmt, params or {})
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1415, in execute
    return meth(
           ^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/sql/elements.py", line 523, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1637, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1842, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1982, in _exec_single_context
    self._handle_dbapi_exception(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2351, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/cursor.py", line 615, in execute
    self._handle_result(self._connection.cmd_query(stmt))
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 824, in _handle_result
    raise get_exception(packet)
sqlalchemy.exc.IntegrityError: (mysql.connector.errors.IntegrityError) 1062 (23000): Duplicate entry '5-6-22000' for key 'candidatos.uk_candidato'
[SQL: INSERT INTO candidatos (eleicao_id, municipio_id, cargo_id, partido_id, nr_votavel, nome) VALUES (%(eid)s, %(mid)s, %(cid)s, %(pid)s, %(nr)s, %(nm)s)]
[parameters: {'eid': 5, 'mid': 159, 'cid': 6, 'pid': 5, 'nr': '22000', 'nm': 'WELLINGTON DA PONTE FUNDA'}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/run_pipeline.py", line 65, in <module>
    main()
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/run_pipeline.py", line 53, in main
    transformer.process_chunk(chunk, metadata)
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/transformers/normalizer.py", line 83, in process_chunk
    self.ensure_candidato(eleicao_id, row)
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/transformers/normalizer.py", line 319, in ensure_candidato
    self.loader.execute_query(
  File "/Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/python/election_data_pipeline/scripts/../src/loaders/mysql_loader.py", line 12, in execute_query
    with self.engine.connect() as conn:
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 234, in __exit__
    self.close()
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1239, in close
    self._transaction.close()
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2585, in close
    self._do_close()
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2723, in _do_close
    self._close_impl()
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2709, in _close_impl
    self._connection_rollback_impl()
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2701, in _connection_rollback_impl
    self.connection._rollback_impl()
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1126, in _rollback_impl
    self._handle_dbapi_exception(e, None, None, None, None)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 2354, in _handle_dbapi_exception
    raise exc_info[1].with_traceback(exc_info[2])
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/base.py", line 1124, in _rollback_impl
    self.engine.dialect.do_rollback(self.connection)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/sqlalchemy/engine/default.py", line 703, in do_rollback
    dbapi_connection.rollback()
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1465, in rollback
    self._execute_query("ROLLBACK")
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1477, in _execute_query
    self.cmd_query(query)
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 1046, in cmd_query
    result = self._handle_result(self._send_cmd(ServerCmd.QUERY, query))
                                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/connection.py", line 667, in _send_cmd
    return self._socket.recv()
           ^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/site-packages/mysql/connector/network.py", line 270, in recv_plain
    chunk = self.sock.recv(4 - packet_len)
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/ssl.py", line 1233, in recv
    return self.read(buflen)
           ^^^^^^^^^^^^^^^^^
  File "/Users/fagnerdossgoncalves/.pyenv/versions/3.12.2/lib/python3.12/ssl.py", line 1106, in read
    return self._sslobj.read(len)
           ^^^^^^^^^^^^^^^^^^^^^^
KeyboardInterrupt

➜  dados-eleicoes-basicos git:(main) ✗ ./run_pipeline.sh