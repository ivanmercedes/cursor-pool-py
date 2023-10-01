from logger_base import log
import psycopg2 as db
import sys


class Conexion:
    _DATABASE = "test_db"
    _USERNAME = 'postgres'
    _PASSWORD = 'root'
    _DB_PORT = '5432'
    _HOST = 'localhost'
    _conexion = None
    _cursor = None

    @classmethod
    def obtener_conexion(cls):
        if cls._conexion is None:
            try:
                cls._conexion = db.connect(host=cls._HOST,
                                           user=cls._USERNAME,
                                           password=cls._PASSWORD,
                                           port=cls._DB_PORT,
                                           database=cls._DATABASE)
                log.debug(f'Conexion exitosa: {cls._conexion}')

                return cls._conexion

            except Exception as e:
                log.error(f'Ocurrió una excepcion {e}')
                sys.exit()

        else:
            return cls._conexion

    @classmethod
    def obtener_cursor(cls):
        if cls._cursor is None or cls._cursor.closed:
            try:
                cls._cursor = cls.obtener_conexion().cursor()
                log.debug(f'Se abrio correctamente el cursor: {cls._cursor}')
                return cls._cursor
            except Exception as e:
                log.error(f'Ocurrio una excepcion: {e}')
                sys.exit()
        else:
            return cls._cursor

if __name__ == '__main__':
    Conexion.obtener_conexion()
    Conexion.obtener_cursor()