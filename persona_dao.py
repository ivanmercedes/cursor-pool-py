import logging

from conexion import Conexion
from persona import Persona
from logger_base import log


class PersonaDAO:
    """
    DAO (Data Access Object)
    CRUD (Create-Read-Update-Delete)
    """

    _SELECCIONAR = 'SELECT * FROM persona ORDER BY id_persona'
    _INSERTAR = 'INSERT INTO persona (nombre, apellido, email) VALUES (%s, %s, %s)'
    _ACTUALIZAR = 'UPDATE persona SET nombre=%s, apellido=%s, email=%s WHERE id_persona=%s'

    _ELIMINAR = 'DELETE FROM persona WHERE id_persona=%s'

    @classmethod
    def seleccionar(cls):
        with Conexion.obtener_conexion() as conexion:
            with conexion.cursor() as cursor:
                cursor.execute(cls._SELECCIONAR)
                registros = cursor.fetchall()
                _personas = []
                for registro in registros:
                    _persona = Persona(registro[0], registro[1], registro[2], registro[3])
                    _personas.append(_persona)

                return _personas

    @classmethod
    def insertar(cls, persona_obj):
        with Conexion.obtener_conexion():
            with Conexion.obtener_cursor() as cursor:
                valores = (persona_obj.nombre, persona_obj.apellido, persona_obj.email)
                cursor.execute(cls._INSERTAR, valores)
                log.debug(f'Persona insertada: {persona_obj}')
                return cursor.rowcount

    @classmethod
    def actulizar(cls, persona_obj):
        with Conexion.obtener_conexion():
            with Conexion.obtener_cursor() as cursor:
                valores = (persona_obj.nombre, persona_obj.apellido, persona_obj.email, persona_obj.id_persona)
                cursor.execute(cls._ACTUALIZAR, valores)
                log.debug(f'Persona actualizada: {persona_obj}')
                return cursor.rowcount

    @classmethod
    def eliminar(cls, persona):
        with Conexion.obtener_conexion():
            with Conexion.obtener_cursor() as cursor:
                valores = (persona.id_persona,)
                cursor.execute(cls._ELIMINAR, valores)
                log.debug(f'Persona eliminada: {persona}')
                return cursor.rowcount


if __name__ == '__main__':
    # Actualizar un registro
    # persona1 = Persona(2, "Pepe rico", "Cuca", "cuca@cuca.com")
    # personas_actualizadas = PersonaDAO.actulizar(persona1)
    # log.debug(f'Personas actualizadas: {personas_actualizadas}')
    # insertar persona
    # persona1 = Persona(nombre="Pedro", apellido="Caca", email="pepe@pepe.com")
    # personas_insertada = PersonaDAO.insertar(persona1)
    # log.debug(f'Personas insertadas: {personas_insertada}')

    # Eliminar registro
    persona1 = Persona(id_persona=20)
    personas_eliminadas = PersonaDAO.eliminar(persona1)
    log.debug(f'Personas eliminadas: {personas_eliminadas}')
    # selecionar objectos
    personas = PersonaDAO.seleccionar()

    for persona in personas:
        logging.debug(persona)
