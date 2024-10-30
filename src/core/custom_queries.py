"""
Módulo para gestionar las consultas relacionadas con cgd_archivos y cgd_rta_procesamiento.
"""
from src.utils.datetime_management import DatetimeManagement 

def update_query_estado_rta_procesamiento_enviado(id_archivo):
    """
    Actualiza el estado de un archivo en cgd_rta_procesamiento a 'ENVIADO'.

    Args:
        id_archivo (str): ID del archivo.

    Returns:
        tuple: Query SQL y parámetros.
    """
    query = """
        UPDATE cgd_rta_procesamiento
        SET estado = 'ENVIADO'
        WHERE id_archivo = :id_archivo 
        AND estado <> 'ENVIADO'
        AND fecha_recepcion = (
            SELECT MAX(fecha_recepcion) 
            FROM cgd_rta_procesamiento 
            WHERE id_archivo = :id_archivo
        )
    """
    params = {"id_archivo": id_archivo}
    return query, params


def query_data_acg_rta_procesamiento_estado_enviado(id_archivo):
    """
    Consulta el estado de un archivo en cgd_rta_procesamiento si está en 'ENVIADO'.

    Args:
        id_archivo (str): ID del archivo.

    Returns:
        tuple: Query SQL y parámetros.
    """
    query = """
        SELECT id_archivo, estado
        FROM CGD_RTA_PROCESAMIENTO
        WHERE id_archivo = :id_archivo
        AND estado = 'ENVIADO'
        AND NOT EXISTS (
            SELECT 1
            FROM 
            WHERE id_archivo = :id_archivo
            AND estado = 'INICIADO'
        )
    """
    params = {"id_archivo": id_archivo}
    return query, params


def query_data_acg_rta_procesamiento(id_archivo):
    """
    Obtiene los detalles de un archivo en cgd_rta_procesamiento.

    Args:
        id_archivo (str): ID del archivo.

    Returns:
        tuple: Query SQL y parámetros.
    """
    query = """
        SELECT id_archivo, estado, tipo_respuesta, nombre_archivo_zip, fecha_recepcion, id_rta_procesamiento
        FROM CGD_RTA_PROCESAMIENTO
        WHERE id_archivo = :id_archivo
        ORDER BY fecha_recepcion DESC
        LIMIT 1
    """
    params = {"id_archivo": id_archivo}
    return query, params


def insert_rta_procesamiento(id_archivo, nombre_archivo_zip, tipo_respuesta):
    """
    Inserta un registro en cgd_rta_procesamiento y coloca el estado en 'INICIADO'.

    Args:
        id_archivo (str): ID del archivo.
        nombre_archivo_zip (str): Nombre del archivo ZIP.
        tipo_respuesta (str): Tipo de respuesta.

    Returns:
        tuple: Query SQL y parámetros.
    """
    query = """
        WITH ultimo_procesamiento AS (
            SELECT COALESCE(MAX(id_rta_procesamiento), 0) AS max_id_rta_procesamiento
            FROM cgd_rta_procesamiento
            WHERE id_archivo = :id_archivo
        )
        INSERT INTO cgd_rta_procesamiento (
            id_rta_procesamiento, id_archivo, nombre_archivo_zip,
            tipo_respuesta, fecha_recepcion, estado, contador_intentos_cargue
        )
        SELECT 
            ultimo_procesamiento.max_id_rta_procesamiento + 1,
            :id_archivo,
            :nombre_archivo_zip,
            :tipo_respuesta,
            :timestamp,
            'INICIADO',
            1
        FROM ultimo_procesamiento
        WHERE ultimo_procesamiento.max_id_rta_procesamiento = 0 
        OR (SELECT estado FROM cgd_rta_procesamiento
            WHERE id_archivo = :id_archivo
            ORDER BY fecha_recepcion DESC
            LIMIT 1) != 'PENDIENTE_REPROCESO';

    """
    params = {
        "id_archivo": id_archivo,
        "nombre_archivo_zip": nombre_archivo_zip,
        "tipo_respuesta": tipo_respuesta,
        "timestamp": DatetimeManagement.convert_string_to_date(
            DatetimeManagement.get_datetime()["timestamp"][:-3]
        ),
    }
    return query, params


def insert_rta_pro_archivos(id_archivo, nombre_archivo, tipo_archivo_rta):
    """
    nEW Insertar registro en CGD_RTA_PRO_ARCHVOS se coloca en estado PENDIENTE_INICIO
    y se coloca CONTADOR_INTENTOS_CARGUE en 0
    el id_rta_procesamiento en manipulado por un trigger llamado trigger_id_rta_procesamiento_pro
    """
    query = """WITH ultimo_procesamiento AS (
                SELECT id_rta_procesamiento
                FROM cgd_rta_procesamiento
                WHERE id_archivo = :id_archivo
                ORDER BY fecha_recepcion DESC
                LIMIT 1
            )
            INSERT INTO cgd_rta_pro_archivos (
                id_rta_procesamiento, id_archivo, nombre_archivo,
                tipo_archivo_rta, estado, contador_intentos_cargue
            ) VALUES (
                (SELECT id_rta_procesamiento FROM ultimo_procesamiento),
                :id_archivo,
                :nombre_archivo,
                :tipo_archivo_rta,
                :estado,
                0
            );"""

    params = {
        "id_archivo": id_archivo,
        "nombre_archivo": nombre_archivo,
        "tipo_archivo_rta": tipo_archivo_rta,
        "estado": "PENDIENTE_INICIO",
    }

    return query, params


def query_archivos_data_count_rta_pro_archivos(id_archivo, id_rta_procesamiento):
    """
    Comparar lo que está en la tabla cgd_rta_pro_archivos con lo que está en la cola.
    """
    query = (
        "SELECT COUNT(1) as cantidad_total_registros "
        "FROM CGD_RTA_PRO_ARCHIVOS "
        "WHERE id_archivo=:id_archivo AND id_rta_procesamiento=:id_rta_procesamiento"
    )
    params = {"id_archivo": id_archivo, "id_rta_procesamiento": id_rta_procesamiento}
    return query, params
