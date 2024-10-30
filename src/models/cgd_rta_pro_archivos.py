""" Módulo que define el modelo SQLAlchemy para la tabla 'CGD_RTA_PRO_ARCHIVOS'. """

from sqlalchemy import NUMERIC, SMALLINT, VARCHAR, Column, ForeignKey

# pylint: disable=relative-beyond-top-level
from .base import Base


# pylint: disable=too-few-public-methods
class CGDRtaProArchivos(Base):
    """
    Clase que define el modelo SQLAlchemy para la tabla 'CGD_RTA_PRO_ARCHIVOS'.

    Esta clase representa la estructura y las propiedades de la tabla 'CGD_RTA_PRO_ARCHIVOS'
    en la base de datos. Utiliza SQLAlchemy para el mapeo objeto-relacional.

    Attributes:
        id_archivo (NUMERIC):
            Identificador del archivo.
        id_rta_procesamiento (NUMERIC):
            Identificador de la respuesta del procesamiento.
        nombre_archivo (VARCHAR):
            Nombre del archivo.
        tipo_archivo_rta (VARCHAR):
            Tipo de archivo de respuesta.
        estado (VARCHAR):
            Estado actual del archivo.
        contador_intentos_cargue (SMALLINT):
            Número de intentos de carga.
        nro_total_registros (NUMERIC):
            Número total de registros en el archivo.
        nro_registros_error (NUMERIC):
            Número de registros con errores.
        nro_registros_validos (NUMERIC):
            Número de registros válidos.
        codigo_error (VARCHAR):
            Código de error relacionado.
        detalle_error (VARCHAR):
            Descripción detallada del error.
    """

    # Nombre de la tabla
    __tablename__ = "cgd_rta_pro_archivos"

    # Campos de la tabla
    id_archivo = Column(
        NUMERIC(16),
        ForeignKey("cgd_rta_procesamiento.id_archivo"),
        primary_key=True,
    )
    id_rta_procesamiento = Column(
        NUMERIC(2),
        ForeignKey("cgd_rta_procesamiento.id_rta_procesamiento"),
        primary_key=True,
    )
    nombre_archivo = Column(VARCHAR(100), primary_key=True)
    tipo_archivo_rta = Column(VARCHAR(30), nullable=False)
    estado = Column(VARCHAR(30), nullable=False)
    contador_intentos_cargue = Column(SMALLINT, nullable=False)
    nro_total_registros = Column(NUMERIC(9))
    nro_registros_error = Column(NUMERIC(9))
    nro_registros_validos = Column(NUMERIC(9))
    codigo_error = Column(
        VARCHAR(30),
        ForeignKey("cgd_catalogo_errores.codigo_error"),
    )
    detalle_error = Column(VARCHAR(2000))
