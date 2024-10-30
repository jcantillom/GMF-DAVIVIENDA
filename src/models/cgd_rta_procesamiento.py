""" Módulo que define el modelo SQLAlchemy para la tabla 'CGD_RTA_PROCESAMIENTO'. """

from sqlalchemy import CHAR, NUMERIC, SMALLINT, TIMESTAMP, VARCHAR, Column, ForeignKey

# pylint: disable=relative-beyond-top-level
from .base import Base


# pylint: disable=too-few-public-methods
class CGDRtaProcesamiento(Base):
    """
    Clase que define el modelo SQLAlchemy para la tabla 'CGD_RTA_PROCESAMIENTO'.

    Esta clase representa la estructura y las propiedades de la tabla 'CGD_RTA_PROCESAMIENTO'
    en la base de datos. Utiliza SQLAlchemy para el mapeo objeto-relacional.

    Attributes:
        id_archivo (NUMERIC):
            Identificador único del archivo.
        id_rta_procesamiento (NUMERIC):
            Identificador único de la respuesta de procesamiento.
        nombre_archivo_zip (VARCHAR):
            Nombre del archivo ZIP.
        tipo_respuesta (CHAR):
            Tipo de respuesta.
        fecha_recepcion (TIMESTAMP):
            Fecha de recepción.
        estado (VARCHAR):
            Estado.
        contador_intentos_cargue (SMALLINT):
            Contador de intentos cargue.
        codigo_error (VARCHAR):
            Código de error.
        detalle_error (VARCHAR):
            Detalle del error.
    """

    # Nombre de la tabla
    __tablename__ = "cgd_rta_procesamiento"

    # Campos de la tabla
    id_archivo = Column(NUMERIC(16), primary_key=True)
    id_rta_procesamiento = Column(NUMERIC(2), primary_key=True)
    nombre_archivo_zip = Column(VARCHAR(100), nullable=False)
    tipo_respuesta = Column(CHAR(2), nullable=False)
    fecha_recepcion = Column(TIMESTAMP, nullable=False)
    estado = Column(VARCHAR(50), nullable=False)
    contador_intentos_cargue = Column(SMALLINT, nullable=False)
    codigo_error = Column(
        VARCHAR(30),
        ForeignKey("cgd_catalogo_errores.codigo_error"),
    )
    detalle_error = Column(VARCHAR(2000))
