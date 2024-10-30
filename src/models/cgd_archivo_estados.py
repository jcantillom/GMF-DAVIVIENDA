""" Módulo que define el modelo SQLAlchemy para la tabla 'CGD_ARCHIVO_ESTADOS'. """

from sqlalchemy import NUMERIC, TIMESTAMP, VARCHAR, Column, ForeignKey

from .base import Base


class CGDArchivoEstados(Base):
    """
    Clase que define el modelo SQLAlchemy para la tabla 'CGD_ARCHIVO_ESTADOS'.

    Esta clase representa la estructura y las propiedades de la tabla 'CGD_ARCHIVO_ESTADOS'
    en la base de datos. Utiliza SQLAlchemy para el mapeo objeto-relacional.

    Attributes:
        id_archivo (NUMERIC):
            Identificador único del archivo.
        estado_inicial (VARCHAR):
            Estado inicial del archivo.
        estado_final (VARCHAR):
            Estado final del archivo.
        fecha_cambio_estado (TIMESTAMP):
            Fecha de cambio de estado del archivo.
    """

    # Nombre de la tabla
    __tablename__ = "cgd_archivo_estados"

    # Campos de la tabla
    id_archivo = Column(
        NUMERIC(16),
        ForeignKey("cgd_archivos.id_archivo"),
        primary_key=True,
    )
    estado_inicial = Column(VARCHAR(50))
    estado_final = Column(VARCHAR(50), primary_key=True)
    fecha_cambio_estado = Column(TIMESTAMP, primary_key=True)
