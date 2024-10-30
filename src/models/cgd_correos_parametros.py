""" Módulo que define el modelo SQLAlchemy para la tabla 'CGD_CORREOS_PARAMETROS'. """

from sqlalchemy import CHAR, VARCHAR, Column, ForeignKey

# pylint: disable=relative-beyond-top-level
from .base import Base


# pylint: disable=too-few-public-methods
class CGDCorreosParametros(Base):
    """
    Clase que define el modelo SQLAlchemy para la tabla 'CGD_CORREOS_PARAMETROS'.

    Esta clase representa la estructura y las propiedades de la tabla 'CGD_CORREOS_PARAMETROS'
    en la base de datos. Utiliza SQLAlchemy para el mapeo objeto-relacional.

    Attributes:
        id_plantilla (CHAR):
            Identificador único de la plantilla a la que pertenece el párametro.
        id_parametro (VARCHAR):
            Identificador único del parámetro de una plantilla.
        descripcion (VARCHAR):
            Descripción del parámetro.
    """

    # Nombre de la tabla
    __tablename__: str = "cgd_correos_parametros"

    # Campos de la tabla
    id_plantilla = Column(
        CHAR(5),
        ForeignKey("cgd_correos_plantillas.id_plantilla"),
        primary_key=True,
    )
    id_parametro = Column(VARCHAR(30), primary_key=True)
    descripcion = Column(VARCHAR(2000), nullable=False)
