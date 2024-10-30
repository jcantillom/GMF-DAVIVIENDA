""" Módulo que define el modelo SQLAlchemy para la tabla 'CGD_CATALOGO_ERRORES'. """

from sqlalchemy import BOOLEAN, VARCHAR, Column

# pylint: disable=relative-beyond-top-level
from .base import Base


# pylint: disable=too-few-public-methods
class CGDCatalogoErrores(Base):
    """
    Clase que define el modelo SQLAlchemy para la tabla 'CGD_CATALOGO_ERRORES'.

    Esta clase representa la estructura y las propiedades de la tabla 'CGD_CATALOGO_ERRORES'
    en la base de datos. Utiliza SQLAlchemy para el mapeo objeto-relacional.

    Attributes:
        codigo_error (VARCHAR):
            Código único del error.
        descripcion (VARCHAR):
            Descripción del error.
        proceso (VARCHAR):
            Proceso desde el cual se genera el error.
        aplica_reprogramar (BOOLEAN):
            Indica si al presentar error, el sistema debe reprogramar la ejecución del proceso.
    """

    # Nombre de la tabla
    __tablename__: str = "cgd_catalogo_errores"

    # Campos de la tabla
    codigo_error = Column(VARCHAR(30), primary_key=True)
    descripcion = Column(VARCHAR(1000), nullable=False)
    proceso = Column(VARCHAR(1000), nullable=False)
    aplica_reprogramar = Column(BOOLEAN, nullable=False)
