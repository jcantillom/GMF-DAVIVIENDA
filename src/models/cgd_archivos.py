""" Módulo que define el modelo SQLAlchemy para la tabla 'CGD_ARCHIVOS'. """

from sqlalchemy import (
    CHAR,
    DATE,
    DECIMAL,
    NUMERIC,
    SMALLINT,
    TIMESTAMP,
    VARCHAR,
    Column,
    ForeignKey,
)

# pylint: disable=relative-beyond-top-level
from .base import Base


# pylint: disable=too-few-public-methods
class CGDArchivos(Base):
    """
    Clase que define el modelo SQLAlchemy para la tabla 'CGD_ARCHIVOS'.

    Esta clase representa la estructura y las propiedades de la tabla 'CGD_ARCHIVOS'
    en la base de datos. Utiliza SQLAlchemy para el mapeo objeto-relacional.

    Attributes:
        id_archivo (NUMERIC):
            Identificador único del archivo.
        nombre_archivo (VARCHAR):
            Nombre del archivo.
        plataforma_origen (CHAR):
            Plataforma de origen del archivo.
        tipo_archivo (CHAR):
            Tipo de archivo.
        consecutivo_plataforma_origen (SMALLINT):
            Consecutivo de la plataforma de origen.
        fecha_nombre_archivo (CHAR):
            Fecha del nombre del archivo.
        fecha_registro_resumen (CHAR):
            Fecha de registro del resumen.
        nro_total_registros (NUMERIC):
            Número total de registros.
        nro_registros_error (NUMERIC):
            Número de registros con error.
        nro_registros_validos (NUMERIC):
            Número de registros válidos.
        estado (VARCHAR):
            Estado del archivo.
        fecha_recepcion (TIMESTAMP):
            Fecha de recepción del archivo.
        fecha_ciclo (DATE):
            Fecha del ciclo del archivo.
        contador_intentos_cargue (SMALLINT):
            Contador de intentos de cargue.
        contador_intentos_generacion (SMALLINT):
            Contador de intentos de generación.
        contador_intentos_empaquetado (SMALLINT):
            Contador de intentos de empaquetado.
        acg_fecha_generacion (TIMESTAMP):
            Fecha de generación del archivo ACG.
        acg_consecutivo (NUMERIC):
            Consecutivo del archivo ACG.
        acg_nombre_archivo (VARCHAR):
            Nombre del archivo ACG.
        acg_registro_encabezado (VARCHAR):
            Registro de encabezado del archivo ACG.
        acg_registro_resumen (VARCHAR):
            Registro de resumen del archivo ACG.
        acg_total_tx (NUMERIC):
            Total de transacciones del archivo ACG.
        acg_monto_total_tx (Decimal):
            Monto total de transacciones del archivo ACG.
        acg_total_tx_debito (NUMERIC):
            Total de transacciones de débito del archivo ACG.
        acg_monto_total_tx_debito (Decimal):
            Monto total de transacciones de débito del archivo ACG.
        acg_total_tx_reverso (NUMERIC):
            Total de transacciones de reverso del archivo ACG.
        acg_monto_total_tx_reverso (Decimal):
            Monto total de transacciones de reverso del archivo ACG.
        acg_total_tx_reintegro (NUMERIC):
            Total de transacciones de reintegro del archivo ACG.
        acg_monto_total_tx_reintegro (Decimal):
            Monto total de transacciones de reintegro del archivo ACG.
        anulacion_nombre_archivo (VARCHAR):
            Nombre del archivo de anulación.
        anulacion_justificacion (VARCHAR):
            Justificación de la anulación.
        anulacion_fecha_anulacion (TIMESTAMP):
            Fecha de anulación del archivo.
        gaw_rta_trans_estado (VARCHAR):
            Estado de la respuesta de la transacción GAW.
        gaw_rta_trans_codigo (VARCHAR):
            Código de la respuesta de la transacción GAW.
        gaw_rta_trans_detalle (VARCHAR):
            Detalle de la respuesta de la transacción GAW.
        id_prc_genera_consol (NUMERIC):
            Identificador del consolidado.
        codigo_error (VARCHAR):
            Código de error.
        detalle_error (VARCHAR):
            Detalle del error.
    """

    # Nombre de la tabla
    __tablename__ = "cgd_archivos"

    # Campos de la tabla
    id_archivo = Column(NUMERIC(16), primary_key=True)
    nombre_archivo = Column(VARCHAR(100), nullable=False)
    plataforma_origen = Column(CHAR(2), nullable=False)
    tipo_archivo = Column(CHAR(2), nullable=False)
    consecutivo_plataforma_origen = Column(SMALLINT, nullable=False)
    fecha_nombre_archivo = Column(CHAR(8), nullable=False)
    fecha_registro_resumen = Column(CHAR(14))
    nro_total_registros = Column(NUMERIC(9))
    nro_registros_error = Column(NUMERIC(9))
    nro_registros_validos = Column(NUMERIC(9))
    estado = Column(VARCHAR(50), nullable=False)
    fecha_recepcion = Column(TIMESTAMP, nullable=False)
    fecha_ciclo = Column(DATE, nullable=False)
    contador_intentos_cargue = Column(SMALLINT, nullable=False)
    contador_intentos_generacion = Column(SMALLINT, nullable=False)
    contador_intentos_empaquetado = Column(SMALLINT, nullable=False)
    acg_fecha_generacion = Column(TIMESTAMP)
    acg_consecutivo = Column(NUMERIC(4))
    acg_nombre_archivo = Column(VARCHAR(100))
    acg_registro_encabezado = Column(VARCHAR(200))
    acg_registro_resumen = Column(VARCHAR(200))
    acg_total_tx = Column(NUMERIC(9))
    acg_monto_total_tx = Column(DECIMAL(19, 2))
    acg_total_tx_debito = Column(NUMERIC(9))
    acg_monto_total_tx_debito = Column(DECIMAL(19, 2))
    acg_total_tx_reverso = Column(NUMERIC(9))
    acg_monto_total_tx_reverso = Column(DECIMAL(19, 2))
    acg_total_tx_reintegro = Column(NUMERIC(9))
    acg_monto_total_tx_reintegro = Column(DECIMAL(19, 2))
    anulacion_nombre_archivo = Column(VARCHAR(100))
    anulacion_justificacion = Column(VARCHAR(4000))
    anulacion_fecha_anulacion = Column(TIMESTAMP)
    gaw_rta_trans_estado = Column(VARCHAR(50))
    gaw_rta_trans_codigo = Column(VARCHAR(4))
    gaw_rta_trans_detalle = Column(VARCHAR(1000))
    codigo_error = Column(
        VARCHAR(30),
        ForeignKey("cgd_catalogo_errores.codigo_error"),
    )
    detalle_error = Column(VARCHAR(2000))
