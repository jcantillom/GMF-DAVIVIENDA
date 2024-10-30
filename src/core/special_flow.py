"""
Modulo para realizar las Acciones del flujo Especial
"""

import os
import sys
from datetime import datetime
import importlib
from typing import Dict, Any
from src.services.database_service import DatabaseService
from src.services.logger_service import LoggerService
from src.services.sqs_service import SQSService
from src.utils.datetime_management import DatetimeManagement
from src.utils.environment import Environment
from src.models.cgd_archivos import CGDArchivos
from src.core.format_name_file import (
    extract_name_file,
    format_id_archivo,
    validate_well_formed_esp,
    check_prefix_esp,
)
from src.core.error_handling import ErrorHandling

class Specialflow:
    """
    Clase para realizar las Acciones del flujo Normal.
    """

    def __init__(
        self,
        services: Dict[str, Any],
        error_handling: ErrorHandling,
    ) -> None:
        """
        Inicializa los tributos necesarios para la clase.

        Args:
            services (Dict[str, Any]):
                Diccionario con las instancias de los servicios.
            event_data (Dict[str, Any]):
                Información del evento recibido.
            validator (Validator):
                Clase para realizar las validaciones de los datos de los archivos.
            error_handling (ErrorHandling):
                Clase para el manejo de los errores.
        """
        # Atributo para el manejo de las variables de entorno
        self.env: Environment = services["env"]
        # Atributo para registrar logs
        self.logger_service: LoggerService = services["logger_service"]
        # Atributo para realizar las operaciones con la base de datos
        self.postgres_service: DatabaseService = services["postgres_service"]
        # Atributo para realizar las operaciones con SQS
        self.sqs_service: SQSService = services["sqs_service"]
        # Atributo para el manejo de los errores
        self.error_handling: ErrorHandling = error_handling
        # Services
        self.services: Dict[str, Any] = services

    def special_flow(self, parameters):
        """
        Flujo Especial.
        Args:
            parameters (_type_): _description_
            file_name = parameters.get("file_name")
            parameterstore = parameters.get("parameterstore")
            sqs_message = parameters.get("sqs_message")
            error_handling = parameters.get("error_handling")
        """
        actions_a = importlib.import_module("src.core.actions").Actions
        file_name = parameters.get("file_name")
        parameterstore = parameters.get("parameterstore")
        error_handling = parameters.get("error_handling")

        actions = actions_a(
            services=self.services,
            error_handling=error_handling,
        )

        try:
            if not self.validate_file_in_database(file_name):
                self.logger_service.log_error(
                    f"El archivo {file_name} no se encuentra en la base de datos"
                )
                return

            if check_prefix_esp(file_name):
                self.process_special_file(file_name, parameterstore, error_handling)
            else:
                self.process_standard_file(file_name, actions)
        except (ValueError, KeyError) as e:
            self.logger_service.log_error(f"Error {str(e)}")

    def validate_file_in_database(self, file_name):
        """ "
        Valida File in database
        """
        result = self.postgres_service.get_all(
            model=CGDArchivos,
            columns=[
                CGDArchivos.id_archivo,
                CGDArchivos.estado,
                CGDArchivos.fecha_nombre_archivo,
            ],
            conditions=[CGDArchivos.acg_nombre_archivo == extract_name_file(file_name)],
        )

        if result[1]:
            self.logger_service.log_error(
                f"Error en consulta de base de datos: {result[2]}"
            )
            return False
        return True

    def process_special_file(self, file_name, parameterstore, error_handling):
        """
        Procesa archivos especiales que tienen el prefijo RE_ESP_
        """
        actions_a = importlib.import_module("src.core.actions").Actions
        actions = actions_a(
            services=self.services,
            error_handling=error_handling,
        )
        self.logger_service.log_info("Procesando archivo especial")

        if not validate_well_formed_esp(
            file_name,
            parameterstore["config-retries"]["start-special-files"],
            parameterstore["config-retries"]["end-special-files"],
        ):
            self.logger_service.log_error(
                f"El archivo {file_name} no está bien formado"
            )
            self.error_handling.errors(
                {
                    "file_name": file_name,
                    "estado": "",
                    "valor": False,
                    "codigo_error": "EICP007",
                    "id_archivo": "",
                }
            )
            return

        result = self.postgres_service.get_all(
            model=CGDArchivos,
            columns=[
                CGDArchivos.id_archivo,
                CGDArchivos.estado,
                CGDArchivos.acg_nombre_archivo,
            ],
            conditions=[
                CGDArchivos.acg_nombre_archivo == extract_name_file(file_name),
                CGDArchivos.tipo_archivo == "05"
            ],
        )

        if result[0]:
            id_archivo = result[0][0]["id_archivo"]
            acg_nombre_archivo = result[0][0]["acg_nombre_archivo"]
            estado = result[0][0]["estado"]
            actions.normal_flow(
                {
                    "file_id": id_archivo,
                    "extracted_file_name": acg_nombre_archivo,
                    "state": estado,
                    "file_name": file_name,
                    "zip_folder_name": "",
                    "result_state_validation": False,
                    "result_zip_validation": False,
                    "result_file_validation": False,
                }
            )
        else:
            self.insert_new_file_record(file_name, actions)

    def process_standard_file(self, file_name, actions):
        """
        Procesa archivos estándar sin prefijo RE_ESP_
        """
        self.logger_service.log_info("Procesando archivo estándar")
        result = self.postgres_service.get_all(
            model=CGDArchivos,
            columns=[
                CGDArchivos.id_archivo,
                CGDArchivos.estado,
                CGDArchivos.acg_nombre_archivo,
            ],
            conditions=[CGDArchivos.acg_nombre_archivo == extract_name_file(file_name)],
        )
        if result[0]:
            id_archivo = result[0][0]["id_archivo"]
            acg_nombre_archivo = result[0][0]["acg_nombre_archivo"]
            estado = result[0][0]["estado"]
            actions.normal_flow(
                {
                    "file_id": id_archivo,
                    "extracted_file_name": acg_nombre_archivo,
                    "state": estado,
                    "file_name": file_name,
                    "zip_folder_name": "",
                    "result_state_validation": False,
                    "result_zip_validation": False,
                    "result_file_validation": False,
                }
            )
        else:
            if not result[1]:
                self.error_handling.errors(
                    {
                        "file_name": file_name,
                        "estado": "",
                        "valor": False,
                        "codigo_error": "EICP001",
                        "id_archivo": extract_name_file(file_name)[21:29] + "01" + "02",
                    }
                )
            else:
                self.logger_service.log_error(f"Error en consulta : {result[2]}")
                self.error_handling.process_file_error(
                    updates={
                        "error_code": "EICP006",
                        "error_detail": result[2],
                    },
                    file_id=id_archivo,
                    move_file=False,
                )

    def insert_new_file_record(self, file_name, actions):
        """
        Inserta un nuevo registro en la base de datos para un archivo especial.
        """
        self.logger_service.log_info("Insertando nuevo archivo en la base de datos")
        timestamp: datetime = DatetimeManagement.convert_string_to_date(
            DatetimeManagement.get_datetime()["timestamp"][:-3],
            date_format="%Y%m%d%H%M%S.%f",
        )

        nombre_archivo_recibido_sin_extension = extract_name_file(file_name)
        id_archivo = format_id_archivo(nombre_archivo_recibido_sin_extension)
        # Se registra inicio de proceso en la tabla de CGDArchivos
        _, error, description_error = self.postgres_service.insert(
            model_instance=CGDArchivos(
                id_archivo=int(id_archivo),
                nombre_archivo=nombre_archivo_recibido_sin_extension,
                plataforma_origen="01",
                tipo_archivo="05",
                consecutivo_plataforma_origen=int("1"),
                fecha_nombre_archivo=nombre_archivo_recibido_sin_extension[21:29],
                fecha_registro_resumen=None,
                nro_total_registros=None,
                nro_registros_error=None,
                nro_registros_validos=None,
                estado="ENVIADO",
                fecha_recepcion=timestamp,
                fecha_ciclo=datetime.strptime(
                    nombre_archivo_recibido_sin_extension[21:29], "%Y%m%d"
                ).strftime("%Y%m%d"),
                contador_intentos_cargue=0,
                contador_intentos_generacion=0,
                contador_intentos_empaquetado=0,
                acg_fecha_generacion=None,
                acg_consecutivo=None,
                acg_nombre_archivo=nombre_archivo_recibido_sin_extension,
                acg_registro_encabezado=None,
                acg_registro_resumen=None,
                acg_total_tx=None,
                acg_monto_total_tx=None,
                acg_total_tx_debito=None,
                acg_monto_total_tx_debito=None,
                acg_total_tx_reverso=None,
                acg_monto_total_tx_reverso=None,
                acg_total_tx_reintegro=None,
                acg_monto_total_tx_reintegro=None,
                anulacion_nombre_archivo=None,
                anulacion_justificacion=None,
                anulacion_fecha_anulacion=None,
                gaw_rta_trans_estado=None,
                gaw_rta_trans_codigo=None,
                gaw_rta_trans_detalle=None,
                codigo_error=None,
                detalle_error=None,
            )
        )

        if error:
            self.logger_service.log_error(
                f"Error al guardar en la base de datos: {description_error}"
            )
            self.error_handling.process_file_error(
                updates={
                    "error_code": "EICP006",
                    "error_detail": description_error,
                },
                file_id=id_archivo,
                move_file=False,
            )
            return

        self.logger_service.log_info(
            "Archivo insertado correctamente en la base de datos"
        )
        actions.normal_flow(
            {
                "file_id": id_archivo,
                "extracted_file_name": nombre_archivo_recibido_sin_extension,
                "state": "ENVIADO",
                "file_name": file_name,
                "zip_folder_name": "",
                "result_state_validation": True,
                "result_zip_validation": False,
                "result_file_validation": False,
            }
        )
