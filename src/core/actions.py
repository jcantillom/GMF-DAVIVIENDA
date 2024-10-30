"""
Modulo para realizar las Acciones del flujo Normal
"""

import sys
import os
import json
from datetime import datetime
from typing import Dict, Any
from src.services.database_service import DatabaseService
from src.services.logger_service import LoggerService
from src.services.s3_service import S3Service
from src.services.sqs_service import SQSService
from src.services.parameter_store_service import ParameterStoreService
from src.models.cgd_archivos import CGDArchivos
from src.models.cgd_rta_procesamiento import CGDRtaProcesamiento
from src.models.cgd_archivo_estados import CGDArchivoEstados
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos
from src.utils.datetime_management import DatetimeManagement
from src.utils.environment import Environment
from src.core.error_handling import ErrorHandling
from src.core.format_name_file import (
    extract_string_after_slash,
    extract_text_type,
    extract_name_file,
)
from src.core.custom_queries import (
    insert_rta_procesamiento,
    query_data_acg_rta_procesamiento,
    insert_rta_pro_archivos,
    query_data_acg_rta_procesamiento_estado_enviado,
    update_query_estado_rta_procesamiento_enviado,
    query_archivos_data_count_rta_pro_archivos,
)
from src.core.unzip_file import Unzipfile
from src.core.verify_files import Verifyfiles
from src.core.special_flow import Specialflow


class Actions:
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
        # Atributo para realizar las operaciones con S3
        self.s3_service: S3Service = services["s3_service"]
        # Atributo para realizar las operaciones con SQS
        self.sqs_service: SQSService = services["sqs_service"]
        # Atributo para el manejo de los errores
        self.error_handling: ErrorHandling = error_handling
        # Services
        self.services: Dict[str, Any] = services

    def start_process(self, record):
        """
        Proceso el archivo flujo Normal.
        """
        # Convierte el body en un json
        sqs_message: dict = json.loads(record["body"])
        # Obtiene la ruta y el nombre del archivo generado por el evento
        file_name: str = sqs_message["Records"][0]["s3"]["object"]["key"]
        # Obtiene el nombre del bucket
        bucket_name: str = self.env.BUCKET

        id_archivo: int = int(sqs_message.get("file_id", 0))
        is_reprocessing: bool = sqs_message.get("is_reprocessing", False)
        response_processing_id: int = int(sqs_message.get("response_processing_id", 0))
        special_flow: Specialflow = Specialflow(
            services=self.services,
            error_handling=self.error_handling,
        )
        unzip_file: Unzipfile = Unzipfile(
            services=self.services,
            error_handling=self.error_handling,
        )
        if not is_reprocessing:
            self.validate_s3_file_in_queue_message(file_name)
            self.logger_service.log_debug("Camino NO")
            special_flow.special_flow(
                {
                    "file_name": file_name,
                    "parameterstore": self.parameter_store(id_archivo),
                    "sqs_message": sqs_message,
                    "error_handling": self.error_handling,
                }
            )
        else:
            self.logger_service.log_info(
                "Este es el camino que toma si Se valida "
                "que el mensaje de cola el indicador de "
                "proceso este TRUE "
            )
            if (id_archivo != 0) and (response_processing_id != 0):
                _, error, description_error = self.postgres_service.update_by_id(
                    model=CGDRtaProcesamiento,
                    record_id=id_archivo,
                    id_name="id_archivo",
                    updates={
                        "estado": "INICIADO",
                        "contador_intentos_cargue": CGDRtaProcesamiento.contador_intentos_cargue
                                                    + 1,
                    },
                )
                if error:
                    self.logger_service.log_error(description_error)
                    self.error_handling.process_file_error(
                        updates={
                            "error_code": "EPGA001",
                            "error_detail": description_error,
                        },
                        file_id=id_archivo,
                        move_file=False,
                    )

                query, params = query_archivos_data_count_rta_pro_archivos(
                    id_archivo, response_processing_id
                )
                result, error, description_error = self.postgres_service.query(
                    query, params
                )

                if not error:
                    self.logger_service.log_debug(
                        "Total de transacciones obtenida desde BBDD"
                    )
                    query, params = unzip_file.read_s3(
                        bucket_name,
                        self.env.FOLDER_PROCESSING,
                        extract_string_after_slash(file_name)[:-4],
                    )
                    if result[0]["cantidad_total_registros"] > 0:
                        self.process_pending_files_and_send_to_queue(
                            id_archivo, query, params
                        )
                    else:
                        if not query:
                            self.validate_s3_file_in_queue_message(file_name)
                        else:
                            self.process_update_db(id_archivo, file_name)
                        self.normal_flow(
                            {
                                "file_id": id_archivo,
                                "extracted_file_name": extract_name_file(file_name),
                                "state": "",
                                "file_name": file_name,
                                "zip_folder_name": params,
                                "result_state_validation": True,
                                "result_zip_validation": bool(query),
                                "result_file_validation": bool(query),
                            }
                        )
                else:
                    self.logger_service.log_error(description_error)
                    self.error_handling.process_file_error(
                        updates={
                            "error_code": "EPGA001",
                            "error_detail": description_error,
                        },
                        file_id=id_archivo,
                        move_file=False,
                    )
                    return
            else:
                self.logger_service.log_debug("Camino NO")
                special_flow.special_flow(
                    {
                        "file_name": file_name,
                        "parameterstore": self.parameter_store(id_archivo),
                        "sqs_message": sqs_message,
                        "error_handling": self.error_handling,
                    }
                )

    def validate_s3_file_in_queue_message(self, file_name):
        """
        Se valida que en el mensaje que envia la cola
        llegue el nombre del S3, el nombre del archivo y
        que dicho nombre exista en el S3
        """

        is_moved = self.s3_service.read_file(
            self.env.BUCKET,
            file_name,
        )
        if file_name == "" or is_moved[2] is True:
            description_error = (
                f"Archivo {file_name} NO ENCONTRADO en el bucket {self.env.BUCKET}"
            )
            self.logger_service.log_error(description_error)
            messages_queue, error = self.sqs_service.get_messages(
                self.env.SQS_URL_PRO_RESPONSE_TO_PROCESS
            )
            if error:
                self.logger_service.log_error(
                    "Hubo un error al obtener los mensajes de la cola SQS."
                )
                raise ValueError(
                    "Hubo un error al obtener los mensajes de la cola SQS."
                )

            if not messages_queue:
                self.logger_service.log_error(
                    "No se encontraron mensajes en la cola SQS."
                )
                sys.exit()

            for message in messages_queue:
                receipt_handle = message.get("ReceiptHandle")
                if receipt_handle:
                    self.sqs_service.delete_message(
                        self.env.SQS_URL_PRO_RESPONSE_TO_PROCESS, receipt_handle
                    )
                    self.logger_service.log_info(
                        "MENSAJE DE COLA respones-to-process ELIMINADO"
                    )
                    sys.exit()

        self.logger_service.log_info(
            """Se valida que el mensaje que envia la cola
            se encuentre en el indicador de proceso en TRUE """
        )

    def normal_flow(self, file_data):
        """
        Flujo Normal.
        """
        id_archivo = file_data.get("file_id")
        estado = file_data.get("state")
        file_name = file_data.get("file_name")
        result_state_validation = file_data.get("result_state_validation")
        result_zip_validation = file_data.get("result_zip_validation")
        result_file_validation = file_data.get("result_file_validation")
        unzip_file: Unzipfile = Unzipfile(
            services=self.services,
            error_handling=self.error_handling,
        )
        if not result_state_validation:
            result_state_validation = self.validate_states(
                id_archivo, estado, file_name
            )

        if result_state_validation:
            if not result_zip_validation:
                self.process_file_and_update_db(id_archivo, file_name)
            if not result_file_validation:
                self.logger_service.log_info(
                    "Se DESCOMPRIME EL ARCHIVO .ZIP,"
                    "SE CREA UNA CARPETA CON EL NOMBRE "
                )
                status, unzipped_folder_name = unzip_file.unzip_file_data(
                    self.env.BUCKET,
                    self.env.FOLDER_PROCESSING,
                    os.path.dirname(os.path.abspath(__file__)),
                    file_name,
                )
            else:
                status = result_file_validation
                unzipped_folder_name = file_data.get("zip_folder_name")
            if status:
                self.check_unzipped_files(unzipped_folder_name, file_data)
            else:
                self.rejected_state_errors(id_archivo, file_name, estado, "EICP003")

    def check_unzipped_files(self, unzipped_folder_name, file_data):
        """
        Ejecutar funcion para validar los archivos descomprimidos
        """
        result = self.postgres_service.get_all(
            model=CGDArchivos,
            columns=[CGDArchivos.id_archivo, CGDArchivos.estado],
            conditions=[
                CGDArchivos.acg_nombre_archivo == file_data.get("extracted_file_name")
            ],
        )
        verify_files: Verifyfiles = Verifyfiles(
            services=self.services,
            error_handling=self.error_handling,
        )
        valido, coincidencias, todos_comienzan_con_re, archivos, tipo = (
            verify_files.verify_files_data(
                self.env.BUCKET,
                unzipped_folder_name,
                self.env.FOLDER_PROCESSING,
            )
        )

        if result[0]:
            id_archivo = result[0][0]["id_archivo"]
            estado = result[0][0]["estado"]
            query, params = query_data_acg_rta_procesamiento(id_archivo)
            result, error, params = self.postgres_service.query(query, params)
            if result:
                if not result[0]["tipo_respuesta"] == tipo:
                    self.rejected_state_errors(
                        id_archivo, file_data.get("file_name"), estado, "EICP004"
                    )
                    return
            else:
                self.logger_service.log_error(
                    f"Consulta {query}, no devolvió registros" + error
                )
                self.error_handling.process_file_error(
                    updates={
                        "error_code": "EICP006",
                        "error_detail": params,
                    },
                    file_id=id_archivo,
                    move_file=False,
                )
                return
        else:
            self.logger_service.log_error(f"Consulta {query}, no devolvió registros")
            self.error_handling.process_file_error(
                updates={
                    "error_code": "EICP006",
                    "error_detail": result[2],
                },
                file_id=id_archivo,
                move_file=False,
            )
            return
        if self.validate_files_and_register_indb(
                {
                    "id_archivo": id_archivo,
                    "file_name": file_data.get("file_name"),
                    "estado": estado,
                    "valido": valido,
                    "todos_comienzan_con_re": todos_comienzan_con_re,
                    "coincidencias": coincidencias,
                    "archivos": archivos,
                    "result": result,
                }
        ):
            self.validate_and_consolidate_response_process(id_archivo, unzipped_folder_name)

    def validate_states(self, id_archivo, estado, file_name):
        """
        Flujo Normal validar estados.
        """
        estado_enviado = self.env.ESTADO_ENVIADO
        estado_prevalidado = self.env.ESTADO_PREVALIDADO
        estado_procesamiento_fallido = self.env.ESTADO_PROCESAMIENTO_FALLIDO
        estado_procesa_pendiente_reintento = self.env.ESTADO_PROCESA_PENDIENTE_REINTENTO
        estado_procesamiento_rechazado = self.env.ESTADO_PROCESAMIENTO_RECHAZADO
        estados_list = [
            estado_enviado,
            estado_prevalidado,
            estado_procesamiento_fallido,
            estado_procesa_pendiente_reintento,
            estado_procesamiento_rechazado,
        ]
        if estado in estados_list:
            return True
        return self.error_handling.errors(
            {
                "file_name": file_name,
                "estado": estado,
                "valor": True,
                "codigo_error": "EICP002",
                "id_archivo": id_archivo,
            }
        )

    def process_file_and_update_db(self, id_archivo, file_name):
        """
        Se mueve el archivo recibido a ruta en S3
        de archivos procesando (/procesando)
        """
        nombre_archivo_destino = extract_string_after_slash(file_name)
        is_moved = self.s3_service.move_file(
            self.env.BUCKET,
            file_name,
            self.env.FOLDER_PROCESSING,
            nombre_archivo_destino,
        )
        if not is_moved:
            self.logger_service.log_error("No se pudo mover a la carpeta procesando")
            self.error_handling.process_file_error(
                updates={
                    "error_code": "EICP006",
                    "error_detail": "No se pudo mover a la carpeta procesando",
                },
                file_id=id_archivo,
                move_file=False,
            )
            return
        self.process_update_db(id_archivo, file_name)

    def process_update_db(self, id_archivo, file_name):
        """
        Se coloca en estado CARGANDO_RTA_PROCESAMIENTO el archivo
        al que corresponde la respuesta que se va cargare
        """
        timestamp: datetime = DatetimeManagement.convert_string_to_date(
            DatetimeManagement.get_datetime()["timestamp"][:-3],
            date_format="%Y%m%d%H%M%S.%f",
        )
        self.logger_service.log_info(
            "Se coloca en estado CARGANDO_RTA_PROCESAMIENTO el archivo"
            "al que corresponde la respuesta que se va cargar"
        )
        result = self.postgres_service.get_all(
            model=CGDArchivos,
            columns=[
                CGDArchivos.id_archivo,
                CGDArchivos.estado,
                CGDArchivos.fecha_nombre_archivo,
            ],
            conditions=[CGDArchivos.id_archivo == id_archivo],
        )
        if result[0]:
            estado_inicial = result[0][0]["estado"]
        else:
            self.logger_service.log_error("Consulta {query}, no devolvió registros")
            self.error_handling.process_file_error(
                updates={
                    "error_code": "EICP006",
                    "error_detail": result[2],
                },
                file_id=id_archivo,
                move_file=False,
            )
            return

        self.logger_service.log_info("Se Inserta en la BD tabla CGD_ARCHIVOS_ESTADOS")
        _, error, description_error = self.postgres_service.insert(
            model_instance=CGDArchivoEstados(
                id_archivo=id_archivo,
                estado_inicial=estado_inicial,
                estado_final="CARGANDO_RTA_PROCESAMIENTO",
                fecha_cambio_estado=timestamp,
            )
        )
        if not error:
            self.logger_service.log_info(
                "Registros guardados en la tabla cgd_archivo_estados"
            )
        else:
            self.logger_service.log_error(description_error)
            self.error_handling.process_file_error(
                updates={
                    "error_code": "EICP006",
                    "error_detail": description_error,
                },
                file_id=id_archivo,
                move_file=False
            )
            return
        _, error, description_error = self.postgres_service.update_by_id(
            model=CGDArchivos,
            record_id=id_archivo,
            id_name="id_archivo",
            updates={
                "estado": "CARGANDO_RTA_PROCESAMIENTO",
                "fecha_recepcion": timestamp,
                "fecha_ciclo": timestamp.date(),
                "contador_intentos_cargue": CGDArchivos.contador_intentos_cargue + 1,
            },
        )
        if not error:
            self.logger_service.log_info(
                "Tabla CGD_ARCHIVOS, actualizada. Estado = 'CARGANDO_RTA_PROCESAMIENTO'"
                "y contador_intentos_generacion incrementado."
            )
        else:
            self.logger_service.log_error(description_error)
            self.error_handling.process_file_error(
                updates={
                    "error_code": "EICP006",
                    "error_detail": description_error,
                },
                file_id=id_archivo,
                move_file=False
            )
            return

        self.logger_service.log_info(
            "SE INSERTA EN CGD_RTA_PROCESAMIENTO NUEVO PROCESO SE COLOCA"
            "EN ESTADO 'INICIADO' Y CONTADOR_INTENTOS_CARGUE A 1 "
        )
        nombre_archivo_zip = extract_string_after_slash(file_name)
        primeros_7_prefijo = nombre_archivo_zip[:7]
        tipo_respuesta = ""
        if primeros_7_prefijo == "RE_PRO_" and nombre_archivo_zip[-6:-4] == "-R":
            tipo_respuesta = "02"

        if primeros_7_prefijo == "RE_PRO_" and nombre_archivo_zip[-6:-4] != "-R":
            tipo_respuesta = "01"

        if primeros_7_prefijo == "RE_ESP_":
            tipo_respuesta = "03"
        query, params = insert_rta_procesamiento(
            id_archivo, nombre_archivo_zip, tipo_respuesta
        )
        result, error, description_error = self.postgres_service.query(query, params)

        if error:
            self.logger_service.log_error(description_error)
            self.error_handling.process_file_error(
                updates={
                    "error_code": "EICP006",
                    "error_detail": description_error,
                },
                file_id=id_archivo,
                move_file=False,
            )
            return

    def validate_files_and_register_indb(self, file_date):
        """
        Se coloca el estado del proceso de cargue de
        respuesta en RECHAZADO y se registra el código y detalle de error
        """
        id_archivo = file_date.get("id_archivo")
        file_name = file_date.get("file_name")
        estado = file_date.get("estado")
        result = file_date.get("result")
        unzip_file: Unzipfile = Unzipfile(
            services=self.services,
            error_handling=self.error_handling,
        )
        query, path = unzip_file.read_s3(
            self.env.BUCKET,
            self.env.FOLDER_PROCESSING,
            extract_string_after_slash(file_name)[:-4],
        )

        if (
                file_date.get("valido")
                and file_date.get("todos_comienzan_con_re")
                and file_date.get("coincidencias")
                and result
        ):

            for file in file_date.get("archivos"):
                if not (
                        extract_string_after_slash(file_name).split("_")[-1].split("-")[0]
                        == file.split("_")[-1].split("-")[0]
                ):
                    self.rejected_state_errors(id_archivo, file_name, estado, "EICP004")
                    return False

            for archivo in file_date.get("archivos"):

                self.logger_service.log_info(
                    "Total de transacciones obtenida desde BBDD"
                )

                query, params = insert_rta_pro_archivos(
                    id_archivo, archivo, extract_text_type(archivo)
                )
                result, error, description_error = self.postgres_service.query(
                    query, params
                )

                if error:
                    self.logger_service.log_error(description_error)
                    self.error_handling.process_file_error(
                        updates={
                            "error_code": "EICP006",
                            "error_detail": description_error,
                        },
                        file_id=id_archivo,
                        move_file=False,
                    )
                    return id_archivo, error, description_error

                self.logger_service.log_info(
                    "Registros guardados en la tabla cgd_rta_pro_archivos"
                )

                body_validate_str = json.dumps(
                    {
                        "bucket_name": self.env.BUCKET,
                        "folder_name": self.env.FOLDER_PROCESSING + path,
                        "file_name": archivo,
                        "file_id": str(id_archivo),
                        "response_processing_id": self.consult_id_rta_procesamiento(id_archivo),
                    }
                )

                self.sqs_service.send_message(
                    self.env.SQS_URL_PRO_RESPONSE_TO_VALIDATE, body_validate_str
                )
            self.logger_service.log_info(
                "cada vez que se escriba en la cola se actualiza el estado"
                "en la tabla CGD_RTA_PRO_ARCHIVOS"
            )

            _, error, description_error = self.postgres_service.update_all(
                model=CGDRtaProArchivos,
                updates={
                    "estado": "ENVIADO",
                },
                conditions=[
                    CGDRtaProArchivos.id_archivo == id_archivo,
                    CGDRtaProArchivos.estado == "PENDIENTE_INICIO"
                ],
            )

            if error:
                self.logger_service.log_error(description_error)
                self.error_handling.process_file_error(
                    updates={
                        "error_code": "EICP006",
                        "error_detail": description_error,
                    },
                    file_id=id_archivo,
                    move_file=False,
                )
                return id_archivo, error, description_error

            self.logger_service.log_info(
                "Tabla CGD_RTA_PRO_ARCHIVOS actualizada. Estado = 'ENVIADO'"
            )

            return True
        self.rejected_state_errors(id_archivo, file_name, estado, "EICP005")
        return False

    def validate_and_consolidate_response_process(self, id_archivo, path):
        """
        Se Valida si el proceso de cargue de respuesta esta en estado ENVIADO
        """
        self.logger_service.log_info(
            "Se Valida si el proceso de cargue de respuesta esta en estado ENVIADO"
        )
        query, params = query_data_acg_rta_procesamiento_estado_enviado(id_archivo)
        result, error, description_error = self.postgres_service.query(query, params)
        if not result:
            self.logger_service.log_info(
                "Se escribe un mensaje en la cola de -pro-responses-to-consolidate-"
            )
            query, params = query_data_acg_rta_procesamiento(id_archivo)
            result, error, description_error = self.postgres_service.query(
                query, params
            )
            if error:
                self.logger_service.log_error(description_error)
                self.error_handling.process_file_error(
                    updates={
                        "error_code": "EICP006",
                        "error_detail": description_error,
                    },
                    file_id=id_archivo,
                    move_file=False,
                )
                return error
            body_consolidate = {
                "file_id": str(id_archivo),
                "response_processing_id": int(result[0]["id_rta_procesamiento"]),
                "bucket_name": self.env.BUCKET,
                "folder_name": self.env.FOLDER_PROCESSING + path
            }
            body_consolidate_str = json.dumps(body_consolidate)
            self.sqs_service.send_message(
                self.env.SQS_URL_PRO_RESPONSE_TO_CONSOLIDATE, body_consolidate_str
            )
            self.logger_service.log_info("Mensaje en cola validate Registrado")
            self.logger_service.log_info(
                "Se coloca el proceso de cargue de respuesta esta en estado ENVIADO"
            )
            query, params = update_query_estado_rta_procesamiento_enviado(id_archivo)
            result, error, description_error = self.postgres_service.query(
                query, params
            )

            if error:
                self.logger_service.log_error(description_error)
                self.error_handling.process_file_error(
                    updates={
                        "error_code": "EICP006",
                        "error_detail": description_error,
                    },
                    file_id=id_archivo,
                    move_file=False,
                )
                return error
            self.logger_service.log_info(
                "Tabla CGD_ARCHIVOS, actualizada. Estado = 'CARGANDO_RTA_PROCESAMIENTO'"
                "y contador_intentos_generacion incrementado."
            )
            return self.logger_service.log_info(
                "[++++++ FIN DEL PROCESO CON EXITO!! ++++++]"
            )

        return self.logger_service.log_info(
            "[++++++ FIN DEL PROCESO CON EXITO!! ++++++]"
        )

    def parameter_store(self, id_archivo):
        """
        Obtiene los parámetros desde Parameter Store
        """
        try:
            parameter_name = [
                self.env.PARAMETER_NAME_TRANSVERSAL,
                self.env.PARAMETER_NAME_PROCESS_RESPONSE,
            ]
            parameter_store_service = ParameterStoreService(
                env=self.env,
                logger_service=self.logger_service,
                parameter_names=parameter_name,
            )
            parameter_process_response = parameter_store_service.parameters[
                "process-responses"
            ]
            return parameter_process_response
        except (ValueError, KeyError) as e:
            self.logger_service.log_info(
                f"Error al obtener los ParameterStore {str(e)}"
            )
            self.error_handling.process_file_error(
                updates={
                    "error_code": "EICP006",
                    "error_detail": str(e),
                },
                file_id=id_archivo,
                move_file=False,
            )
            return None

    def process_pending_files_and_send_to_queue(self, id_archivo, archivos, path):
        """
        SE recorre cada uno de los archivos para poder
        insertar sus tipos en la tabla
        """
        query, params = query_data_acg_rta_procesamiento(id_archivo)
        result, error, query = self.postgres_service.query(query, params)
        if error:
            self.logger_service.log_error(query)
            self.error_handling.process_file_error(
                updates={
                    "error_code": "EICP006",
                    "error_detail": query,
                },
                file_id=id_archivo,
                move_file=False,
            )
            return
        response_processing_id = int(result[0]["id_rta_procesamiento"])
        result = self.postgres_service.get_all(
            model=CGDRtaProArchivos,
            columns=[
                CGDRtaProArchivos.estado,
                CGDRtaProArchivos.tipo_archivo_rta,
            ],
            conditions=[CGDRtaProArchivos.id_archivo == id_archivo],
        )
        self.logger_service.log_info(
            "Se recorren en la tabla CGD_RTA_PRO_ARCHIVOS todos "
            "los archivos de la respuesta y por cada uno de los "
            "que esten en estado PENDIENTE_INICIO se escribe un "
            "mensaje en la cola pro-response-to-validate"
        )
        for archivo in archivos:
            for pro_archivo in result[0]:
                tipo_archivo = archivo.split("-")[-1].replace(".txt", "")
                if (
                        pro_archivo.get("tipo_archivo_rta") == tipo_archivo
                        and pro_archivo.get("estado") == "PENDIENTE_INICIO"
                ):
                    body_validate = {
                        "bucket_name": self.env.BUCKET,
                        "folder_name": self.env.FOLDER_PROCESSING + path,
                        "file_name": archivo,
                        "file_id": str(id_archivo),
                        "response_processing_id": response_processing_id,
                    }
                    body_validate_str = json.dumps(body_validate)
                    self.sqs_service.send_message(
                        self.env.SQS_URL_PRO_RESPONSE_TO_VALIDATE, body_validate_str
                    )

        self.logger_service.log_info(
            "cada vez que se escriba en la cola"
            "se actualiza el estado en la tabla CGD_RTA_PRO_ARCHIVOS"
        )
        _, error, query = self.postgres_service.update_all(
            model=CGDRtaProArchivos,
            updates={
                "estado": "ENVIADO",
            },
            conditions=[
                CGDRtaProArchivos.id_archivo == id_archivo,
                CGDRtaProArchivos.estado == "PENDIENTE_INICIO"
            ],
        )
        self.validate_and_consolidate_response_process(id_archivo, path)

    def rejected_state_errors(self, id_archivo, file_name, estado, codigo_error):
        """
        Se coloca el estado del proceso de cargue de
        respuesta en RECHAZADO y se registra al codigo y detalle de error
        """
        unzip_file: Unzipfile = Unzipfile(
            services=self.services,
            error_handling=self.error_handling,
        )
        self.logger_service.log_info(
            "Se coloca el estado del proceso de cargue de"
            "respuesta en RECHAZADO y se registra al codigo y detalle de error"
        )

        timestamp: datetime = DatetimeManagement.convert_string_to_date(
            DatetimeManagement.get_datetime()["timestamp"][:-3],
            date_format="%Y%m%d%H%M%S.%f",
        )
        _, error, description_error = self.postgres_service.update_by_id(
            model=CGDRtaProcesamiento,
            record_id=id_archivo,
            id_name="id_archivo",
            updates={
                "estado": "RECHAZADO",
            },
        )
        if error:
            self.logger_service.log_error(description_error)
            self.error_handling.process_file_error(
                updates={
                    "error_code": "EICP006",
                    "error_detail": description_error,
                },
                file_id=id_archivo,
                move_file=False,
            )
            return error

        self.logger_service.log_info(
            "Se coloca en estado PROCESAMIENTO_RECHAZADO el"
            "proceso de cargue del archivo original de movimientos al que corresponde la respuesta"
        )

        result = self.postgres_service.get_all(
            model=CGDArchivos,
            columns=[
                CGDArchivos.id_archivo,
                CGDArchivos.estado,
                CGDArchivos.fecha_nombre_archivo,
            ],
            conditions=[CGDArchivos.id_archivo == id_archivo],
        )
        _, error, description_error = self.postgres_service.insert(
            model_instance=CGDArchivoEstados(
                id_archivo=int(id_archivo),
                estado_inicial=result[0][0]["estado"],
                estado_final="PROCESAMIENTO_RECHAZADO",
                fecha_cambio_estado=timestamp,
            )
        )

        if error:
            self.logger_service.log_error(description_error)
            self.error_handling.process_file_error(
                updates={
                    "error_code": "EICP006",
                    "error_detail": description_error,
                },
                file_id=id_archivo,
                move_file=False,
            )
            return error

        self.logger_service.log_info(
            "Tabla CGD_ARCHIVO_ESTADOS, INSERT. Estado = 'PROCESAMIENTO_RECHAZADO' "
        )

        _, error, description_error = self.postgres_service.update_by_id(
            model=CGDArchivos,
            record_id=id_archivo,
            id_name="id_archivo",
            updates={
                "estado": "PROCESAMIENTO_RECHAZADO",
                "fecha_recepcion": timestamp,
                "fecha_ciclo": timestamp.date(),
                "contador_intentos_cargue": CGDArchivos.contador_intentos_cargue + 1,
            },
        )
        if error:
            self.logger_service.log_error(description_error)
            self.error_handling.process_file_error(
                updates={
                    "error_code": "EICP006",
                    "error_detail": description_error,
                },
                file_id=id_archivo,
                move_file=False,
            )
            return error
        self.logger_service.log_info(
            "Tabla CGD_ARCHIVOS, actualizada. Estado = 'PROCESAMIENTO_RECHAZADO' "
        )
        fecha_datetime = datetime.strptime(str(datetime.now()), "%Y-%m-%d %H:%M:%S.%f")
        _, params = unzip_file.read_s3(
            self.env.BUCKET,
            self.env.FOLDER_PROCESSING,
            extract_string_after_slash(file_name)[:-4],
        )
        unzip_file.move_folder(
            self.env.BUCKET,
            self.env.FOLDER_PROCESSING + params,
            self.env.FOLDER_REJECTED
            + fecha_datetime.strftime("%Y%m")
            + "/",
        )
        return self.error_handling.errors(
            {
                "file_name": file_name,
                "estado": estado,
                "valor": False,
                "codigo_error": codigo_error,
                "id_archivo": id_archivo,
            }
        )

    def consult_id_rta_procesamiento(self, id_archivo):
        """
        Trae el id_rta_procesamiento
        """
        id_rta_procesamiento = 1
        query, params = query_data_acg_rta_procesamiento(id_archivo)
        result, params, query = self.postgres_service.query(query, params)
        if result:
            id_rta_procesamiento = int(result[0]["id_rta_procesamiento"])
            return id_rta_procesamiento
        else:
            self.logger_service.log_error(query)
            self.error_handling.process_file_error(
                updates={
                    "error_code": "EICP006",
                    "error_detail": query,
                },
                file_id=id_archivo,
                move_file=False,
            )
            return id_rta_procesamiento
