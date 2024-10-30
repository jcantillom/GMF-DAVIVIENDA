""" Modulo para el manejo de errores. """

import sys
import json
from datetime import datetime
from typing import Any, Dict, Optional
from src.services.database_service import DatabaseService
from src.services.logger_service import LoggerService
from src.services.s3_service import S3Service
from src.services.sqs_service import SQSService
from src.models.cgd_catalogo_errores import CGDCatalogoErrores
from src.models.cgd_correos_parametros import CGDCorreosParametros
from src.models.cgd_archivo_estados import CGDArchivoEstados
from src.models.cgd_archivos import CGDArchivos
from src.models.cgd_rta_procesamiento import CGDRtaProcesamiento
from src.utils.environment import Environment
from src.utils.datetime_management import DatetimeManagement
from src.core.format_name_file import extract_string_after_slash

from src.core.custom_queries import (
    query_data_acg_rta_procesamiento,
)

class ErrorHandling:
    """
    Clase para el manejo de los errores.
    """

    def __init__(
        self,
        services: Dict[str, Any],
        event_data: Dict[str, Any],
    ) -> None:
        """
        Inicializa los tributos necesarios para la clase.

        Args:
            services (Dict[str, Any]):
                Diccionario con las instancias de los servicios.
            event_data (Dict[str, Any]):
                Información del evento recibido.
        """
        # Atributo para el manejo de las variables de entorno
        self.env: Environment = services["env"]
        # Atributo para registrar logs
        self.logger_service: LoggerService = services["logger_service"]
        # Atributo con la información de los parámetros
        self.parameters: Dict[str, Any] = services[
            "parameter_store_service"
        ].parameters["transversal"]
        # Atributo para realizar las operaciones con la base de datos
        self.postgres_service: DatabaseService = services["postgres_service"]
        # Atributo para realizar las operaciones con SQS
        self.sqs_service: SQSService = services["sqs_service"]
        # Atributo para realizar las operaciones con S3
        self.s3_service: S3Service = services["s3_service"]
        # Atributo con la información del evento
        self.event_data: Dict[str, Any] = event_data


    def _message_structure(self, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Genera la estructura del mensaje que se va enviar a la cola.

        Args:
            values (Dict[str, Any]):
                Valores para el mensaje que se enviará a la cola para el envío del correo.

        Returns:
            Dict[str, Any]:
                Estructura del mensaje que se va enviar a la cola.
        """
        # Se define la variable para crear la estructura del mensaje con sus respectivos valores
        message_structure: dict = {
            "id_plantilla": values["id_plantilla"],
            "parametros": [],
        }

        # Obtiene los parámetros del correo
        mail_parameters = self.postgres_service.get_all(
            columns=[CGDCorreosParametros.id_parametro],
            conditions=[CGDCorreosParametros.id_plantilla == values["id_plantilla"]],
        )[0]

        # Crea la estructura con el mensaje que se va enviar a la cola
        for parameter in mail_parameters:
            message_structure["parametros"].append(
                {
                    "nombre": parameter["id_parametro"],
                    "valor": values[parameter["id_parametro"]],
                }
            )

        return message_structure

    def process_file_error(
        self,
        updates: Dict[str, Any],
        file_id: int,
        move_file: Optional[bool] = False,
    ) -> None:
        """
        Procesa el error del archivo.

        Args:
            updates (Dict[str, Any]):
                Diccionario con los campos y valores a actualizar.
            file_id (int):
                Identificar del archivo.
            id_template (str):
                Id de la plantilla.
            move_file (Optional[bool]):
                Indicador si se debe mover el archivo de carpeta. Por defecto False.
        """
        self.logger_service.log_info("Comienza el flujo de error")

        # Obtiene el nombre del archivo a partir del evento
        data = json.loads(self.event_data["Records"][0]["body"])
        file_name = data["Records"][0]["s3"]["object"]["key"]
        # Obtiene y convierte el timestamp de string a un objeto datetime
        timestamp: datetime = DatetimeManagement.convert_string_to_date(
            DatetimeManagement.get_datetime()["timestamp"][:-3],
            date_format="%Y%m%d%H%M%S.%f",
        )

        # Obtiene la información de la tabla de CGDArchivos
        archivos: Dict[str, Any] = self.postgres_service.get_by_id(
            model=CGDArchivos,
            columns=[
                CGDArchivos.estado,
                CGDArchivos.contador_intentos_cargue,
                CGDArchivos.fecha_recepcion
            ],
            record_id=file_id,
            id_name="id_archivo",
        )[0]

        estado = archivos.get("estado")
        contador_intentos_cargue = archivos.get("contador_intentos_cargue")
        fecha_recepcion_archivo = archivos.get("fecha_recepcion")

        nombre_archivo_zip = extract_string_after_slash(file_name)

        # Obtiene la información de la tabla de CGDRtaProcesamiento id_rta_procesamiento
        id_rta_procesamiento = 0
        query, params = query_data_acg_rta_procesamiento(file_id)
        result, params, query = self.postgres_service.query(query, params)
        if result:
            id_rta_procesamiento = int(result[0]["id_rta_procesamiento"])

        # Obtiene la información de la tabla de CGDCatalogoErrores
        catalogo_errores: Dict[str, Any] = self.postgres_service.get_by_id(
            model=CGDCatalogoErrores,
            record_id=updates["error_code"],
            id_name="codigo_error",
        )[0]

        aplica_reprogramar = catalogo_errores.get("aplica_reprogramar", True)
        fecha_datetime = datetime.strptime(str(fecha_recepcion_archivo), "%Y-%m-%d %H:%M:%S.%f")
        fecha_formateada = fecha_datetime.strftime("%d/%m/%Y")
        hora_formateada = fecha_datetime.strftime("%I:%M %p")

        # Valores para el mensaje que se enviará a la cola para el envío del correo
        values: dict = {
            "id_plantilla": "",
            "estado": "",
            "nombre_archivo_rta": nombre_archivo_zip,
            "plataforma_origen": "STRATUS",
            "fecha_recepcion": fecha_formateada,
            "hora_recepcion": hora_formateada,
            "codigo_falla": updates["error_code"],
            "codigo_rechazo": updates["error_code"],
            "descripcion_falla": catalogo_errores.get("descripcion", ""),
            "descripcion_rechazo": catalogo_errores.get("descripcion", ""),
            "detalle_falla": catalogo_errores.get("descripcion", ""),
            "nombre_proceso": catalogo_errores.get("proceso", "CARGUE"),
        }

        # Si se cuenta con id_archivo y id_rta_procesamiento
        # se actualiza el estado en la tabla CGDRtaProcesamiento
        if id_rta_procesamiento and id_rta_procesamiento != 0:
            # Actualiza los valores en la tabla CGD_RTA_PROCESAMIENTO
            self.postgres_service.update_by_id(
                model=CGDRtaProcesamiento,
                record_id=file_id,
                id_name="id_archivo",
                updates={
                    "codigo_error": updates["error_code"],
                    "detalle_error": catalogo_errores.get("descripcion", ""),
                },
            )

        # Si no se cuenta con id_archivo y id_rta_procesamiento inserta en cgd_archivos
        else:
            # Se inserta en la tabla cgd_archivos
            self.postgres_service.insert(
                model_instance=CGDArchivos(
                    model=CGDArchivos,
                    record_id=file_id,
                    id_name="id_archivo",
                    updates={
                        "codigo_error": updates["error_code"],
                        "detalle_error": catalogo_errores.get("descripcion", ""),
                    },
                )
            )
        aplica_reprogramar = True
        # Valida si el error aplica reprogramar y si no ha excedido la cantidad de intentos máximos
        if (
            aplica_reprogramar
            and contador_intentos_cargue
            < int(self.parameters["config-retries"]["number-retries"])
        ):
            # se actualiza la tabla CGDArchivos estado PROCESA_PENDIENTE_REINTENTO
            # se incrementa el contador de intentos
            self.postgres_service.update_by_id(
                model=CGDArchivos,
                record_id=file_id,
                id_name="id_archivo",
                updates={
                    "estado": "PROCESA_PENDIENTE_REINTENTO",
                },
            )
            # Se inserta el nuevo estado en la tabla CGDArchivoEstados
            self.postgres_service.insert(
                model_instance=CGDArchivoEstados(
                    id_archivo=file_id,
                    estado_inicial=archivos.get("estado"),
                    estado_final="PROCESA_PENDIENTE_REINTENTO",
                    fecha_cambio_estado=timestamp,
                )
            )

            # Crear nuevo mensaje para encolar con indicador de reproceso
            new_message = {
                "bucket_name": self.env.BUCKET,
                "file_name": file_name,
                "is_reprocessing": True
            }
            # Solo si se cuenta con id_rta_procesamiento se actualiza
            # estado en la tabla CGDRtaProcesamiento
            if id_rta_procesamiento and id_rta_procesamiento != 0:
                # Se actualiza el estado en la tabla CGD_RTA_PROCESAMIENTO
                self.postgres_service.update_by_id(
                    model=CGDRtaProcesamiento,
                    record_id=file_id,
                    id_name="id_archivo",
                    updates={"estado": "PENDIENTE_REINTENTO"},
                )
                # Agrega el atributo de file_id
                new_message["file_id"] = str(file_id)
                # Agrega el atributo de response_processing_id
                new_message["response_processing_id"] = str(id_rta_procesamiento)

            # Se elimina el mensaje de la cola "pro_response_to_process"
            self.sqs_service.delete_message(
                queue_url=self.env.SQS_URL_PRO_RESPONSE_TO_PROCESS,
                receipt_handle=self.event_data["Records"][0]["receiptHandle"],
            )

            # Envía mensaje a la cola de SQS origen con indicador de reproceso y con delay_seconds
            self.sqs_service.send_message(
                queue_url=self.env.SQS_URL_PRO_RESPONSE_TO_PROCESS,
                message_body=new_message,
                delay_seconds=(
                    self.parameters["config-retries"][
                        "time-between-retry"
                    ]
                ),
            )

            # Se genera el mensaje para la cola de envío de correos
            values["id_plantilla"] = "PC012"
            message_structured = self._message_structure(values=values)
            # Envía mensaje a la cola de SQS para envío de correos
            self.sqs_service.send_message(
                queue_url=self.env.SQS_URL_EMAILS,
                message_body=json.dumps(message_structured),
            )
            # Finaliza el proceso
            self.logger_service.log_info("Finaliza proceso de manejo de errores")
            sys.exit(0)
        else:
            # se actualiza la tabla CGDArchivos estado PROCESAMIENTO_FALLIDO
            self.postgres_service.update_by_id(
                model=CGDArchivos,
                record_id=file_id,
                id_name="id_archivo",
                updates={
                    "estado": "PROCESAMIENTO_FALLIDO",
                },
            )
            # Se inserta el nuevo estado en la tabla CGDArchivoEstados
            self.postgres_service.insert(
                model_instance=CGDArchivoEstados(
                    id_archivo=file_id,
                    estado_inicial=archivos.get("estado"),
                    estado_final="PROCESAMIENTO_FALLIDO",
                    fecha_cambio_estado=timestamp,
                )
            )

            if id_rta_procesamiento and id_rta_procesamiento != 0:
                # Se actualiza el estado en la tabla CGD_RTA_PROCESAMIENTO
                self.postgres_service.update_by_id(
                    model=CGDRtaProcesamiento,
                    record_id=file_id,
                    id_name="id_archivo",
                    updates={"estado": "FALLIDO"},
                )

            # Enviar mensaje a la cola de SQS para envío de correos con la plantilla PC013
            values["id_plantilla"] = "PC013"
            message_structured = self._message_structure(values=values)
            self.sqs_service.send_message(
                self.env.SQS_URL_EMAILS,
                json.dumps(message_structured),
                0,
            )
            # mover carpeta de procesando a rechazados
            nombre_archivo_path = f"{self.env.FOLDER_PROCESSING}{nombre_archivo_zip}"
            if self.s3_service.move_file(
                self.env.BUCKET,
                nombre_archivo_path,
                self.env.FOLDER_REJECTED
                + timestamp.strftime("%Y%m")
                + "/",
                nombre_archivo_zip,
            ):
                self.logger_service.log_info(
                    f"Archivo movido a la carpeta rechazados/{nombre_archivo_zip}"
                )
            # Elimina el mensaje de la cola "pro_response_to_process"
            self.sqs_service.delete_message(
                queue_url=self.env.SQS_URL_PRO_RESPONSE_TO_PROCESS,
                receipt_handle=self.event_data["Records"][0]["receiptHandle"],
            )
            # Finaliza el proceso
            self.logger_service.log_info("Finaliza proceso de manejo de errores")
            sys.exit(0)

    def errors(self, file_data):
        """
        Se coloca el estado del proceso de cargue de
        respuesta en RECHAZADO y se registra el código y detalle de error
        """
        self.logger_service.log_info("Comienza el flujo de error")
        result = self.postgres_service.get_all(
            model=CGDCatalogoErrores,
            columns=[CGDCatalogoErrores.codigo_error, CGDCatalogoErrores.descripcion],
            conditions=[
                CGDCatalogoErrores.codigo_error == file_data.get("codigo_error")
            ],
        )
        if result[1]:
            self.logger_service.log_error(
                "=== CONSULTA TIPO DE ERROR ====>" + (result[2] if result[2] else "")
            )
            return None

        codigo = result[0][0]["codigo_error"]
        descripcion_error = result[0][0]["descripcion"]
        fecha_datetime = datetime.strptime(str(datetime.now()), "%Y-%m-%d %H:%M:%S.%f")
        try:
            if self.s3_service.move_file(
                self.env.BUCKET,
                file_data.get("file_name"),
                self.env.FOLDER_REJECTED
                + fecha_datetime.strftime("%Y%m")
                + "/",
                extract_string_after_slash(file_data.get("file_name")),
            ):
                self.logger_service.log_info(
                    "Archivo movido a la carpeta rechazados/"
                    + extract_string_after_slash(file_data.get("file_name"))
                )
            else:
                self.logger_service.log_error(
                    "No se pudo mover a la carpeta rechazados"
                )
                return None
            self.logger_service.log_info("Inicio envío mensaje cola 'Envío de correos'")
            values_plantilla = {
                "id_plantilla": "PC009",
                "codigo_rechazo": codigo,
                "descripcion_rechazo": descripcion_error,
                "fecha_recepcion": fecha_datetime.strftime("%d/%m/%Y"),
                "hora_recepcion": fecha_datetime.strftime("%I:%M %p"),
                "nombre_respuesta_pro_tu": extract_string_after_slash(
                    file_data.get("file_name")
                ),
            }
            message_structured = self._message_structure(values=values_plantilla)
            self.sqs_service.send_message(
                self.env.SQS_URL_EMAILS,
                json.dumps(message_structured),
                0,
            )
            self.logger_service.log_info("Fin envío mensaje cola 'Envío de correos'")
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

            if not messages_queue or error:
                self.logger_service.log_error(
                    "No se encontraron mensajes en la cola SQS."
                )
                raise ValueError("No se encontraron mensajes en la cola SQS.")

            for message in messages_queue:
                receipt_handle = message.get("ReceiptHandle")

            if receipt_handle is None:
                self.logger_service.log_error(
                    "Error, el receiptHandle de la cola es null"
                )
                raise ValueError("El receiptHandle no debe ser None")

            if self.sqs_service.delete_message(
                self.env.SQS_URL_PRO_RESPONSE_TO_PROCESS, receipt_handle
            ):
                self.logger_service.log_info("Fin eliminación mensaje de cola")
                self.logger_service.log_info(
                    "FIN PROCESO MANEJO DE ERRORES PARA ESTADO: "
                    + file_data.get("estado")
                )
                sys.exit(0)
            else:
                self.logger_service.log_info("FIN ETAPA DE ERROR")
                self.logger_service.log_error("Error al eliminar el mensaje de la cola")
                return None

            if file_data.get("estado") == "PROCESADO" and file_data.get("valor"):
                return None

        except (ValueError, KeyError) as e:
            self.logger_service.log_error(f"Error {str(e)}")
            return None
