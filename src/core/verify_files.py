"""
    Modulo para realizar las Acciones del flujo Normal.
"""

import os
import sys
from typing import Dict, Any
import boto3
from src.utils.environment import Environment
from src.services.logger_service import LoggerService
from src.core.error_handling import ErrorHandling

class Verifyfiles:
    """
    Clase para realizar las Acciones del flujo Normal.
    """

    # Inicializar el cliente de S3

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
        # Atributo para el manejo de los errores
        self.error_handling: ErrorHandling = error_handling

    def verify_files_data(self, bucket_name, folder_name, path):
        """
        Verifica los datos del zip

        Args:
            bucket_name (_type_): _description_
            folder_name (_type_): _description_
            path (_type_): _description_

        Returns:
            _type_: _description_
        """
        s3 = boto3.client("s3", region_name=self.env.REGION_ZONE, endpoint_url=self.env.LOCALSTACK_ENDPOINT)
        try:
            folder_name = path + folder_name
            # Listar los objetos en la carpeta del bucket (con timestamp)
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
            objects = response.get("Contents", [])
            # Obtener el nombre de los archivos dentro de la carpeta con timestamp
            archivos = [
                obj["Key"].split("/")[-1]
                for obj in objects
                if not obj["Key"].endswith("/")
            ]
            # Contar cuántos archivos hay
            cantidad_archivos = len(archivos)
            self.logger_service.log_debug(
                f"Se encontraron {cantidad_archivos} archivos en la carpeta {folder_name}."
            )

            # Verificar si todos los archivos comienzan con "RE_"
            todos_comienzan_con_re = all(
                archivo.startswith("RE_") for archivo in archivos
            )
            if not todos_comienzan_con_re:
                self.logger_service.log_debug(
                    "No todos los archivos comienzan con 'RE_'."
                )

            # Definir los textos para cada caso
            textos_caso_2 = self.env.CONSTANTE_TU_DEBITO_REVERSO
            textos_caso_3 = self.env.CONSTANTES_TU_REINTEGROS
            textos_caso_4 = self.env.CONSTANTES_TU_ESPECIALES

            # Verificar casos
            coincidencias = []
            if cantidad_archivos == 5:
                coincidencias = [
                    texto
                    for texto in textos_caso_2
                    if any(texto in archivo for archivo in archivos)
                ]
                if coincidencias:
                    self.logger_service.log_debug(
                        f"Coincidencias encontradas en el caso 2: {coincidencias}"
                    )
                    return True, coincidencias, todos_comienzan_con_re, archivos, "01"

            elif cantidad_archivos == 3:
                coincidencias = [
                    texto
                    for texto in textos_caso_3
                    if any(texto in archivo for archivo in archivos)
                ]
                if coincidencias:
                    self.logger_service.log_debug(
                        f"Coincidencias encontradas en el caso 3: {coincidencias}"
                    )
                    return True, coincidencias, todos_comienzan_con_re, archivos, "02"

            elif cantidad_archivos == 2:
                coincidencias = [
                    texto
                    for texto in textos_caso_4
                    if any(texto in archivo for archivo in archivos)
                ]
                if coincidencias:
                    self.logger_service.log_debug(
                        f"Coincidencias encontradas en el caso 4: {coincidencias}"
                    )
                    return True, coincidencias, todos_comienzan_con_re, archivos, "03"

            # Si no se cumplen las condiciones anteriores
            self.logger_service.log_debug(
                "No se encontraron coincidencias para los casos definidos."
            )
            return False, [], todos_comienzan_con_re, [], "00"

        except (ValueError, KeyError) as e:
            self.logger_service.log_error(f"Error verificando archivos: {str(e)}")
            return False, [], False, [], "00"

    def validate_file_format(self, file_name):
        """
        Valida si el formato del archivo cumple con ciertas condiciones.
        """
        if file_name.endswith(".zip"):
            return True
        self.logger_service.log_error(f"Formato de archivo inválido para {file_name}")
        return False
