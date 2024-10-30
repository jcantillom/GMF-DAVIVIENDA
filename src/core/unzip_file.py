"""
Modulo para realizar las Acciones del flujo Normal.
"""

from typing import Dict, Any
from datetime import datetime
import zipfile
import os
import shutil
import boto3
from src.utils.environment import Environment
from src.services.logger_service import LoggerService
from src.core.error_handling import ErrorHandling


class Unzipfile:
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
        # Atributo para el manejo de los errores
        self.error_handling: ErrorHandling = error_handling
        # Atributo para registrar logs
        self.logger_service: LoggerService = services["logger_service"]

    def download_zip_file_from_s3(self, s3, bucket_name, zip_file_key, project_root):
        """
        Downloads a zip file from S3 and returns the local path.
        """
        local_zip_file = os.path.basename(zip_file_key)
        local_zip_path = os.path.join("./tmp", local_zip_file)
        s3.download_file(bucket_name, zip_file_key, local_zip_path)
        return local_zip_file, local_zip_path

    def unzip_file(self, local_zip_path, file_name):
        """
        Unzips the file and returns the unzipped folder name and path.
        """
        zip_base_name = os.path.splitext(os.path.basename(local_zip_path))[0]
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unzipped_folder_name = f"{zip_base_name}_{timestamp}"
        unzipped_path = os.path.join("/tmp/", unzipped_folder_name)
        os.makedirs(unzipped_path, exist_ok=True)

        with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
            zip_ref.extractall(unzipped_path)

        return unzipped_folder_name, unzipped_path

    def upload_unzipped_files_to_s3(
        self, bucket_name, folder_name, unzipped_folder_name, unzipped_path
    ):
        """
        Uploads the unzipped files to S3.
        """
        s3 = boto3.client("s3", region_name=self.env.REGION_ZONE, endpoint_url=self.env.LOCALSTACK_ENDPOINT)
        for root, _, files in os.walk(unzipped_path):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, unzipped_path)
                s3_key = (
                    f"{folder_name.rstrip('/')}/"
                    f"{unzipped_folder_name}/"
                    f"{relative_path.replace(os.sep, '/')}"
                )
                try:
                    with open(file_path, "rb") as f:
                        self.logger_service.log_debug(
                            f"Uploading {file_path} to {s3_key}"
                        )
                        s3.upload_fileobj(f, bucket_name, s3_key)
                    self.logger_service.log_debug(
                        f"File {file_path} uploaded successfully as {s3_key}"
                    )
                except (ValueError, KeyError) as e:
                    self.logger_service.log_error(
                        f"Error uploading {file_path} to S3: {str(e)}"
                    )

    def unzip_file_data(self, bucket_name, folder_name, project_root, file_name):
        """
        Main function to download, unzip, upload, and clean up zip file data from S3.
        """
        s3 = boto3.client("s3", region_name=self.env.REGION_ZONE, endpoint_url=self.env.LOCALSTACK_ENDPOINT)
        unzipped_folder_name = ""
        local_zip_path = ""

        try:
            # List objects in the S3 folder
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
            objects = response.get("Contents", [])

            # Find the zip file in the S3 folder
            zip_file_key = next(
                (obj["Key"] for obj in objects if obj["Key"].endswith(".zip")), None
            )
            if zip_file_key is None:
                raise FileNotFoundError(f"No zip file found in folder {folder_name}.")

            # Download the zip file
            local_zip_file, local_zip_path = self.download_zip_file_from_s3(
                s3, bucket_name, zip_file_key, project_root
            )

            # Unzip the file
            unzipped_folder_name, unzipped_path = self.unzip_file(
                local_zip_path, project_root
            )

            # Upload the unzipped files to S3
            self.upload_unzipped_files_to_s3(
                bucket_name, folder_name, unzipped_folder_name, unzipped_path
            )

            # Delete the original zip file from S3
            s3.delete_object(Bucket=bucket_name, Key=zip_file_key)
            self.logger_service.log_debug(
                f"Zip file {local_zip_file} unzipped to {folder_name}"
                f"/{unzipped_folder_name} and deleted."
            )

            return True, unzipped_folder_name

        except (ValueError, KeyError, FileNotFoundError) as e:
            self.logger_service.log_error(f"Error: {str(e)}")
            return False, unzipped_folder_name

        finally:
            # Cleanup local files
            if local_zip_path and os.path.exists(local_zip_path):
                os.remove(local_zip_path)
            if unzipped_folder_name and os.path.exists(
                os.path.join(project_root, unzipped_folder_name)
            ):
                shutil.rmtree(
                    os.path.join(project_root, unzipped_folder_name), ignore_errors=True
                )
            self.logger_service.log_debug("Temporary files cleaned up.")

    def read_s3(self, bucket_name, folder_name, file_name):
        """
        Ejemplo de uso:
        Definir el directorio raíz del proyecto
        project_root = os.path.dirname(os.path.abspath(__file__))

        bucket_name = "01-bucketrtaprocesa-d01"
        folder_name = "procesando"
        unzip_file(bucket_name, folder_name, project_root)
        """
        lista_archivos = []
        carpeta_mas_reciente = None
        fechas_carpetas = []
        s3 = boto3.client("s3", region_name=self.env.REGION_ZONE, endpoint_url=self.env.LOCALSTACK_ENDPOINT)
        try:

            # Listar todas las carpetas que comienzan con el prefijo carpeta_inicial
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

            # Buscar carpetas que coincidan con el nombre base
            for obj in response.get("Contents", []):
                carpeta_completa = obj["Key"]

                # Verificar si la carpeta comienza con la cadena que tienes
                if carpeta_completa.startswith(folder_name + file_name):
                    # Extraer el nombre de la carpeta (segunda parte del Key)
                    carpeta_actual = carpeta_completa.split("/")[1]
                    if "_" in carpeta_actual:
                        fechas_carpetas.append(
                            (carpeta_actual, carpeta_actual.split("_")[-1])
                        )

            # Ordenar por la fecha y seleccionar la más reciente
            if fechas_carpetas:
                carpeta_mas_reciente = max(fechas_carpetas, key=lambda x: x[1])[0]

                # Listar los archivos dentro de la carpeta más reciente
                prefix_carpeta_mas_reciente = folder_name + carpeta_mas_reciente + "/"
                response_archivos = s3.list_objects_v2(
                    Bucket=bucket_name, Prefix=prefix_carpeta_mas_reciente
                )

                # Añadir solo la última parte de la ruta (el nombre del archivo)
                lista_archivos = [
                    archivo["Key"].split("/")[-1]
                    for archivo in response_archivos.get("Contents", [])
                ]

            return lista_archivos, carpeta_mas_reciente

        except (ValueError, KeyError) as e:
            self.logger_service.log_error(f"Error: {str(e)}")
            return False, None

    def move_folder(self, bucket_name, source_folder, destination_folder):
        """
        Mueve una carpeta con todas sus subcarpetas y archivos
        de una ubicación en S3 a otra dentro del mismo bucket,
        manteniendo la estructura de carpetas y subcarpetas.

        :param bucket_name: Nombre del bucket de S3
        :param source_folder: Ruta de la carpeta de origen en S3
        :param destination_folder: Ruta de la carpeta de destino en S3
        """

        s3 = boto3.resource("s3", region_name=self.env.REGION_ZONE)

        try:
            # Seleccionar el bucket
            bucket = s3.Bucket(bucket_name)

            for obj in bucket.objects.filter(Prefix=source_folder):
                source_key = obj.key

                destination_key = destination_folder + source_key[len(source_folder) :]

                # Copiar el objeto al nuevo destino
                s3.Object(bucket_name, destination_key).copy_from(
                    CopySource={"Bucket": bucket_name, "Key": source_key}
                )

                # Eliminar el objeto del origen después de la copia
                s3.Object(bucket_name, source_key).delete()

            self.logger_service.log_debug(
                f"Todos los archivos y subcarpetas de {source_folder}"
                f"fueron movidos a {destination_folder}."
            )

        except (ValueError, KeyError) as e:
            self.logger_service.log_error(f"Error: {str(e)}")
