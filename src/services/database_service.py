""" Modulo para interactuar con la Base de Datos. """

# Dependencias
import re
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

# Dependencias externas
from sqlalchemy import Connection, Engine, Row, and_, create_engine, event, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Query, Session, sessionmaker
from sqlalchemy.orm.attributes import InstrumentedAttribute

# pylint: disable=import-error
# Models
from src.models.base import Base

# Services
from src.services.logger_service import LoggerService
from src.services.secrets_service import SecretsService

# Utils
from src.utils.environment import Environment
from src.utils.singleton import Singleton


class DatabaseService(metaclass=Singleton):
    """
    Clase para gestionar la conexión y operaciones con la Base de Datos usando SQLAlchemy.

    Esta clase implementa el patrón Singleton para garantizar que solo haya una instancia de la
    conexión a la base de datos.
    """

    # Bandera para asegurar que la inicialización de la instancia se realice solo una vez
    _initialized: bool = False

    def __init__(
        self,
        env: Environment,
        secrets_service: SecretsService,
        logger_service: LoggerService,
    ) -> None:
        """
        Inicializa la conexión a la Base de Datos usando SQLAlchemy.

        Args:
            env (Environment):
                Instancia con los valores de las variables de entorno.
            secrets_service (SecretsService):
                Instancia con los valores de los secrets.
            logger_service (LoggerService):
                Servicio de logging para registrar errores y eventos.

        Raises:
            SQLAlchemyError:
                Si ocurre un error al realizar una operación con la Base de Datos.
        """
        # Valida si ya existe una instancia
        if not self._initialized:
            # Cambia el estado de la bandera para indicar que ya existe una instancia
            self._initialized = True
            # Atributo para registrar logs
            self.logger_service: LoggerService = logger_service

            # Inicializa una conexión para interactuar con la Base de Datos
            self.engine: Engine = create_engine(
                f'postgresql://{secrets_service.USERNAME}:{secrets_service.PASSWORD}'
                f'@{env.DB_HOST}:{env.DB_PORT}/{env.DB_NAME}'
            )
            # Crea una fábrica de sesiones para producir instancias de sesiones de la base de datos
            self.session_factory: sessionmaker = sessionmaker(bind=self.engine)

            # Evento para capturar las consultas SQL
            self._attach_sql_listener()

    def _attach_sql_listener(self):
        """
        Configura un listener para capturar y registrar consultas SQL antes de su ejecución.
        Esto permite monitorear las consultas que se ejecutan en la base de datos, junto
        con sus parámetros asociados, para facilitar el debugging y la auditoría.

        El listener utiliza el evento before_cursor_execute, que es disparado justo antes de que
        una sentencia SQL sea enviada al cursor de la base de datos.

        Se registra la consulta y los parámetros en debug con el servicio de logging definido en la
        instancia.
        """
        # Define el evento que se ejecuta antes de la ejecución de cada consulta SQL
        @event.listens_for(self.session_factory().bind.engine, "before_cursor_execute")
        def before_cursor_execute(
            _conn: Connection,
            _cursor: Any,
            *args: Union[str, tuple, dict],
            **_kwargs: Any,
        ) -> None:
            """
            Listener que se activa antes de ejecutar un cursor en la base de datos.

            Args:
                _conn (Connection):
                    Conexión activa a la base de datos.
                _cursor (Any):
                    Cursor de la base de datos que ejecutará la consulta.
                *args (Union[str, tuple, dict]):
                    Parámetros posicionales que incluyen:
                        args[0] - statement (str):
                            Sentencia SQL que será ejecutada.
                        args[1] (Union[tuple, dict]):
                    Parámetros asociados a la sentencia SQL.
                **kwargs (Any):
                    Parámetros adicionales que pueden incluir información como el contexto de
                    ejecución u otros datos de metadatos no obligatorios.
            """
            try:
                # Obtiene los argumentos posicionales
                statement = args[0]
                parameters = args[1]

                # Elimina los saltos de linea de la sentencia
                formatted_statement: str = statement.replace("\n", "")

                # Registrar log de debug de la sentencia SQL y los parámetros
                self.logger_service.log_debug(
                    f'SQL Query: {formatted_statement}, Params: {parameters}'
                )
            except IndexError:
                pass

    def query(
        self,
        sql_query: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[list, bool, str]:
        """
        Ejecuta una consulta SQL nativa y retorna los resultados en formato JSON si es una
        operación de consulta. Para las operaciones de modificación de datos ejecuta el query.

        Args:
            sql_query (str):
                Consulta SQL a ejecutar.
            params (Optional[Dict[str, Any]]):
                Parámetros para la consulta SQL.

        Returns:
            Tuple[List[Dict[str, Any]], bool, str]:
                Resultados exitosos, indicador de error y descripción de error.

        Raises:
            IntegrityError:
                Si ocurre un error de integridad de datos.
            SQLAlchemyError:
                Si ocurre un error al realizar una operación con la Base de Datos.
        """
        # Registra log informativo de inicio de operación de query en la Base de Datos
        # Definición de los variables de la tupla a retornar
        results: list = []
        error: bool = False
        description: str = ""
        
        # Se utiliza un bloque with para gestionar la sesión de SQLAlchemy
        with Session(self.engine) as session:
            # PRUEBA
            try:
                # Ejecuta la consulta SQL
                result: Result[Any] = session.execute(text(sql_query), params)
                # Valida si la SQL es de consulta
                if sql_query.strip().upper().startswith("SELECT"):
                    # Obtiene las filas y las columnas del resultado
                    rows: List[Row[Any]] = result.fetchall()
                    columns: List[str] = result.keys()
                    # Obtiene el resultado en formato JSON
                    results = self._convert_to_json(result=rows, columns=columns)
                else:
                    # Si la SQL no es una consulta SELECT, se realiza un commit en la sesión
                    # para aplicar los cambios y se devuelve una lista vacía [].
                    session.commit()
            except SQLAlchemyError as e:
                # Cambia estado del error y obtiene la descripción del error
                error = True
                description = (
                    f'Error PostgreSQL {{1}}" "1=Error al ejecutar el query: {e}'
                )
                # Se hace un rollback de la transacción para deshacer cualquier cambio no
                # confirmado y luego se lanza la excepción
                session.rollback()
        
        return results, error, description

    def get_all(
        self,
        model: Optional[Type[Base]] = None,
        conditions: Optional[List[Any]] = None,
        columns: Optional[List[Any]] = None,
        order_by: Optional[List[Any]] = None,
    ) -> Tuple[List[Dict[str, Any]], bool, str]:
        """
        Recupera todos los registros de una tabla.

        Args:
            model (Optional[Type[Base]]):
                Clase del modelo (entidad) de SQLAlchemy.
            conditions (Optional[List[Any]]):
                Lista de condiciones para filtrar los resultados.
            columns (Optional[List[Any]]):
                Lista de las columnas que se requieren obtener.
            order_by (Optional[List[Any]]):
                Lista de columnas para ordenar los resultados.

        Returns:
            Tuple[List[Dict[str, Any]], bool, str]:
                Resultados exitosos, indicador de error y descripción de error.

        Raises:
            IntegrityError:
                Si ocurre un error de integridad de datos.
            SQLAlchemyError:
                Si ocurre un error al realizar una operación con la Base de Datos.
        """
        # Registra log informativo de inicio de operación de get_all en la Base de Datos
        self.logger_service.log_info("Inicia operacion de get_all en la Base de Datos")

        # Valida que se envíe el modelo o las columnas para realizar la consulta
        error: bool = self._validate_model_and_columns(
            model=model,
            columns=columns,
        )
        if error:
            return [], error

        # Ejecuta y retorna los datos del resultado del query SQL
        return self._execute_query(
            lambda session: self._build_query(
                session, model, conditions, columns, order_by
            ).all(),
            "get_all",
            columns,
            is_select=True,
        )

    def get_by_id(
        self,
        model: Type[Base],
        record_id: Any,
        columns: Optional[List[Any]] = None,
        id_name: Optional[str] = "id",
        order_by: Optional[List[Any]] = None,
    ) -> Tuple[Dict[str, Any], bool, str]:
        """
        Recupera un registro de una tabla por su ID.

        Args:
            model (List[Any]):
                Clase del modelo (entidad) de SQLAlchemy.
            record_id (Any):
                ID del registro a recuperar.
            columns (Optional[List[Any]]):
                Lista de las columnas que se requieren obtener.
            id_name (Optional[str]):
                Nombre del ID de la tabla. Por defecto se busca por la columna id.
            order_by (Optional[List[Any]]):
                Lista de columnas para ordenar los resultados.

        Returns:
            Tuple[Dict[str, Any], bool, str]:
                Resultados exitosos, indicador de error y descripción de error.

        Raises:
            IntegrityError:
                Si ocurre un error de integridad de datos.
            SQLAlchemyError:
                Si ocurre un error al realizar una operación con la Base de Datos.
        """
        # Registra log informativo de inicio de operación de get_by_id en la Base de Datos
        self.logger_service.log_info("Inicia operacion de get_by_id en la Base de Datos")

        # Obtiene el atributo de la columna de ID dinámicamente
        id_name_attr = getattr(model, id_name)

        # Ejecuta y retorna los datos del resultado del query SQL
        return self._execute_query(
            lambda session: self._build_query(
                session, model, [id_name_attr == record_id], columns, order_by
            ).first(),
            "get_by_id",
            columns,
            is_select=True,
        )

    def get_by_id_all(
        self,
        model: Type[Base],
        record_id: Any,
        columns: Optional[List[Any]] = None,
        id_name: Optional[str] = "id",
        order_by: Optional[List[Any]] = None,
    ) -> Tuple[Dict[str, Any], bool, str]:
        """
        Recupera todos los registros de una tabla por su ID.

        Args:
            model (List[Any]):
                Clase del modelo (entidad) de SQLAlchemy.
            record_id (Any):
                ID del registro a recuperar.
            columns (Optional[List[Any]]):
                Lista de las columnas que se requieren obtener.
            id_name (Optional[str]):
                Nombre del ID de la tabla. Por defecto se busca por la columna id.
            order_by (Optional[List[Any]]):
                Lista de columnas para ordenar los resultados.

        Returns:
            Tuple[Dict[str, Any], bool, str]:
                Resultados exitosos, indicador de error y descripción de error.

        Raises:
            IntegrityError:
                Si ocurre un error de integridad de datos.
            SQLAlchemyError:
                Si ocurre un error al realizar una operación con la Base de Datos.
        """
        # Registra log informativo de inicio de operación de get_by_id en la Base de Datos
        self.logger_service.log_info("Inicia operacion de get_by_id en la Base de Datos")

        # Obtiene el atributo de la columna de ID dinámicamente
        id_name_attr = getattr(model, id_name)

        # Ejecuta y retorna los datos del resultado del query SQL
        return self._execute_query(
            lambda session: self._build_query(
                session, model, [id_name_attr == record_id], columns, order_by
            ),
            "get_by_id",
            columns,
            is_select=True,
        )

    def insert(
        self,
        model_instance: Base,
    ) -> Tuple[List[Dict[str, Any]], bool, str]:
        """
        Inserta un registro en la base de datos.

        Args:
            model_instance (Union[Base, Dict[str, Any]]):
                Instancia del modelo (entidad) o diccionario con los datos a insertar.

        Returns:
            Tuple[List[Dict[str, Any]], bool, str]:
                Resultados exitosos, indicador de error y descripción de error.

        Raises:
            IntegrityError:
                Si ocurre un error de integridad de datos.
            SQLAlchemyError:
                Si ocurre un error al realizar una operación con la Base de Datos.
        """
        # Registra log informativo de inicio de operación de insert en la Base de Datos
        self.logger_service.log_info("Inicia operacion de insert en la Base de Datos")

        # Ejecuta y retorna los datos del resultado del query SQL
        return self._execute_query(
            lambda session: session.add(model_instance),
            "insert",
        )

    def insert_many(
        self,
        model_instances: List[Base],
    ) -> Tuple[List[Dict[str, Any]], bool, str]:
        """
        Inserta múltiples registros en la base de datos.

        Args:
            model_instances (List[Base]):
                Lista de instancias del modelo (entidad) a insertar.

        Returns:
            Tuple[List[Dict[str, Any]], bool, str]:
                Resultados exitosos, indicador de error y descripción de error.

        Raises:
            IntegrityError:
                Si ocurre un error de integridad de datos.
            SQLAlchemyError:
                Si ocurre un error al realizar una operación con la Base de Datos.
        """
        # Registra log informativo de inicio de operación de insert_many en la Base de Datos
        self.logger_service.log_info("Inicia operacion de insert_many en la Base de Datos")

        # Ejecuta y retorna los datos del resultado del query SQL
        return self._execute_query(
            lambda session: session.bulk_save_objects(model_instances),
            "insert_many",
        )

    def update_all(
        self,
        model: Type[Base],
        updates: Dict[str, Any],
        conditions: Optional[List[Any]] = None,
    ) -> Tuple[List[Dict[str, Any]], bool, str]:
        """
        Actualiza los registros de una tabla en la base de datos.

        Args:
            model (Type[Base]):
                Clase del modelo (entidad) de SQLAlchemy.
            updates (Dict[str, Any]):
                Datos a actualizar.
            conditions (Optional[List[Any]]):
                Condiciones para especificar las filas a actualizar.

        Returns:
            Tuple[List[Dict[str, Any]], bool, str]:
                Resultados exitosos, indicador de error y descripción de error.

        Raises:
            IntegrityError:
                Si ocurre un error de integridad de datos.
            SQLAlchemyError:
                Si ocurre un error al realizar una operación con la Base de Datos.
        """
        # Registra log informativo de inicio de operación de update_all en la Base de Datos
        self.logger_service.log_info("Inicia operacion de update_all en la Base de Datos")

        # Ejecuta y retorna los datos del resultado del query SQL
        return self._execute_query(
            lambda session: self._build_query(session, model, conditions).update(
                updates,
                synchronize_session=False,
            ),
            "update_all",
        )

    def update_by_id(
        self,
        model: Type[Base],
        record_id: Any,
        updates: Dict[str, Any],
        id_name: str = "id",
    ) -> Tuple[List[Dict[str, Any]], bool, str]:
        """
        Actualiza un registro específico de una tabla por su ID en la base de datos.

        Args:
            model (Type[Base]):
                Clase del modelo (entidad) de SQLAlchemy.
            record_id (Any):
                ID del registro a actualizar.
            updates (Dict[str, Any]):
                Datos a actualizar.
            id_name (Optional[str]):
                Nombre del ID de la tabla. Por defecto se busca por la columna id.

        Returns:
            Tuple[List[Dict[str, Any]], bool, str]:
                Resultados exitosos, indicador de error y descripción de error.

        Raises:
            IntegrityError:
                Si ocurre un error de integridad de datos.
            SQLAlchemyError:
                Si ocurre un error al realizar una operación con la Base de Datos.
        """
        # Registra log informativo de inicio de operación de update_by_id en la Base de Datos
        self.logger_service.log_info("Inicia operacion de update_by_id en la Base de Datos")

        # Ejecuta y retorna los datos del resultado del query SQL
        return self._execute_query(
            lambda session: self._find_record_by_id(
                session,
                model,
                record_id,
                updates,
                id_name,
            ),
            "update_by_id",
        )

    def delete_all(
        self,
        model: Type[Base],
        conditions: Optional[List[Any]] = None,
    ) -> Tuple[List[Dict[str, Any]], bool, str]:
        """
        Elimina los registros de una tabla en la base de datos.

        Args:
            model (Type[Base]):
                Clase del modelo (entidad) de SQLAlchemy.
            conditions (Optional[List[Any]]):
                Condiciones para especificar las filas a eliminar.

        Returns:
            Tuple[List[Dict[str, Any]], bool, str]:
                Resultados exitosos, indicador de error y descripción de error.

        Raises:
            IntegrityError:
                Si ocurre un error de integridad de datos.
            SQLAlchemyError:
                Si ocurre un error al realizar una operación con la Base de Datos.
        """
        # Registra log informativo de inicio de operación de delete_all en la Base de Datos
        self.logger_service.log_info("Inicia operacion de delete_all en la Base de Datos")

        # Ejecuta y retorna los datos del resultado del query SQL
        return self._execute_query(
            lambda session: self._build_query(session, model, conditions).delete(
                synchronize_session=False
            ),
            "delete_all",
        )

    def delete_by_id(
        self,
        model: Type[Base],
        record_id: Any,
        id_name: str = "id",
    ) -> Tuple[List[Dict[str, Any]], bool, str]:
        """
        Elimina un registro específico de una tabla por su ID en la base de datos.

        Args:
            model (Type[Base]):
                Clase del modelo (entidad) de SQLAlchemy.
            record_id (Any):
                ID del registro a eliminar.
            id_name (Optional[str]):
                Nombre del ID de la tabla. Por defecto se busca por la columna id.

        Returns:
            Tuple[List[Dict[str, Any]], bool, str]:
                Resultados exitosos, indicador de error y descripción de error.

        Raises:
            IntegrityError:
                Si ocurre un error de integridad de datos.
            SQLAlchemyError:
                Si ocurre un error al realizar una operación con la Base de Datos.
        """
        # Registra log informativo de inicio de operación de delete_by_id en la Base de Datos
        self.logger_service.log_info("Inicia operacion de delete_by_id en la Base de Datos")

        # Ejecuta y retorna los datos del resultado del query SQL
        return self._execute_query(
            lambda session: self._find_record_by_id(
                session,
                model,
                record_id,
                id_name,
            ),
            "delete_by_id",
        )

    def _validate_model_and_columns(
        self,
        model: Type[Base],
        columns: List[Any],
    ) -> bool:
        """
        Valida que se envíe el modelo o las columnas para realizar la consulta.

        Args:
            model (Type[Base]):
                Clase del modelo (entidad) de SQLAlchemy.
            columns (List[Any]):
                Lista de las columnas que se requieren obtener.

        Returns:
            bool:
                Indicador de error.
        """
        # Define la variable del indicador de error
        error: bool = False

        # Valida que se envíe el modelo o las columnas para realizar la consulta
        if model is None and columns is None:
            # Cambia estado del error y obtiene la descripción del error
            error = True
            # Registra log del error al validar el envío del modelo o las columnas
            self.logger_service.log_error(
                'Error Base de Datos {{1}}" "1=Se debe proporcionar al menos el modelo '
                'o las columnas para realizar la consulta'
            )

        return error

    @staticmethod
    def _build_query(
        session: Session,
        model: Optional[Type[Base]] = None,
        conditions: Optional[List[Any]] = None,
        columns: Optional[List[Any]] = None,
        order_by: Optional[List[Any]] = None,
    ) -> Query:
        """
        Construye un query de consulta SQLAlchemy.

        Args:
            session (Session):
                Sesión de SQLAlchemy.
            model (Optional[Type[Base]]):
                Clase del modelo (entidad) de SQLAlchemy.
            conditions (Optional[List[Any]]):
                Lista de condiciones para filtrar los resultados.
            columns (Optional[List[Any]]):
                Lista de las columnas que se requieren obtener.
            order_by (Optional[List[Any]]):
                Lista de columnas para ordenar los resultados.

        Returns:
            Query:
                Consulta SQLAlchemy construida.
        """
        # Valida si el resultado de la consulta se va a realizar al modelo o a columnas especificas
        query: Query = session.query(*columns) if columns else session.query(model)

        # Agrega las condiciones para filtrar los resultados
        if conditions:
            query = query.filter(and_(*conditions))

        # Agrega el ordenamiento de los resultados
        if order_by:
            query = query.order_by(*order_by)

        return query

    @staticmethod
    def _find_record_by_id(
        session: Session,
        model: Type[Base],
        record_id: Any,
        updates: Optional[Dict[str, Any]] = None,
        id_name: str = "id",
    ) -> None:
        """
        Busca un registro por su ID y opcionalmente lo actualiza o elimina.

        Args:
            session (Session):
                Sesión de SQLAlchemy.
            model (Type[Base]):
                Clase del modelo (entidad) de SQLAlchemy.
            record_id (Any):
                ID del registro a consultar.
            updates (Optional[Dict[str, Any]]):
                Datos a actualizar si la acción es 'update'.
            id_name (str):
                Nombre del ID de la tabla.

        Raises:
            ValueError: Si no se encuentra el registro con el ID dado.
        """
        # Obtiene el atributo de la columna de ID dinámicamente
        id_name_attr = getattr(model, id_name)

        # Query para la consulta del registro que se va actualizar/eliminar
        result: Union[Any, None] = (
            session.query(model).filter(id_name_attr == record_id).first()
        )

        # Valida si se encontró el ID
        if not result:
            raise ValueError(f'No se encontró el registro con el ID {record_id}')

        if updates:
            # Actualiza el registro especificado
            for key, value in updates.items():
                setattr(result, key, value)
        else:
            # Elimina el registro especificado
            session.delete(result)

    def _execute_query(
        self,
        query_func: Callable[[Session], Any],
        operation_name: str,
        columns: Optional[List[Any]] = None,
        is_select: bool = False,
    ) -> Tuple[Union[List[Dict[str, Any]], Dict[str, Any]], bool, str]:
        """
        Ejecuta una operación de consulta o modificación en la base de datos.

        Args:
            query_func (Callable[[Session], Any]):
                Función que ejecuta la consulta o modificación en la sesión de SQLAlchemy.
            operation_name (str):
                Nombre de la operación a ejecutar (para fines de registro).
            columns (Optional[List[Any]]):
                Lista de columnas esperadas en el resultado.
            is_select (bool):
                Indica si la operación es una consulta select.

        Returns:
            Tuple[Union[List[Dict[str, Any]], Dict[str, Any]], bool, str]:
                Resultados exitosos, indicador de error y descripción de error.

        Raises:
            IntegrityError:
                Si ocurre un error de integridad de datos.
            SQLAlchemyError:
                Si ocurre un error al realizar una operación con la Base de Datos.
        """
        # Definición de las variables de la tupla a retornar
        results: Union[Dict[str, Any], List[Dict[str, Any]]] = {} if operation_name == "get_by_id" else []
        error: bool = False
        failed_results: str = ""

        # Se utiliza un bloque with para gestionar la sesión de SQLAlchemy
        with self.session_factory() as session:
            try:
                # Ejecuta la consulta o modificación SQL
                result: Any = query_func(session)

                # Commit para operaciones de modificación
                if not is_select:
                    session.commit()
                elif result:
                    if operation_name == "query":
                        # Obtiene las filas y las columnas del resultado
                        rows: List[Row[Any]] = result.fetchall()
                        columns: List[str] = result.keys()
                        # Convierte el resultado en formato JSON
                        results = self._convert_to_json(result=rows, columns=columns)
                    elif operation_name == "get_all":
                        # Convierte el resultado en formato JSON
                        results = self._convert_to_json(result=result, columns=columns)
                    elif operation_name == "get_by_id":
                        # Convierte el resultado en formato JSON
                        results = self._convert_to_json(
                            result=[result], columns=columns
                        )[0]

            except SQLAlchemyError as e:
                # Cambia estado del error y obtiene la descripción del error
                error = True

                # Procesa la cadena de error para omitir la parte no deseada
                error_message = str(getattr(e, "args", str(e)))
                match = re.search(r'\) (.+)', error_message)
                if match:
                    # Agrega la descripción del error
                    failed_results = match.group(1)
                else:
                    # Agrega la descripción del error
                    failed_results = error_message
                # Elimina los saltos de línea y la parte final no deseada
                failed_results = failed_results.replace('\\n', ' ').replace("',)", "").strip()

                # Registra log del error al realizar la operación en la Base de Datos
                self.logger_service.log_error(
                    f'Error Base de Datos {{1}}" '
                    f'"1=Error al ejecutar la operacion de {operation_name}: {failed_results}'
                )

                if not is_select:
                    # Registra log informativo de fin de la operación en la Base de Datos
                    self.logger_service.log_info(
                        f'Se realiza Rollback en la Base de Datos de la '
                        f'operacion de {operation_name}'
                    )
                    session.rollback()

        # Registra log informativo de fin de la operación en la Base de Datos
        self.logger_service.log_info(f'Finaliza operacion de {operation_name} en la Base de Datos')

        return results, error, failed_results

    @staticmethod
    def _convert_to_json(
        result: List[Any],
        columns: Optional[List[Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Convierte los resultados de la consulta a formato JSON.

        Args:
            result (List[Any]):
                Resultados de la consulta.
            columns (Optional[List[Any]]):
                Lista de columnas esperadas.

        Returns:
            List[Dict[str, Any]]:
                Lista de los resultados de la consulta en formato JSON.
        """
        # Valida si se encontraros registros para la consulta
        if not result:
            return []

        # Valida si la consulta tiene columnas para transformar el resultado en formato json
        if columns:
            column_keys = [
                col.key if isinstance(col, InstrumentedAttribute) else col
                for col in columns
            ]
            return [
                {
                    (
                        key.split(".")[-1]
                        if isinstance(key, str) and "." in key
                        else key
                    ): value
                    for key, value in zip(column_keys, row)
                }
                for row in result
            ]

        # Transforma el resultado de la consulta del modelo en formato json
        return [
            {
                key: value
                for key, value in row.__dict__.items()
                if key != "_sa_instance_state"
            }
            for row in result
        ]
        
    def get_by_id_query(self, valor: str, codigo_dominio: str):
        query = """ 
            select * from cgd_dominios where codigo_dominio = :codigo_dominio and valor = :valor
        """
        params = {
            'valor': valor,
            'codigo_dominio': codigo_dominio
        }
        return query, params
