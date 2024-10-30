""" Modulo para obtener y convertir las fechas y horas. """

# Dependencias
from datetime import datetime
from typing import Dict, Optional

# Dependencias externas
from pytz import timezone
from pytz.tzinfo import BaseTzInfo


class DatetimeManagement:
    """
    Clase para obtener y convertir las fechas y horas.
    """

    # Constantes
    TIMEZONE_DEFAULT = "America/Bogota"
    TIMESTAMP_FORMAT = "%Y%m%d%H%M%S.%f"
    DATE_FORMAT = "%d/%m/%Y"
    TIME_FORMAT = "%I:%M %p"

    @classmethod
    def get_datetime(
        cls,
        time_zone: Optional[str] = TIMEZONE_DEFAULT,
    ) -> Dict[str, str]:
        """
        Obtiene la fecha y hora actual de Colombia.

        Args:
            time_zone (Optional[str]):
                Zona horaria a utilizar. Por defecto, 'America/Bogota'.

        Returns:
            Dict[str, str]:
                Diccionario con la información de la fecha y hora actual.
        """
        # Define la zona horaria especificada
        target_tz: BaseTzInfo = timezone(time_zone)

        # Obtiene la fecha y hora actual en la zona horaria especificada
        target_time: datetime = datetime.now(target_tz)

        # Formatea la fecha y la hora
        timestamp: str = target_time.strftime(cls.TIMESTAMP_FORMAT)
        date: str = target_time.strftime(cls.DATE_FORMAT)
        time: str = target_time.strftime(cls.TIME_FORMAT)

        return {
            "timestamp": timestamp,
            "date": date,
            "time": time,
        }

    @classmethod
    def convert_string_to_date(
        cls,
        date_str: str,
        date_format: Optional[str] = TIMESTAMP_FORMAT,
    ) -> datetime:
        """
        Convierte un string a un objeto datetime de acuerdo al formato especificado.

        Args:
            date_str (str):
                La fecha en formato string.
            date_format (Optional[str]):
                El formato de la fecha. Por defecto, '%Y%m%d%H%M%S'.

        Returns:
            datetime:
                Objeto datetime.
        """
        return datetime.strptime(date_str, date_format)

    @classmethod
    def convert_date_to_string(
        cls,
        date: datetime,
        date_format: Optional[str] = TIMESTAMP_FORMAT,
    ) -> str:
        """
        Convierte un objeto datetime a un string de acuerdo al formato especificado.

        Args:
            date (datetime):
                Objeto datetime a convertir.
            date_format (Optional[str]):
                El formato del string de fecha. Por defecto, '%Y%m%d%H%M%S'.

        Returns:
            str:
                La fecha en formato string.
        """
        return date.strftime(date_format)
