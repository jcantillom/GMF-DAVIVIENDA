""" Modulo para la implementación del patrón Singleton. """

# Dependencias
from typing import Dict, Type


class Singleton(type):
    """
    Metaclase para crear un singleton.
    """

    _instances: Dict[Type, "Singleton"] = {}

    def __call__(cls, *args: tuple, **kwargs: dict) -> "Singleton":
        """
        Controla la creación de instancias de la clase que utiliza esta Metaclase.

        Args:
            cls:
                La clase que está siendo instanciada.
            *args (tuple):
                Argumentos posicionales para el constructor de la clase.
            **kwargs (dict):
                Argumentos de palabras clave para el constructor de la clase.

        Returns:
            Singleton:
                La única instancia de la clase 'cls'.
        """
        # Verifica si ya existe una instancia de la clase 'cls'
        if cls not in cls._instances:
            # Si no existe, crea una nueva instancia y la guarda en '_instances'
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        # Retorna la instancia existente o la nueva instancia
        return cls._instances[cls]
