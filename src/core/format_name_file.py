"""
Modulo que formatea el nombre
"""
from datetime import datetime
from src.utils.datetime_management import DatetimeManagement



def extract_name_file(archivo_ruta):
    """
    Funcinon que permite formatear el nombre de un archivo
    para poder ser comparado
    en base de datos
    Args:
        archivo_ruta (_type_): _description_

    Returns:
        _type_: _description_
    """
    print("extract_name_file", archivo_ruta)
    opciones_prefijo = ["RE_PRO_", "RE_ESP_", "RE_PRE_"]

    archivo_sin_extension = archivo_ruta[:-4]

    for prefijo in opciones_prefijo:
        if prefijo in archivo_sin_extension:
            if prefijo == "RE_ESP_":
                codigo = archivo_sin_extension.split("/")[-1]
                print("codigo", codigo)
                return codigo
            codigo = archivo_sin_extension.split(prefijo)[-1]
            print("codigo", codigo)
            return codigo

    return None


def check_prefix_esp(archivo_ruta):
    """
    Ejemplo de uso
    ruta_archivo = "recibidos/RE_PRO_TUTGMF0001003920240802-0001.zip"
    codigo_extraido = format_name_file(ruta_archivo)
    Args:
        archivo_ruta (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Extraer la parte de la ruta después de la última barra "/"
    archivo_nombre = archivo_ruta.split("/")[-1]

    # Buscar y extraer el prefijo (primeros 7 caracteres)
    prefijo = archivo_nombre[:7]

    # Verificar si el prefijo es "RE_ESP_"
    if prefijo == "RE_ESP_":
        return True
    return False


def validate_well_formed_esp(archivo_ruta, startspecialfiles, endspecialfiles):
    """
    Ejemplo de uso
    ruta_archivo = "recibidos/RE_ESP_TUTGMF0001003920240802-0001.zip"
    es_re_esp = verificar_prefijo(ruta_archivo)
    Args:
        archivo_ruta (_type_): _description_
        startspecialfiles (_type_): _description_
        endspecialfiles (_type_): _description_

    Returns:
        _type_: _description_
    """
    parte_fija1 = archivo_ruta[10:31]
    parte_fija2 = archivo_ruta[-9:-4]

    if parte_fija1 != startspecialfiles or parte_fija2 != endspecialfiles:
        return False

    fecha_str = archivo_ruta[31:39]

    try:
        fecha_archivo = datetime.strptime(fecha_str, "%Y%m%d").date()
    except ValueError:
        return False

    timestamp: datetime = DatetimeManagement.convert_string_to_date(
        DatetimeManagement.get_datetime()["timestamp"][:-3],
        date_format="%Y%m%d%H%M%S.%f",
    )
    fecha_actual = timestamp.date()
    if fecha_archivo <= fecha_actual:
        return True
    return False


def extract_string_after_slash(cadena):
    """
    Ejemplo de uso
    ruta_archivo = "recibidos/RE_ESP_TUTGMF0001003920240802-0001.zip"
    resultado = verificar_archivo(ruta_archivo)
    True si es 2024-08-02, de lo contrario False

    Args:
        cadena (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Dividimos la cadena en partes usando el caracter '/'
    partes = cadena.split("/")
    # Retornamos la parte después del primer '/'
    return partes[1] if len(partes) > 1 else None


def extract_text_type(archivo):
    """
    Extra el texto tipo
   

    Args:
        archivo (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Divide el nombre del archivo usando el guion como separador
    partes = archivo.split("-")

    # Toma la tercera parte, que es el texto después del segundo guion
    if len(partes) >= 3:
        texto = partes[-1].split(".")[0]
        return texto
    return None


def format_id_archivo(nombre_archivo_recibido_sin_extension):
    """
    Formate el id archivo

    Args:
        nombre_archivo_recibido_sin_extension (_type_): _description_

    Returns:
        _type_: _description_
    """
    id_archivo = (
        nombre_archivo_recibido_sin_extension[21:29]
        + "01"
        + "05"
        + nombre_archivo_recibido_sin_extension[-4:]
    )
    return id_archivo
