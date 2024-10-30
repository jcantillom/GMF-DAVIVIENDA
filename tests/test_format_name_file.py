import unittest
from unittest.mock import patch
from src.core.format_name_file import (
    extract_name_file,
    check_prefix_esp,
    validate_well_formed_esp,
    extract_string_after_slash,
    extract_text_type,
    format_id_archivo,
)


class TestFormatNameFile(unittest.TestCase):
    
    def test_extract_name_file(self):
        archivo_ruta_1 = "recibidos/RE_PRO_TUTGMF0001003920240802-0001.zip"
        resultado_1 = extract_name_file(archivo_ruta_1)
        self.assertEqual(resultado_1, "TUTGMF0001003920240802-0001")

        archivo_ruta_2 = "recibidos/RE_ESP_TUTGMF0001003920240802-0001.zip"
        resultado_2 = extract_name_file(archivo_ruta_2)
        self.assertEqual(resultado_2, "RE_ESP_TUTGMF0001003920240802-0001")

        archivo_ruta_3 = "recibidos/TUTGMF0001003920240802.zip"
        resultado_3 = extract_name_file(archivo_ruta_3)
        self.assertIsNone(resultado_3)


    def test_check_prefix_esp(self):
        archivo_ruta = "recibidos/RE_ESP_TUTGMF0001003920240802-0001.zip"
        resultado = check_prefix_esp(archivo_ruta)
        self.assertTrue(resultado)

        archivo_ruta = "recibidos/RE_PRO_TUTGMF0001003920240802-0001.zip"
        resultado = check_prefix_esp(archivo_ruta)
        self.assertFalse(resultado)

    @patch('src.core.format_name_file.DatetimeManagement.get_datetime')
    def test_validate_well_formed_esp(self, mock_get_datetime):
        # Mock current datetime to a fixed date for comparison
        mock_get_datetime.return_value = {"timestamp": "20240803000000.000000"}

        archivo_ruta = "recibidos/RE_ESP_TUTGMF0001003920240802-0001.zip"
        startspecialfiles = "RE_ESP_TUTGMF00010039"  
        endspecialfiles = "-0001"  

        resultado = validate_well_formed_esp(archivo_ruta, startspecialfiles, endspecialfiles)
        self.assertTrue(resultado) 
        archivo_ruta_2 = "recibidos/RE_ESP_TUTGMF0001003920240803-0002.zip"
        resultado_2 = validate_well_formed_esp(archivo_ruta_2, startspecialfiles, endspecialfiles)
        self.assertFalse(resultado_2)  

    def test_extract_string_after_slash(self):
        cadena = "recibidos/RE_PRO_TUTGMF0001003920240802-0001.zip"
        resultado = extract_string_after_slash(cadena)
        self.assertEqual(resultado, "RE_PRO_TUTGMF0001003920240802-0001.zip")

    def test_extract_text_type(self):
        archivo = "RE_TUTGMF0001003920240930-0001-CONTROLTX.TXT"
        resultado = extract_text_type(archivo)
        self.assertEqual(resultado, "CONTROLTX") 

        archivo = "RE_PRO_TUTGMF0001003920240802.zip"
        resultado = extract_text_type(archivo)
        self.assertIsNone(resultado)  

    def test_format_id_archivo(self):
        nombre_archivo_sin_extension = "TUTGMF0001003920240802"
        resultado = format_id_archivo(nombre_archivo_sin_extension)
        self.assertEqual(resultado, "201050802")

        nombre_archivo_sin_extension_2 = "2-0001"
        resultado_2 = format_id_archivo(nombre_archivo_sin_extension_2)
        self.assertEqual(resultado_2, "01050001")


if __name__ == "__main__":
    unittest.main()
