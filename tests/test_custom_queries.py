import unittest

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.core.custom_queries import (
    update_query_estado_rta_procesamiento_enviado,
    query_data_acg_rta_procesamiento_estado_enviado,
    query_data_acg_rta_procesamiento,
    insert_rta_procesamiento,
    insert_rta_pro_archivos,
    query_archivos_data_count_rta_pro_archivos,
)


class TestSQLQueries(unittest.TestCase):

    def test_update_query_estado_rta_procesamiento_enviado(self):
        id_archivo = '123456'
        query, params = update_query_estado_rta_procesamiento_enviado(id_archivo)
        expected_query = """
        UPDATE cgd_rta_procesamiento
        SET estado = 'ENVIADO'
        WHERE id_archivo = :id_archivo 
        AND estado <> 'ENVIADO'
        AND fecha_recepcion = (
            SELECT MAX(fecha_recepcion) 
            FROM cgd_rta_procesamiento 
            WHERE id_archivo = :id_archivo
        )
        """
        expected_params = {"id_archivo": id_archivo}

        self.assertEqual(query.strip(), expected_query.strip())
        self.assertEqual(params, expected_params)

    def test_query_data_acg_rta_procesamiento_estado_enviado(self):
        id_archivo = '123456'
        query, params = query_data_acg_rta_procesamiento_estado_enviado(id_archivo)
        expected_query = """
        SELECT id_archivo, estado
        FROM CGD_RTA_PROCESAMIENTO
        WHERE id_archivo = :id_archivo
        AND estado = 'ENVIADO'
        AND NOT EXISTS (
            SELECT 1
            FROM CGD_RTA_PROCESAMIENTO
            WHERE id_archivo = :id_archivo
            AND estado = 'INICIADO'
        )
        """
        expected_params = {"id_archivo": id_archivo}

        self.assertEqual(query.strip(), expected_query.strip())
        self.assertEqual(params, expected_params)

    def test_query_data_acg_rta_procesamiento(self):
        id_archivo = '123456'
        query, params = query_data_acg_rta_procesamiento(id_archivo)
        expected_query = """
        SELECT id_archivo, estado, tipo_respuesta, nombre_archivo_zip, fecha_recepcion, id_rta_procesamiento
        FROM CGD_RTA_PROCESAMIENTO
        WHERE id_archivo = :id_archivo
        ORDER BY fecha_recepcion DESC
        LIMIT 1
        """
        expected_params = {"id_archivo": id_archivo}

        self.assertEqual(query.strip(), expected_query.strip())
        self.assertEqual(params, expected_params)

    def test_insert_rta_procesamiento(self):
        # Datos de prueba
        id_archivo = '12345'
        nombre_archivo_zip = 'archivo_test.zip'
        tipo_respuesta = 'EXITO'

        # Llamamos a la función
        query, params = insert_rta_procesamiento(id_archivo, nombre_archivo_zip, tipo_respuesta)

        # Verificamos que la consulta SQL no esté vacía
        self.assertTrue(query)

        # Verificamos que los parámetros sean los esperados
        expected_params = {
            "id_archivo": id_archivo,
            "nombre_archivo_zip": nombre_archivo_zip,
            "tipo_respuesta": tipo_respuesta
        }
        self.assertEqual(params, expected_params)

        # Verificamos que la consulta SQL contenga las partes clave esperadas
        self.assertIn("INSERT INTO cgd_rta_procesamiento", query)
        self.assertIn(":id_archivo", query)
        self.assertIn(":nombre_archivo_zip", query)
        self.assertIn(":tipo_respuesta", query)
        self.assertIn("'INICIADO'", query)
        
    def test_insert_rta_pro_archivos(self):
        id_archivo = '123456'
        nombre_archivo = 'archivo_rta.zip'
        tipo_archivo_rta = 'RTA'
        query, params = insert_rta_pro_archivos(id_archivo, nombre_archivo, tipo_archivo_rta)
        expected_query = """WITH ultimo_procesamiento AS (
                SELECT id_rta_procesamiento
                FROM cgd_rta_procesamiento
                WHERE id_archivo = :id_archivo
                ORDER BY fecha_recepcion DESC
                LIMIT 1
            )
            INSERT INTO cgd_rta_pro_archivos (
                id_rta_procesamiento, id_archivo, nombre_archivo,
                tipo_archivo_rta, estado, contador_intentos_cargue
            ) VALUES (
                (SELECT id_rta_procesamiento FROM ultimo_procesamiento),
                :id_archivo,
                :nombre_archivo,
                :tipo_archivo_rta,
                :estado,
                0
            );"""
        expected_params = {
            "id_archivo": id_archivo,
            "nombre_archivo": nombre_archivo,
            "tipo_archivo_rta": tipo_archivo_rta,
            "estado": "PENDIENTE_INICIO",
        }

        self.assertEqual(query.strip(), expected_query.strip())
        self.assertEqual(params, expected_params)

    def test_query_archivos_data_count_rta_pro_archivos(self):
            id_archivo = '123456'
            id_rta_procesamiento = '987654'
            query, params = query_archivos_data_count_rta_pro_archivos(id_archivo, id_rta_procesamiento)
            
            expected_query = """
            SELECT COUNT(1) as cantidad_total_registros
            FROM CGD_RTA_PRO_ARCHIVOS
            WHERE id_archivo=:id_archivo AND id_rta_procesamiento=:id_rta_procesamiento
            """
            expected_params = {"id_archivo": id_archivo, "id_rta_procesamiento": id_rta_procesamiento}
            
            # Elimina los espacios adicionales y saltos de línea
            formatted_query = query.replace("\n", "").replace(" ", "")
            formatted_expected_query = expected_query.replace("\n", "").replace(" ", "")
            
            self.assertEqual(formatted_query, formatted_expected_query)
            self.assertEqual(params, expected_params)



if __name__ == "__main__":
    unittest.main()
