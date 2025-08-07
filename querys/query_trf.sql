SELECT
  cod_sucursal,
  cod_producto,
  cod_talla,
  trf_h.numero_trf,
  nombre_razon,
  cod_ano_comercial,
  cod_semana,
  fecha_apr_ini,
  fecha_des_ini,
  cantidad_apr,
  cantidad_can,
  cantidad_des,
FROM `bold-momentum-270218.bo_data.trf_h_resumen_procesado` trf_h
LEFT JOIN `bold-momentum-270218.bo_data.maestro_trf_procesado` mtrf
  ON trf_h.numero_trf = mtrf.numero_trf
LEFT JOIN `bold-momentum-270218.pbi_data.maestro_sku_procesado` msku
  ON trf_h.codigo_sku = msku.codigo_sku
LEFT JOIN `bold-momentum-270218.bo_data.maestro_sucursal` ms
  ON mtrf.codigo_sucursal = ms.cod_sucursal
LEFT JOIN `bold-momentum-270218.bo_data.tabla_fechas_view` tf
  ON trf_h.fecha_apr_ini = tf.cod_fecha
WHERE
tipo_sucursal IN (1, 5)
AND ano_temporada >= "2024"
-- AND cod_producto = 660102 AND cod_talla = 103 AND cod_sucursal = 108
ORDER BY
cod_sucursal,
cod_producto,
cod_talla,
nombre_razon,
fecha_apr_ini,
fecha_des_ini