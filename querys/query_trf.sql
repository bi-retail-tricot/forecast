SELECT
  cod_sucursal,
  nombre_sucursal,
  nombre_temporada,
  ano_temporada,
  nombre_depto,
  nombre_linea,
  cod_producto,
  cod_talla,
  nom_talla,
  trf_h.numero_trf,
  nombre_razon,
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
LEFT JOIN `bold-momentum-270218.bo_data.maestro_sucursal_procesado` ms
  ON mtrf.codigo_sucursal = ms.cod_sucursal
WHERE
tipo_sucursal = 1
AND ano_temporada >= "2024"
ORDER BY
cod_sucursal,
nombre_temporada,
ano_temporada,
nombre_depto,
nombre_linea,
cod_producto,
cod_talla,
nombre_razon,
fecha_apr_ini,
fecha_des_ini