WITH first_date AS (
  SELECT
    cod_producto,
    cod_talla,
    cod_sucursal,
    MIN(f.cod_fecha) AS first_date_on_store
  FROM `bold-momentum-270218.bo_data.fact_venta_stock_sku_tabla` v
  LEFT JOIN `bold-momentum-270218.bo_data.tabla_fechas_view` f USING(cod_fecha)
  WHERE can_unidad_stock_fis > 1
  GROUP BY 1, 2, 3
),
sales_weeks_base AS (
  SELECT
    ms.cod_sucursal,
    m.cod_producto,
    m.cod_talla,
    f.cod_ano_comercial,
    f.cod_fecha,
    f.cod_semana,
    f.cod_dia
  FROM `bold-momentum-270218.pbi_data.maestro_sku_procesado` m
  CROSS JOIN `bold-momentum-270218.bo_data.maestro_sucursal_procesado` ms
  CROSS JOIN `bold-momentum-270218.bo_data.tabla_fechas_view` f
  WHERE ms.tipo_sucursal IN (1,5) AND ms.min_venta IS NOT NULL
),
productos_temporada_filtrados AS (
  SELECT
    p.cod_producto,
    p.cod_talla,
    p.ano_temporada,
    p.nombre_temporada
  FROM `bold-momentum-270218.pbi_data.maestro_sku_procesado` p
  WHERE 
    (
      (p.nombre_temporada = {nombre_temporada} AND CAST(p.ano_temporada AS INT64) = {ano_temporada})
    )
),
calendar_base AS (
  SELECT
    b.cod_sucursal,
    b.cod_producto,
    b.cod_talla,
    b.cod_ano_comercial,
    b.cod_fecha,
    b.cod_semana,
    b.cod_dia
  FROM sales_weeks_base b
  JOIN productos_temporada_filtrados p USING(cod_producto, cod_talla)
  LEFT JOIN first_date d
    ON d.cod_producto = b.cod_producto
   AND d.cod_talla = b.cod_talla
   AND d.cod_sucursal = b.cod_sucursal
  WHERE 
    b.cod_fecha >= d.first_date_on_store
    AND (
      -- Temporada Invierno: semanas 45–52 del año anterior y 1–36 del año base
      (
        {nombre_temporada} = "Invierno" AND (
          (b.cod_ano_comercial = {ano_temporada} - 1)
          OR (b.cod_ano_comercial = {ano_temporada} AND b.cod_semana BETWEEN 1 AND 35)
        )
      )

      -- Temporada Verano: semanas 45–52 del año anterior y 1–8 del año base
      OR (
        {nombre_temporada} = "Verano" AND (
          (b.cod_ano_comercial = {ano_temporada})
          OR (b.cod_ano_comercial = {ano_temporada} + 1 AND b.cod_semana BETWEEN 1 AND 9)
        )
      )
    )
)
SELECT
  cb.cod_sucursal,
  cb.cod_producto,
  cb.cod_talla,
  cb.cod_ano_comercial,
  cb.cod_semana,
  IFNULL(SUM(CASE WHEN fv.cod_fecha = cb.cod_fecha THEN fv.can_unidad ELSE 0 END), 0) AS weekly_sales,
  IFNULL(SUM(CASE WHEN cb.cod_dia = 1 AND fv.cod_fecha = cb.cod_fecha THEN IFNULL(fv.can_unidad_stock_fis, 0) + IFNULL(fv.can_unidad, 0) ELSE 0 END), 0) AS stock_start_week,
  IFNULL(SUM(CASE WHEN cb.cod_dia = 7 AND fv.cod_fecha = cb.cod_fecha THEN fv.can_unidad_stock_fis ELSE 0 END), 0) AS stock_end_week,
  IFNULL(SUM(CASE WHEN fv.cod_fecha = cb.cod_fecha THEN fv.mnt_venta_neta ELSE 0 END), 0) AS mnt_venta_neta,
  IFNULL(SUM(CASE WHEN fv.cod_fecha = cb.cod_fecha THEN fv.mnt_costo_venta ELSE 0 END), 0) AS mnt_costo_venta
FROM calendar_base cb
LEFT JOIN `bold-momentum-270218.bo_data.fact_venta_stock_sku_tabla` fv
  ON fv.cod_producto = cb.cod_producto
 AND fv.cod_talla = cb.cod_talla
 AND fv.cod_sucursal = cb.cod_sucursal
 AND fv.cod_fecha = cb.cod_fecha
GROUP BY
  cb.cod_sucursal,
  cb.cod_producto,
  cb.cod_talla,
  cb.cod_ano_comercial,
  cb.cod_semana
ORDER BY
  cb.cod_sucursal,
  cb.cod_producto,
  cb.cod_talla,
  cb.cod_ano_comercial,
  cb.cod_semana;