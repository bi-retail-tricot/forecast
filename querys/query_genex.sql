SELECT
    g.cod_sucursal,
    g.cod_producto,
    t.cod_talla,
    tfv.cod_ano_comercial,
    tfv.cod_semana,
    g.vta_periodo,
    g.vta_promedio,
    g.semana_vta,
    g.ume,
    g.factor,
    g.stock_on_hand,
    g.stock_sucursal,
    g.stock_bodega,
    g.repo_x_ume,
    g.repo_x_dda,
    g.can_original,
    g.can_final,
    g.estado,
    g.clasif,
FROM `data-ingest-ops-463416.reposicion.genex` AS g
LEFT JOIN `bold-momentum-270218.bo_data.maestro_talla` AS t
    ON g.nom_talla = t.nom_talla
LEFT JOIN `bold-momentum-270218.bo_data.maestro_producto` AS p
    ON g.cod_producto = p.cod_producto
LEFT JOIN `bold-momentum-270218.bo_data.maestro_sucursal` AS s
    ON g.cod_sucursal = s.cod_sucursal
LEFT JOIN `bold-momentum-270218.bo_data.tabla_fechas_view` AS tfv
  ON PARSE_DATE('%d-%m-%Y', g.cod_fecha) = tfv.fecha
-- WHERE g.cod_sucursal = 180 AND t.cod_talla = 103 AND g.cod_producto = 640924
ORDER BY
    g.cod_sucursal,
    g.cod_producto,
    t.cod_talla,
    tfv.cod_ano_comercial,
    tfv.cod_semana