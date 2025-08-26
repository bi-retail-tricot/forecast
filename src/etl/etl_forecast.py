import polars as pl
import logging

from src.utils.setup_logging import setup_logging

setup_logging()

# ---------- helpers ----------
def _safe_div(num: pl.Expr, den: pl.Expr, fill: float = 0.0) -> pl.Expr:
    return (num / pl.when(den == 0).then(None).otherwise(den)).fill_null(fill)

# Claves consistentes con tu pipeline actual
KEY_SKU = ['cod_sucursal','cod_producto','cod_talla']
KEY_CAL = ['cod_ano_comercial','cod_semana']  # calendario comercial (se usa para joins semanales)

def add_time_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    - sin/cos de semana comercial (estacionalidad cíclica)
    - weeks_since_arrival: desde primera semana con stock>0 (fallback: week_number-1)
    - week_idx_season: ranking de cod_semana dentro de (temporada, año)
    - early_lifecycle: semanas 0..4 desde llegada
    """
    # sin/cos usando cod_semana (1..52 aprox)
    df = df.with_columns([
        (2 * pl.lit(3.141592653589793) * pl.col('cod_semana') / 52).alias('_theta')
    ]).with_columns([
        pl.col('_theta').sin().cast(pl.Float32).alias('sin_week'),
        pl.col('_theta').cos().cast(pl.Float32).alias('cos_week')
    ]).drop('_theta')

    # primera semana con stock > 0 por SKU-sucursal
    first_in_stock = (
        df
        .with_columns([
            pl.when(pl.col('stock_start_week') > 0)
              .then(pl.col('week_number'))
              .otherwise(None)
              .alias('_week_if_stock')
        ])
        .group_by(KEY_SKU)
        .agg(pl.col('_week_if_stock').min().alias('_first_week_in_stock'))
    )

    df = df.join(first_in_stock, on=KEY_SKU, how='left')

    df = df.with_columns([
        (pl.col('week_number') - pl.col('_first_week_in_stock')).alias('weeks_since_arrival')
    ]).with_columns([
        # fallback si nunca hubo stock en la serie
        pl.when(pl.col('weeks_since_arrival').is_null())
          .then(pl.col('week_number') - 1)
          .otherwise(pl.col('weeks_since_arrival'))
          .cast(pl.Int16)
          .alias('weeks_since_arrival')
    ]).drop('_first_week_in_stock')

    # week_idx_season = ranking de cod_semana dentro de (temporada, año)
    df = (
        df.sort(['cod_temporada','ano_temporada','cod_semana'])
          .with_columns([
              pl.int_range(pl.len())
              .over(['cod_temporada','ano_temporada'])
              .add(1)
              .cast(pl.UInt8)
              .alias('week_idx_season')
          ])
    )

    # early_lifecycle
    df = df.with_columns([
        (pl.col('weeks_since_arrival') <= 4).cast(pl.UInt8).alias('early_lifecycle')
    ])

    return df

def add_price_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    - discount_pct, price_change_d1, is_promo
    - price_rel_sku_season: normaliza precio por mediana del SKU en (temporada,año)
    """
    # descuento y flag promo
    df = df.with_columns([
        _safe_div(
            pl.col('mnt_precio_base') - pl.col('mnt_precio_vigente'),
            pl.col('mnt_precio_base'),
            fill=0.0
        ).clip(0, 1).cast(pl.Float32).alias('discount_pct'),
        (pl.col('mnt_precio_vigente') - pl.col('mnt_precio_vigente').shift(1)
            .over(KEY_SKU)).cast(pl.Int32).alias('price_change_d1'),
    ])

    df = df.with_columns([
        ((pl.col('flag_sale') == 1) | (pl.col('discount_pct') > 0)).cast(pl.UInt8).alias('is_promo')
    ])

    # precio relativo al nivel típico del SKU en la temporada (mediana)
    sku_season_price = (
        df.group_by(['cod_sku','cod_temporada','ano_temporada'])
          .agg(pl.col('mnt_precio_vigente').median().alias('_med_price_sku_season'))
    )
    df = df.join(sku_season_price, on=['cod_sku','cod_temporada','ano_temporada'], how='left')
    df = df.with_columns([
        _safe_div(pl.col('mnt_precio_vigente').cast(pl.Float32),
                  pl.col('_med_price_sku_season').cast(pl.Float32),
                  fill=1.0).alias('price_rel_sku_season')
    ]).drop('_med_price_sku_season')

    return df

def add_stock_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    - max_sellable, oos_end, stock_delta, repo_intensity, on_shelf_ratio
    """
    df = df.with_columns([
        (pl.col('stock_start_week') + pl.col('reposition')).cast(pl.Int32).alias('max_sellable'),
        (pl.col('stock_end_week') == 0).cast(pl.UInt8).alias('oos_end'),
        (pl.col('stock_end_week') - pl.col('stock_start_week')).cast(pl.Int16).alias('stock_delta'),
        _safe_div(pl.col('reposition'), (pl.col('stock_end_week') + 1)).cast(pl.Float32).alias('repo_intensity'),
    ])

    # on_shelf_ratio: si weekly_available_stock son unidades máximas vendibles semanales => ratio a max_sellable
    # si en tu dato weekly_available_stock son días con stock, reemplaza por /7.0
    df = df.with_columns([
        _safe_div(pl.col('weekly_available_stock').cast(pl.Float32),
                  (pl.col('max_sellable') + 1).cast(pl.Float32)).clip(0, 1).alias('on_shelf_ratio')
    ])

    return df

def add_history_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    - lags de ventas 1..4
    - roll_mean_4, roll_std_4
    - roll_zero_rate_4, roll_promo_rate_4, roll_repo_rate_4
    - roll_price_mean_4
    Todas calculadas por grupo KEY_SKU y orden temporal.
    """
    df = df.sort(KEY_SKU + ['cod_ano_comercial','cod_semana','week_number'])

    # Lags
    for k in [1,2,3,4]:
        df = df.with_columns([
            pl.col('weekly_sales').shift(k).over(KEY_SKU).fill_null(0).cast(pl.Int16).alias(f'lag_{k}')
        ])

    # Rolling sobre ventas
    df = df.with_columns([
        pl.col('weekly_sales').shift(1).over(KEY_SKU).rolling_mean(window_size=4, min_periods=1)
            .fill_null(0).round(3).alias('roll_mean_4'),
        pl.col('weekly_sales').shift(1).over(KEY_SKU).rolling_std(window_size=4, min_periods=2)
            .fill_null(0).round(3).alias('roll_std_4'),
        (pl.col('weekly_sales')==0).cast(pl.Int8).shift(1).over(KEY_SKU).rolling_mean(window_size=4, min_periods=1)
            .fill_null(0).round(3).alias('roll_zero_rate_4'),
        pl.col('is_promo').shift(1).over(KEY_SKU).rolling_mean(window_size=4, min_periods=1)
            .fill_null(0).round(3).alias('roll_promo_rate_4'),
        pl.col('flag_repo').shift(1).over(KEY_SKU).rolling_mean(window_size=4, min_periods=1)
            .fill_null(0).round(3).alias('roll_repo_rate_4'),
        pl.col('mnt_precio_vigente').cast(pl.Float32).shift(1).over(KEY_SKU).rolling_mean(window_size=4, min_periods=1)
            .fill_null(pl.col('mnt_precio_vigente').cast(pl.Float32)).round(2).alias('roll_price_mean_4'),
    ])

    return df

def add_peer_aggregates(df: pl.DataFrame) -> pl.DataFrame:
    """
    - peer_mean_depto_4wk: promedio depto-sucursal semanal con rolling 4
    - peer_mean_marca_4wk: promedio marca-sucursal semanal con rolling 4
    - peer_oos_rate_depto_4wk: tasa quiebre depto-sucursal semanal rolling 4
    """
    # Base semanal por grupo
    weekly_depto = (
        df.group_by(['cod_sucursal','cod_depto'] + KEY_CAL)
          .agg([
              pl.col('weekly_sales').mean().alias('peer_mean_depto_week'),
              pl.col('oos_end').mean().alias('peer_oos_rate_depto_week')
          ])
          .sort(['cod_sucursal','cod_depto','cod_ano_comercial','cod_semana'])
    )

    weekly_marca = (
        df.group_by(['cod_sucursal','cod_marca'] + KEY_CAL)
          .agg(pl.col('weekly_sales').mean().alias('peer_mean_marca_week'))
          .sort(['cod_sucursal','cod_marca','cod_ano_comercial','cod_semana'])
    )

    # Rolling 4 semanas (por grupo, usando index entero: week_number dentro de cada grupo SKU no sirve;
    # aquí usamos (cod_ano_comercial, cod_semana) como clave y simple rolling por filas consecutivas)
    weekly_depto = weekly_depto.with_columns([
        pl.col('peer_mean_depto_week').shift(1).rolling_mean(window_size=4, min_periods=1)
            .round(3).alias('peer_mean_depto_4wk'),
        pl.col('peer_oos_rate_depto_week').shift(1).rolling_mean(window_size=4, min_periods=1)
            .round(3).alias('peer_oos_rate_depto_4wk'),
    ])

    weekly_marca = weekly_marca.with_columns([
        pl.col('peer_mean_marca_week').shift(1).rolling_mean(window_size=4, min_periods=1)
            .round(3).alias('peer_mean_marca_4wk'),
    ])

    # Join back por semana
    df = df.join(
        weekly_depto.select(['cod_sucursal','cod_depto'] + KEY_CAL + ['peer_mean_depto_4wk','peer_oos_rate_depto_4wk']),
        on=['cod_sucursal','cod_depto'] + KEY_CAL,
        how='left'
    ).join(
        weekly_marca.select(['cod_sucursal','cod_marca'] + KEY_CAL + ['peer_mean_marca_4wk']),
        on=['cod_sucursal','cod_marca'] + KEY_CAL,
        how='left'
    )

    # Relleno básico
    df = df.with_columns([
        pl.col('peer_mean_depto_4wk').fill_null(0),
        pl.col('peer_mean_marca_4wk').fill_null(0),
        pl.col('peer_oos_rate_depto_4wk').fill_null(0),
    ])

    return df

def add_train_flags_and_weights(df: pl.DataFrame) -> pl.DataFrame:
    """
    - available_clean: disponible y sin censura por quiebre
    - sample_weight: 1*on_shelf_ratio si available_clean else 0 (robusto y simple)
    """
    df = df.with_columns([
        ((pl.col('flag_inventory_available') == 1) & (pl.col('flag_stockout') == 0))
            .cast(pl.UInt8).alias('available_clean')
    ])

    df = df.with_columns([
        (pl.col('available_clean').cast(pl.Float32) * pl.col('on_shelf_ratio').cast(pl.Float32))
            .alias('sample_weight')
    ])

    return df

def process_data(df: pl.DataFrame) -> pl.DataFrame:
    logging.info("Processing data...")

    # nuevos pasos (en este orden)
    df = add_time_features(df)
    df = add_price_features(df)
    df = add_stock_features(df)
    df = add_history_features(df)
    df = add_peer_aggregates(df)
    df = add_train_flags_and_weights(df)

    logging.info("Data processing completed successfully.")
    return df
