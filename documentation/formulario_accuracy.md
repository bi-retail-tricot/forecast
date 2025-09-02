# Métricas de error

## WAPE (Weighted Absolute Percentage Error)

$$WAPE = \frac{\sum_{i=1}^{n} |Predicción_i - Valor\ Real_i|}{\sum_{i=1}^{n} Ventas_i}$$

**Qué mide:** Qué tan desviadas están las predicciones en relación al total de ventas.

## BIAS

$$BIAS = \frac{\sum_{i=1}^{n} (Predicción_i - Valor\ Real_i)}{\sum_{i=1}^{n} Ventas_i}$$

**Qué mide:** Si el modelo tiende a predecir de más (valor positivo) o de menos (valor negativo) consistentemente.

## MAE (Mean Absolute Error)

$$MAE = \frac{1}{n} \sum_{i=1}^{n} |Predicción_i - Valor\ Real_i|$$

**Qué mide:** El promedio de error absoluto en unidades, sin importar si fue por exceso o falta de unidades.

## RMSE (Root Mean Square Error)

$$RMSE = \sqrt{\frac{1}{n} \sum_{i=1}^{n} (Predicción_i - Valor\ Real_i)^2}$$

**Qué mide:** Similar al MAE pero penaliza más fuertemente los errores grandes. Útil para detectar outliers en las predicciones.