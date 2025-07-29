## MÉTRICAS TEMPORALES

| Campo | Detalle | Interpretación |
|-------|---------|----------------|
| **Semanas en temporada** | Maxima cantidad de semanas que el producto puede estar presente en la temporada |Cantidad la cantidad de semanas desde que el producto tiene inventario hasta la ultima semana de la temporada, se puede medir a que altura entró el producto en sucursales|
| **Semanas con inventario disponible** | Semanas con stock > 0 | Cantidad de semanas que el producto estuvo disponible para venta |
| **Semanas con venta** | Semanas con venta > 0 | Promedio de semanas que vendió |
| **Semanas con quiebre de stock** | Semanas con venta > 0 y stock al final de semana = 0 | Cantidad de quiebres de stock que hubo |

---

## MÉTRICAS DE DEMANDA

| Campo | Fórmula | Interpretación |
|-------|---------|----------------|
| **ADI** | $ADI = \frac{\text{Semanas con inventario}}{\text{Semanas con ventas}}$ | Intervalo entre ventas (semanas) |
| **Venta promedio semanal** | $\frac{\sum \text{ventas}}{\text{semanas con venta}}$ | Venta media cuando hay venta
| **Venta prom semanal (Croston)** | $\frac{\text{Venta media cuando hubo venta}}{\text{Intervalo entre ventas}}$ | Venta promedio semanal ajustada por método Croston* |
| **Inventario promedio semanal** | $\frac{\sum \text{inventario}}{\text{semanas con inventario}}$ | Stock disponible medio por semana |
| **Semanas de venta promedio** | $\frac{\text{Inventario promedio semanal}}{\text{Venta prom semanal}}$ | Cobertura de semanas promedio |

---

## NOTAS TÉCNICAS
### Clasificación de demanda basada en Syntetos & Boylan (2005)

| Tipo | ADI | CV² | Características |
|------|-----|-----|-----------------|
| **Suave** | ≤ 1.32 | ≤ 0.49 | Venta regular y constante en cantidad |
| **Intermitente** | > 1.32 | ≤ 0.49 | Venta espaciada pero constante en cantidad|
| **Errática** | ≤ 1.32 | > 0.49 | Venta frecuente pero variable en cantidad|
| **Irregular** | > 1.32 | > 0.49 | Impredecible en tiempo y cantidad |

**Donde:**
- $ADI = \frac{\text{Semanas con inventario}}{\text{Semanas con ventas}}$ (frecuencia de demanda)
- $CV^2 = \left(\frac{\sigma}{\mu}\right)^2$ (variabilidad de la demanda)



## Método Croston (1972)
Ajusta la venta promedio considerando períodos sin venta, especialmente útil para demanda intermitente. Desarrollado por J.D. Croston para mejorar pronósticos en series con muchos ceros.

${Croston} = \frac{\text{Venta media cuando hay venta}}{\text{Intervalo entre ventas}}$