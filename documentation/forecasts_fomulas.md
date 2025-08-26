### Definiciones

* $Venta_{prom}$: venta promedio (`vta_promedio`)
* $Factor$: factor (`factor`)
* $Factor_{hist}$: factor histórico (`factor_historico`)
* $SV$: semanas de venta (`semana_vta`)

---

### Ecuaciones de cada forecast

1. **Forecast actual**

$$
Venta_{prom} \times Factor \times SV
$$

2. **Forecast con factor histórico**

$$
Venta_{prom} \times Factor_{hist} \times SV
$$

3. **Forecast venta promedio límitada a 1**

$$
\min(Venta_{prom}, 1) \times Factor \times SV
$$

4. **Forecast semanas venta fijo en 4**

$$
Venta_{prom} \times Factor \times 4
$$

5. **Forecast factor fijo igual a 1**

$$
Venta_{prom} \times 1 \times SV
$$

6. **Forecast aleatorio**

$$
Venta_{aleatorio} \times Factor_{aleatorio} \times 8
$$


* $Factor_{aleatorio}$: factor aleatorio entre 0.5 y 1.
* $Venta_{aleatorio}$: venta promedio aleatoria entre 0.8 y 1.2.