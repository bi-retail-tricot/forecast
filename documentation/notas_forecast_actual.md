#  Hallazgos Forecast Actual Tricot

## 1. Precisión del Forecast

* **WAPE promedio \~1.0 (100%)** → forecast poco confiable para reposición automática.
* Mejora en semanas estables (junio con WAPE 0.65–0.9), pero **se rompe en semanas con eventos o alta estacionalidad** (ej. marzo, abril, mayo).
* **Sesgo positivo (Bias >0)**: tendencia sistemática a **sobreestimar demanda** → riesgo de sobrestock.

---

## 2. Impacto en Productos de Moda (compra única)

* **Sobrepronóstico** → exceso de stock en tiendas, obsolescencia rápida, rebajas y erosión de margen.
* **Subpronóstico** → quiebres de stock en tiendas clave, ventas perdidas a precio full, clientes insatisfechos.
* **Asignación desigual** → algunas tiendas con sobrestock y otras agotadas, generando transferencias costosas.
* **Ventana corta de venta** (6–10 semanas) → errores iniciales tienen impacto irreversible.

---

## 3. Consecuencias Financieras y Operativas

* **Menor margen**: descuentos de hasta -50% en liquidaciones.
* **Pérdida de ventas plenas** por quiebres en sucursales de alta demanda.
* **Capital inmovilizado** en inventario lento.
* **Costos extras** por transferencias intertienda.

---

## 4. Recomendaciones Clave

1. **Mejorar forecast con segmentación**: separar básicos vs moda estacional.
2. **Asignación inicial diferenciada** por cluster de tiendas (no flat).
3. **Test & Roll-out rápido**: lanzar parte del stock y redistribuir tras primeras semanas.
4. **Calendario comercial integrado al forecast** (promos, lanzamientos, feriados).
5. **Monitoreo semanal con alertas**: activar revisión cuando WAPE >0.5.

---

**Conclusión:** El forecast actual entrega tendencia, pero no es suficiente para asignación óptima en moda. Se requiere **complementar el modelo con inputs comerciales y reglas de negocio**, priorizando asignación inteligente y redistribución temprana para maximizar ventas a precio full y proteger margen.

---

### **Slide 1 – Forecast Moda: Hallazgos Clave**

* El forecast actual muestra **WAPE promedio ≈ 1.0 (100% de error)** → no es confiable como input directo de la fórmula de reposición.
* Sesgo positivo → sobreestimación de la demanda.
* Buen ajuste en semanas estables, pero falla en semanas con eventos o quiebres estacionales.

---

### **Slide 2 – Fórmula de Reposición (contexto aplicado)**

* **Repo. por demanda = TRUNCAR\[(Venta promedio × Factor × SV) + 0.6] – Stock actual**
* Con un forecast poco preciso:

  * **Sobrepronóstico** → la fórmula empuja más reposición → sobrestock en tiendas.
  * **Subpronóstico** → la fórmula entrega menos reposición → quiebres de stock en sucursales clave.

---

### **Slide 3 – Impacto en Productos Moda (stock limitado)**

* Compra única → no hay reorden, la asignación inicial es crítica.
* **Sobrepronóstico + factor elevado** → exceso en tiendas pequeñas → liquidaciones forzadas.
* **Subpronóstico + SV alto** → agotamiento temprano en tiendas grandes → ventas perdidas.
* **Resultado**: inequidad de stock entre sucursales, mayores transferencias intertienda.

---

### **Slide 4 – Consecuencias Financieras**

* Margen erosionado por **rebajas hasta -50%** en sobrestock.
* **Pérdida de ventas full price** en tiendas con quiebres.
* **Inventario inmovilizado** que no rota durante la temporada.
* Costos adicionales por transferencias y redistribución.

---

### **Slide 5 – Recomendaciones**

1. **Mejorar calidad del forecast** antes de aplicarlo a la fórmula.
2. Ajustar el **Factor** por categoría (ej. 1.0 en moda estacional, 1.5 en básicos).
3. Definir **SV dinámico**: menor cobertura en moda, mayor en básicos.
4. **Asignación inicial por cluster de tiendas**, no flat.
5. Integrar **inputs comerciales** en la estimación de demanda (promos, lanzamientos, feriados).

---

👉 Con este enfoque, la presentación muestra **cómo la fórmula de reposición amplifica los errores del forecast** y por qué es clave mejorar precisión y ajustar parámetros (factor, SV) según tipo de producto.

---