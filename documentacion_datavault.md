
# 📘 Documentación de Arquitectura Data Vault - Modelo Medallium

## 🔢 Versión
**2**

---

## 📂 Fuente de Datos Original (`src`)
- **Base de Datos:** `SNOWFLAKE_SAMPLE_DATA`
- **Esquema:** `TPCH_SF1`
- **Descripción:** Tablas fuente originales provenientes del conjunto de datos de muestra de Snowflake.

**Tablas:**
| Nombre      | Descripción                                 |
|-------------|---------------------------------------------|
| CUSTOMER    | Tabla original de CLIENTES                  |
| LINEITEM    | Tabla original de LINEAS DE PEDIDOS         |
| NATION      | Tabla original de PAISES                    |
| ORDERS      | Tabla original de PEDIDOS                   |
| PART        | Tabla original de PARTES                    |
| PARTSUPP    | Tabla original de PARTES DE PROVEEDORES     |
| REGION      | Tabla original de REGIONES                  |
| SUPPLIER    | Tabla original de PROVEEDORES               |

---

## 🪵 Capa STAGING (`stg`)
- **Base de Datos:** `SDGVAULTMART`
- **Esquema:** `DBT_SDGVAULT`
- **Propósito:** Corresponde con la capa **LANDING** del modelo físico Medallium. Contiene datos brutos ingestados.

**Tablas:**
- STG_CLIENTES  
- STG_PAISES  
- STG_PARTES  
- STG_PROVEEDORES  
- STG_REGIONES  
- STG_LINEAS_PEDIDO  
- STG_PEDIDOS  
- STG_PARTES_PROVEEDOR  

---

## 🧱 Capa RAW DATA VAULT (`raw`)
- **Base de Datos:** `SDGVAULTMART`
- **Esquema:** `DBT_SDGVAULT`
- **Propósito:** Capa **BRONZE**, para estandarización, semántica, calidad técnica y modelo **Raw Data Vault**.

### Hubs:
- HUB_CLIENTES  
- HUB_PAISES  
- HUB_PARTES  
- HUB_PEDIDOS  
- HUB_PROVEEDORES  
- HUB_REGIONES  
- HUB_LINEAS_PEDIDOS  

### Links:
- LNK_CLIENTES_PEDIDOS  
- LNK_PAISES_CLIENTES  
- LNK_PAISES_PROVEEDORES  
- LNK_PARTES_LINEAS_PEDIDOS  
- LNK_PEDIDOS_LINEAS_PEDIDOS  
- LNK_PROVEEDORES_LINEAS_PEDIDOS  
- LNK_PROVEEDORES_PARTES  
- LNK_REGIONES_PAISES  

### Satellites:
- SAT_CLIENTES_CONTACTO  
- SAT_CLIENTES_CUENTA  
- SAT_CLIENTES  
- SAT_LINEAS_PEDIDOS  
- SAT_PAISES  
- SAT_PARTES  
- SAT_PARTES_PROVEEDORES  
- SAT_PEDIDOS  
- SAT_PROVEEDORES  
- SAT_REGIONES  

---

## 🧠 Capa BUSINESS DATA VAULT (`business`)
- **Base de Datos:** `SDGVAULTMART`
- **Esquema:** `DBT_SDGVAULT`
- **Propósito:** También parte de la capa **BRONZE**, esta sección alberga estructuras **Business Vault** para eficiencia histórica.

**Tablas:**
| Nombre         | Descripción                                                                 |
|----------------|-----------------------------------------------------------------------------|
| PIT_CLIENTES   | Tabla PIT para obtención eficiente del histórico de satélites de Cliente   |
| BRIDGE_PEDIDOS | Tabla BRIDGE para obtener histórico de tablas de enlace de Pedidos         |

---

## 🌟 Capa MART (`mart`)
- **Base de Datos:** `SDGVAULTMART`
- **Esquema:** `DBT_SDGVAULT`
- **Propósito:** Corresponde a la capa **GOLDEN**, con modelos en estrella listos para análisis de negocio.

**Tablas:**
| Nombre        | Descripción                                     |
|---------------|-------------------------------------------------|
| DIM1_PAISES   | Tabla de dimensión Tipo 1 de PAISES             |
| DIM2_CLIENTES | Tabla original de LINEAS DE PEDIDOS *(¿Error de descripción?)* |

---

## 📌 Observaciones
- Podría haber una descripción incorrecta en `DIM2_CLIENTES` (¿es de CLIENTES o de LINEAS DE PEDIDOS?).
- Las capas están bien segmentadas en función del arquetipo **Medallium**.
- Se recomienda añadir descripciones a las tablas faltantes en `stg`, `raw`, etc., para mejorar la trazabilidad.
