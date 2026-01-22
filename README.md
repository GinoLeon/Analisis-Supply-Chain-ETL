# Analisis-Supply-Chain-ETL

## Descripción
Pipeline ETL desarrollado en Python para procesar datos de una cadena de suministro,
aplicando limpieza, validación, modelado dimensional y carga en SQL Server.

## Tecnologías
- Python (pandas, SQLAlchemy)
- SQL Server
- Modelo dimensional (Star Schema)
- Git & GitHub

## 🔄 Proceso ETL
1. Extracción desde CSV
2. Limpieza y normalización de datos
3. Validación de duplicados y datos inconsistentes
4. Creación de dimensiones:
   - DimMaterial
   - DimVendor
   - DimVendorLocation
   - DimLogistics
5. Carga de hechos en FactSupplyChain

## Modelo de Datos
- FactSupplyChain
- DimMaterial
- DimVendor
- DimVendorLocation
- DimLogistics