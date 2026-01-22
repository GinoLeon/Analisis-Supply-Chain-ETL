import pandas as pd
from sqlalchemy import create_engine,text
import urllib


# 1. CONFIGURACIÓN DE CONEXIÓN
params = urllib.parse.quote_plus(
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=AQUI VA EL NOMBRE DE TU SERVIDOR ;"
    r"DATABASE=Proyecto3-2026;"
    r"Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def ejecutar_etl():
    print("Iniciando Pipeline ETL...")


    path = r"C:\Users\User\Desktop\Proyecto Github Gino\Analisis-Supply-Chain-ETL\data\supply_chain_data.csv"
    df = pd.read_csv(path, encoding="ISO-8859-1")
    df.info()

    df.columns = [c.strip().replace(" ", "_") for c in df.columns]

    numeric_cols = [
        "Manufacturing_costs",
        "Price",
        "Defect_rates",
        "Revenue_generated",
        "Shipping_costs",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col].astype(str).str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")


    int_cols = [
        "Stock_levels",
        "Order_quantities",
        "Number_of_products_sold",
        "Lead_times",
    ]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Reorder Point (ROP)
    df["Reorder_Point"] = (df["Number_of_products_sold"] / 30) * df["Lead_times"]



    # 1️⃣ DimMaterial
    # Detectar SKUs duplicados en el CSV
    duplicated_sku = df[df.duplicated(subset=["SKU"], keep=False)]

    conflicting_sku = (
    duplicated_sku
    .groupby("SKU")
    .filter(lambda x: x.nunique().gt(1).any())
    )

    if not conflicting_sku.empty:
        conflicting_sku.to_csv(
            r"C:\Users\User\Desktop\Proyecto Github Gino\Analisis-Supply-Chain-ETL\rejects\reject_dim_material.csv",
            index=False
    )

    df_valid = df[~df["SKU"].isin(conflicting_sku["SKU"])]


    dim_material = df_valid[
        ["SKU", "Product_type", "Manufacturing_costs", "Price", "Defect_rates"]
    ].drop_duplicates(subset=["SKU"])
    dim_material.columns = [
        "SKU",
        "Product_type",
        "Manufacturing_costs",
        "Price",
        "Defect_rate_percentage"
    ]
    existing_sku = pd.read_sql(
        "SELECT SKU FROM DimMaterial",
        engine
    )

    dim_material = dim_material[
        ~dim_material["SKU"].isin(existing_sku["SKU"])
    ]

    if not dim_material.empty:
        dim_material.to_sql(
            "DimMaterial",
            engine,
            if_exists="append",
            index=False
        )

    # 2️⃣ DimVendor
    dim_vendor = (
        df[["Supplier_name"]]
        .drop_duplicates()
    )

    dim_vendor.columns = ["Supplier_Name"]

    existing_vendor = pd.read_sql(
        "SELECT Supplier_Name FROM DimVendor",
        engine
    )

    dim_vendor = dim_vendor[
        ~dim_vendor["Supplier_Name"].isin(existing_vendor["Supplier_Name"])
    ]

    if not dim_vendor.empty:
        dim_vendor.to_sql(
            "DimVendor",
            engine,
            if_exists="append",
            index=False
        )

   # 2️⃣ DimVendor-Location
    vendor_location = (
        df_valid
        .groupby(["Supplier_name", "Location"], as_index=False)
        .agg({
            "Lead_time": "mean"
        })
    )

    vendor_location.columns = [
        "Supplier_Name",
        "Location",
        "Average_Lead_Time_Days"
    ]

    vendor_location["Average_Lead_Time_Days"] = (
        vendor_location["Average_Lead_Time_Days"]
        .round()
        .astype(int)
    )
    dim_vendor_db = pd.read_sql(
        "SELECT Vendor_ID, Supplier_Name FROM DimVendor",
        engine
    )

    vendor_location = vendor_location.merge(
        dim_vendor_db,
        on="Supplier_Name",
        how="left"
    )
    existing_vendor_location = pd.read_sql(
        "SELECT Vendor_ID, Location FROM DimVendorLocation",
        engine
    )

    vendor_location = vendor_location.merge(
        existing_vendor_location,
        on=["Vendor_ID", "Location"],
        how="left",
        indicator=True
    )

    vendor_location = vendor_location[
        vendor_location["_merge"] == "left_only"
    ].drop(columns="_merge")

    if not vendor_location.empty:
        vendor_location[[
        "Vendor_ID",
        "Location",
        "Average_Lead_Time_Days"
    ]].to_sql(
        "DimVendorLocation",
        engine,
        if_exists="append",
        index=False
    )

    # 3️⃣ DimLogistics
    dim_logistics = df[
        ["Shipping_carriers", "Transportation_modes", "Routes"]
    ].drop_duplicates()

    dim_logistics.columns = [
        "Shipping_carrier",
        "Transportation_mode",
        "Route"
    ]

    existing_logistics = pd.read_sql(
    "SELECT Shipping_carrier, Transportation_mode, Route FROM DimLogistics",
    engine
    )

    dim_logistics = dim_logistics.merge(
        existing_logistics,
        on=["Shipping_carrier", "Transportation_mode", "Route"],
        how="left",
        indicator=True
    )

    dim_logistics = dim_logistics[dim_logistics["_merge"] == "left_only"]
    dim_logistics.drop(columns="_merge", inplace=True)

    if not dim_logistics.empty:
        dim_logistics.to_sql(
            "DimLogistics",
            engine,
            if_exists="append",
            index=False
        )


    # 4️⃣ FactSupplyChain
    dim_logistics_db = pd.read_sql(
        "SELECT Shipping_ID, Shipping_carrier, Transportation_mode, Route FROM DimLogistics",
        engine
    )

    fact_data = df_valid.merge(
        dim_logistics_db,
        left_on=["Shipping_carriers", "Transportation_modes", "Routes"],
        right_on=["Shipping_carrier", "Transportation_mode", "Route"],
        how="left"
    )

    dim_vendor_db = pd.read_sql(
    "SELECT Vendor_ID, Supplier_Name FROM DimVendor",
    engine
    )

    fact_data = fact_data.merge(
    dim_vendor_db,
    left_on="Supplier_name",
    right_on="Supplier_Name",
    how="left"
    )

    dim_vendor_location_db = pd.read_sql(
    """
    SELECT 
        Vendor_Location_ID,
        Vendor_ID,
        Location
    FROM DimVendorLocation
    """,
    engine
    )

    fact_data = fact_data.merge(
        dim_vendor_location_db,
        on=["Vendor_ID", "Location"],
        how="left"
    )


    fact_data = fact_data[
        [
            "SKU",
            "Vendor_Location_ID",
            "Shipping_ID",
            "Stock_levels",
            "Order_quantities",
            "Number_of_products_sold",
            "Revenue_generated",
            "Shipping_costs",
            "Lead_times",
            "Reorder_Point",
        ]
    ]

    fact_data.columns = [
        "SKU",
        "Vendor_Location_ID",
        "Shipping_ID",
        "Stock_levels",
        "Order_quantities",
        "Number_of_products_sold",
        "Revenue_generated",
        "Shipping_costs",
        "Actual_Lead_Time_Days",
        "Reorder_Point",
    ]

    fact_data.to_sql(
        "FactSupplyChain",
        engine,
        if_exists="append",
        index=False
    )

    print("ETL completado correctamente. Datos cargados en SQL Server.")

if __name__ == "__main__":

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE FactSupplyChain"))

    ejecutar_etl()

