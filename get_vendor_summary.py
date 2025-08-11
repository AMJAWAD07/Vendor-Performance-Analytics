import sqlite3
import pandas as pd
import time
import logging

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

conn = sqlite3.connect('inventory.db')

def table_exists(table_name, conn):
    '''Check if table exists in SQLite using raw connection'''
    query = f"""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='{table_name}';
    """
    cursor = conn.execute(query)
    result = cursor.fetchone()
    return result is not None

def truncate_table(table_name, conn):
    '''Delete all rows from a table'''
    query = f'DELETE FROM "{table_name}";'
    conn.execute(query)
    conn.commit()

def ingest_db(clean_df, table_name, conn):
    if not table_exists(table_name, conn):
        logging.info(f"Table '{table_name}' does not exist, creating it and ingesting data...")
        print(f"Table '{table_name}' does not exist, creating it and ingesting data...")
        clean_df.to_sql(table_name, con=conn, if_exists='append', index=False)
        logging.info(f"Data ingested into newly created table '{table_name}'.")
        
        return

    logging.info(f'Truncating existing table "{table_name}" before ingestion.')
    print(f'Truncating existing table "{table_name}" before ingestion.')
    truncate_table(table_name, conn)
    logging.info("Ingesting data")
    print("Ingesting data")
    clean_df.to_sql(table_name, con=conn, if_exists='append', index=False)
    logging.info(f"Data appended into existing table '{table_name}'.")
    print(f"Data appended into existing table '{table_name}'.")

def create_vendor_summary(conn) :
    '''this function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary AS (
            SELECT
                VendorNo,
                Brand,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(SalesDollars) AS TotalSalesDollars,
                SUM(SalesPrice) AS TotalSalesPrice,
                SUM(ExciseTax) AS TotalExciseTax
            FROM sales
            GROUP BY VendorNo, Brand
    )
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC""", conn)
    return vendor_sales_summary

def clean_data(df):
    '''this function will clean the data'''
    # changing datatype to float
    df['Volume'] = df['Volume'].astype('float')
    
    # filling missing value with 0
    df.fillna(0,inplace = True)
    
    # removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    # creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin']= (df['GrossProfit'] / df ['TotalSalesDollars'])*100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] /df ['TotalPurchaseDollars']
                                            
    return df

if __name__== '__main__':
    # creating database connection
    start = time.time()
    print('Creating Vendor Summary Table ..... ')
    logging.info('Creating Vendor Summary Table ..... ')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())
    print('Cleaning Data ..... ')
    logging.info('Cleaning Data ..... ')
    clean_df = clean_data(summary_df)
    logging.info("cleaned data")
    print("cleaned data")          
    logging.info(clean_df.head())
    logging.info('Ingesting data ..... ')
    print('Ingesting data ..... ')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    print("Ingested Data")
    logging.info("Ingested Data")
    end = time.time()
    total_time = (end - start)/60
    logging.info('----------Ingestion Complete----------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')
    print('----------Completed----------')
    print(f'Total Time Taken: {total_time:.2f} minutes')