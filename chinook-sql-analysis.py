from sqlalchemy import create_engine , inspect 
import pandas as pd


db_path = 'E:\\engenrring Hema\\Data Engineeing\\projects\\chinook analysis\\data source\\Chinook_Sqlite.sqlite'
engine = create_engine(f'sqlite:///{db_path}')

inspector = inspect(engine)

table_names = inspector.get_table_names()
print("Table Names : " , table_names)

for table in table_names : 
    print("Table Name : " , table , "===")

    columns = inspector.get_columns(table)

    col_names = [col['name'] for col in columns]
    print("Column Name : " , col_names)

    query = f'SELECT * FROM {table} LIMIT 5'
    df = pd.read_sql(query , engine)
    print(df)

## Top 5 Customers by Total Spending by year

query = '''
    WITH customer_spening 
    AS (
    SELECT  FirstName || ' ' ||LastName AS "Full Name",
            Country,
            strftime('%Y' , InvoiceDate) AS Year ,
            SUM(Total) AS "Total Spending" 
    FROM 'Invoice' AS  s
    INNER JOIN Customer AS c 
    ON s.CustomerId = C.CustomerId
    GROUP BY strftime('%Y' , InvoiceDate) , C.CustomerId
    )

    SELECT * 
    FROM (
    SELECT  * , 
            ROW_NUMBER() OVER(PARTITION BY Year ORDER BY "Total Spending" DESC) AS RN
    FROM customer_spening ) as partitioning_customers
    WHERE RN <= 5
'''

total_spending = pd.read_sql(query,engine)
print("First Questoin  ========")
print(total_spending)

## second question 

query2 = '''
    WITH genre_usage
    AS (
        SELECT  s.BillingCountry AS Country , 
            g.Name AS "Genre Name" , 
            SUM(Total) AS "Total Spending"
        FROM Invoice AS s
        INNER JOIN InvoiceLine sl
        ON s.InvoiceId = sl.InvoiceId
        INNER JOIN Track t
        ON sl.TrackId = t.TrackId
        INNER JOIN Genre g 
        ON t.GenreId = g.GenreId
        GROUP BY s.BillingCountry , g.Name 
    )
    SELECT  Country , 
            "Genre Name" , 
            "Total Spending" FROM 
    (
    SELECT  Country , 
            "Genre Name" , 
            "Total Spending", 
            ROW_NUMBER() OVER(PARTITION BY Country ORDER BY "Total Spending" DESC) AS RN
    FROM genre_usage
    ) AS ordered_genere_spending
    WHERE RN = 1
'''

mostGenre = pd.read_sql(query2 , engine)
print("Second Questoin  ========")
print(mostGenre.head(30))

## third questin 

query3 = '''
    SELECT  art.Name AS "Artist Name" , 
            SUM(sl.UnitPrice * Quantity) AS "Total Revenue" 
    FROM InvoiceLine sl
    INNER JOIN Track t 
    ON sl.TrackId = t.TrackId
    INNER JOIN Album a
    ON t.AlbumId = a.AlbumId
    INNER JOIN Artist art
    ON art.ArtistId = a.ArtistId 
    GROUP BY art.Name 
    ORDER BY "Total Revenue" DESC
    '''

print("Third Question ========")
artist_revenue = pd.read_sql(query3,engine)
print(artist_revenue.head(5))


### customers of top revenue artists
top_artist_revenue = artist_revenue.head(5)['Artist Name'].tolist()
artist_names_tuple = tuple(top_artist_revenue)
customer_artist_revenue = f'''
    WITH top_customer_spending_for_top_artist_revenue
    AS (
        SELECT  FirstName || " " || LastName AS fullName , 
            art.Name AS "Artist Name" ,
            SUM(Total) AS "Total Spending"
        FROM Customer c 
        INNER JOIN Invoice s 
        ON c.CustomerId = s.CustomerId 
        INNER JOIN InvoiceLine sl 
        ON s.InvoiceId = sl.InvoiceId
        INNER JOIN Track t
        ON sl.TrackId = t.TrackId
        INNER JOIN Album a 
        ON t.AlbumId = a.AlbumId
        INNER JOIN Artist art
        ON a.ArtistId = art.ArtistId
        WHERE art.Name IN {artist_names_tuple}
        GROUP BY c.CustomerId
    )
    SELECT fullName ,
            "Total Spending" , 
            "Artist Name"
    FROM (
        SELECT fullName ,
                "Total Spending" , 
                "Artist Name" ,
                ROW_NUMBER() OVER(PARTITION BY "Artist Name" ORDER BY "Total Spending" DESC) AS RN
        FROM top_customer_spending_for_top_artist_revenue ) AS newTable
    WHERE RN <= 3
    '''
customer_most_artistRevenue = pd.read_sql(customer_artist_revenue,engine)
print(customer_most_artistRevenue)

# 4 Customer Lifetime Value by Cohort

segment_cohort_firstPurchase = '''
WITH first_date_purchaseCustomer AS (
    SELECT CustomerId,
            MIN(InvoiceDate) AS first_date_purchase
        FROM Invoice
        GROUP BY CustomerId
    ),
    cohort_invoices AS (
        SELECT 
            strftime('%Y', fcus.first_date_purchase) AS cohort_year,
            strftime('%Y', s.InvoiceDate) AS invoice_year,
            s.CustomerId
        FROM Invoice s
        INNER JOIN first_date_purchaseCustomer fcus
            ON s.CustomerId = fcus.CustomerId
        GROUP BY cohort_year, invoice_year, s.CustomerId
    )
    SELECT 
        cohort_year,
        COUNT(DISTINCT CASE WHEN invoice_year = '2009' THEN CustomerId END) AS y2009,
        COUNT(DISTINCT CASE WHEN invoice_year = '2010' THEN CustomerId END) AS y2010,
        COUNT(DISTINCT CASE WHEN invoice_year = '2011' THEN CustomerId END) AS y2011,
        COUNT(DISTINCT CASE WHEN invoice_year = '2012' THEN CustomerId END) AS y2012,
        COUNT(DISTINCT CASE WHEN invoice_year = '2013' THEN CustomerId END) AS y2013
    FROM cohort_invoices
    GROUP BY cohort_year
    ORDER BY cohort_year;


    '''

analyse_cohort = pd.read_sql(segment_cohort_firstPurchase , engine)
print("Fifth question ===========")
print(analyse_cohort)

## Average Time Between Customer Purchases

query5 = '''
    SELECT CustomerId , AVG(JULIANDAY(Next_Order_date) - JULIANDAY(InvoiceDate)) AS GAB_In_Days
    FROM (
        SELECT CustomerId ,
                InvoiceDate , 
                LEAD(InvoiceDate , 1 , NULL)  OVER(PARTITION BY CustomerId ORDER BY CustomerId , InvoiceDate) AS Next_Order_date
        FROM Invoice s )
    GROUP BY CustomerId
    '''

gab_between_orders = pd.read_sql(query5,engine)
print(gab_between_orders)


### SIX QUESTION 
query6 = '''
    SELECT mt.Name , strftime('%Y' , InvoiceDate) AS Year , SUM(sl.UnitPrice * sl.Quantity) AS "Total Revenue"
    FROM InvoiceLine sl
    INNER JOIN Track t 
    ON sl.TrackId = t.TrackId
    INNER JOIN MediaType mt
    ON mt.MediaTypeId = t.MediaTypeId
    INNER JOIN Invoice s
    ON s.InvoiceId = sl.InvoiceId
    GROUP BY mt.Name , strftime('%Y' , InvoiceDate)
    '''

most_mediatype_revenue = pd.read_sql(query6 , engine)
print("Six Question =========")
print(most_mediatype_revenue)

### seven question : Playlist / Track Popularity Correlation

query7 = '''
    SELECT *
    FROM (
        SELECT pl.Name AS "Playlist Name", COUNT(DISTINCT sl.TrackId) AS "Number of sold tracks" , ROUND((CAST(COUNT(DISTINCT sl.TrackId) AS float) / COUNT(sl.TrackId)) * 100 , 1) AS "Percentage sold tracks" , SUM(UnitPrice * Quantity) AS "Total Revenue"
        FROM Playlist pl
        INNER JOIN PlaylistTrack  plt
        ON pl.PlaylistId = plt.PlaylistId
        INNER JOIN InvoiceLine sl
        ON plt.TrackId = sl.TrackId
        GROUP BY pl.PlaylistId
        )
    GROUP BY "Playlist Name" 
    ORDER BY "Total Revenue" DESC
'''

analysis_playlist = pd.read_sql(query7 , engine)
print(analysis_playlist)

