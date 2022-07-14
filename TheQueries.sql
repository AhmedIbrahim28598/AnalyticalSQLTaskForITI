/* We should convert the column type 'invoicedate' from text to date */
ALTER Table Online_Retail ALTER COLUMN invoicedate SET DATA TYPE DATE
USING TO_TIMESTAMP(invoicedate, 'MM/DD/YYYY HH24:MI:SS');

/*we need to clean the data from some empty values that can affect the results,
so we will do at the beginning, so we will do some operations on a column 'Customerid' */
delete from Online_Retail
where customerid = '';

---------------------------------------------------------------------------------------
-- Question One : The 5 Analytical Queries..

--1--Story: The Manager want to track of the behavior of each customer in each country and all the transactions they made.
SELECT   InvoiceNo,invoicedate,customerid,country, quantity*unitprice as sales,
ROW_NUMBER()  OVER( partition by country ORDER BY quantity*unitprice DESC) 
from Online_Retail
--Description: we Got The NumbersIds Of All Customers due to their Invoiceno to discover the least transactions and Fix it.

-------------------------------

--2--Story: The Customer Satisfaction Manager wants to organize a competition to reward the most buying customers and motivate the least buying ones.
SELECT distinct country , 
first_value(customerid)  OVER( partition by country ORDER BY count(invoiceno) DESC) as Most_Purchased_Customer,
first_value(customerid)  OVER( partition by country ORDER BY count(invoiceno)) as Lowest_purchase_customer
from Online_Retail
group by country,customerid
--Description: We check who is the most buying and least buying client among our clients in all countries.

-------------------------------

--3- /*Story: The marketing manager wants to know the best-selling and least-selling products in each country
   to study organizing a huge advertising campaign covering the problems facing the least-selling
   products in the markets in each country.
   */
select distinct country
,first_value(description)  OVER( partition by country ORDER BY max(quantity*unitprice) DESC) as Most_Purchased_Customer
,first_value(description)  OVER( partition by country ORDER BY max(quantity*unitprice) ) as Lowest_Purchased_Customer
from Online_Retail
group by country,description
--Description: We check which products are most bought and least purchased for each country.

-------------------------------

--4--Story: The manager wants to keep track of the relations between all countries in terms of sales order.
select 
max(country),
lead(country,1)over(order by count(invoiceno)) as next_country,
lag(country,1)over(order by count(invoiceno)) as Previous_country
from Online_Retail
group by country
order by count(invoiceno) desc
--Description: We have reached the amount of purchase in all countries of the world and the order of the countries in relation to before and after.

-------------------------------

--5- /*Story: I am trying to get information when the last time the customer made a purchase,
and when was the time before it, how much time is between the two periods,
and the total number of times this customer made a purchase in general. */
select 
max(invoicedate) as last_day,
count(invoiceno)over(partition by customerid order by invoicedate) as total_sales_times,
lag(invoicedate,1)over(partition by customerid order by invoicedate) as Previous_Time_Buy,
max(invoicedate)-lag(invoicedate,1)over(partition by customerid order by invoicedate) as Total_days_before_repurchase
from Online_Retail
group by customerid,invoicedate,invoiceno
--Discription: We want to follow the customer's behavior in terms of the purchase process over time.

----------------------------------------------------------------------------------

-- Question Two :

WITH TempTable as
(   SELECT distinct customerid,
    MAX(invoicedate) over() - first_value(invoicedate) 
    OVER(Partition by customerid ) AS Recency,
    COUNT(*) OVER(PARTITION BY customerid) As frequency,
    SUM(ROUND(CAST(unitprice AS numeric), 2)) 
	OVER(PARTITION BY customerid) As monetary
    FROM Online_Retail
    ORDER BY customerid
)
select * ,case when (r_score = 5 and (fm_score in (5, 4)) or (r_score = 4 and fm_score = 5))
                 THEN   'Champions'
           when ((r_score in( 5 , 4) and fm_score = 2) or ((r_score in( 3, 4) and fm_score = 3)))
                 THEN 'Potential loyalists'
           when ((r_score = 5  and fm_score = 3) or (r_score = 4  and fm_score = 4) or (r_score = 3 and (fm_score in( 4 , 5))))
                 THEN 'Loyal Customers'
           when (r_score = 5  and fm_score = 1) 
                 THEN 'Loyal Customers'
           when ((r_score in( 4, 3))  and fm_score = 1) 
                 THEN 'Promising'
           when ((r_score in( 3, 2) and fm_score = 2) or (r_score = 2 and fm_score = 3))
                 THEN 'Customers Needing Attention'
           when ((r_score = 2 and (fm_score in( 5, 4))) or (r_score = 1 and fm_score = 3))
                 THEN 'At Risk'
           when (r_score = 1 and (fm_score in( 5, 4)))
                 THEN 'Cant Lose Them'
           when (r_score = 1  and fm_score = 2)
                 THEN 'Hibernating'
           when (r_score = 1  and fm_score = 1)
                 THEN 'Lost'
       END as GroupName
from(SELECT  *,
        NTILE(5) OVER(ORDER BY RECENCY desc) as r_score,
		NTILE(5) OVER(order by round(((cast(frequency as numeric)+
				cast(monetary as numeric))/2))) as fm_score
          FROM TempTable
          order BY  monetary)as Tempo ;