5th ichenu after 6th,ala giving

given question 5,6 fast by 2:55 pls ,Check 
5th not working want 6th also simple solution

Check 5,6 SOlutions before RDD

8 Solution
spark.sql("SELECT DISTINCT b.custid, f.flighttype, b.travelclass FROM bookings b JOIN flights f ON b.flightid = f.flightid GROUP BY b.custid, f.flighttype, b.travelclass HAVING COUNT(b.flightid) > 1 ORDER BY b.custid ASC").show()




7 Solution
spark.sql("select distinct f.flightid, f.flightname, f.flighttype, b.flightcharge from flights f join bookings b on f.flightid=b.flightid where b.flightcharge < (select avg(flightcharge) from bookings) order by f.flightid, f.flighttype, b.flightcharge").show()



































#DATAFRAMES

example dataset 

cust.txt
cust_id,name,ctype,email,city
101,John doe,Premium,john.doe@email.com,New York

prod.txt
p_id,pname,category,subcategory,price
1001,Laptop Pro,Electronics,Laptops,1200.0

ord.csv
o_id,cust_id,p_id,ord_date,del_date,quantity,ord_amount
2001,101,1001,2025-08-11,2025-08-14,1,1200.0

this is how my data set looks like 
i already typed this below code 

df_cust=spark.read.csv('cust.txt',header=True,inferSchema=True)
df_prod=spark.read.csv('prod.txt',header=True,inferSchema=True)
df_prod=spark.read.csv('ord.txt',header=True,inferSchema=True)

the example question is 

Display the details of premium customers who are from "Houston" or "Chicago"

dataframes solution is 
cdf2=df_cust.filter("ctype=='Premium' and city=='Houston' or city=='Chicago'").show()

now this is my new data set 

bookings.txt
bookingid,flightid,custid,travelclass,flightcharge,bookingdate
201,F101,C301,Business,12000,22-Mar-18

customers.txt
c301,John

flightid,flightname,flighttype,source,destination
F101,Spice Jet Airlines,Domestic,Mumbai,Kolkata


question 5 
display flightid,flightname,flighttype,source and destination for the flights which are traveling btw same source and desticnation. Arrange the records in alphabetical order of source, destincation and flight id 

the part od sample output is 

flightid        flightname      flighttype      source        destination
   F103     Deccan Airlines      Domestic       Chennai         Bengaluru
   F106        Air India         Domestic       Chennai         Bengaluru
   F105     Swiss Airlines      International    Zurich         Spain 
   F107     Indigo Airlines     International    Zurich         Spain

question 6
display flightid,flighttype and total flight charge collected by each flight as total_flight_charge(column alias) for all flights,. Display total fligth charges as 0 if a flight is yet to collect any flight charge arrange records based on descrending order of flight id.

part of sample output is 

flightid      flighttype     total_flight_charge    
    F103        Domestic                    3000
    F102    International                      0
    F101        Domestic                    51500
  







now give me dataframes solution for this questions without comments and explanation exactly like i showed above also change the inferSchema lines according to the new data set 






5 Solution
df2 = df_flights.groupBy("source", "destination", "flightid", "flightname", "flighttype").count().filter("count > 1").select("flightid","flightname","flighttype","source","destination").orderBy("source","destination","flightid").show()


6 Solution
df2 = df_flights.join(df_bookings, on="flightid", how="left").groupBy("flightid","flighttype").agg(sum("flightcharge").alias("total_flight_charge")).na.fill(0,subset=["total_flight_charge"]).orderBy(desc("flightid")).show()



5 SOlution updated(Got two)
i)
fdf3=df_flight.join(df_flight.groupBy("source","destination").count().filter("count>1"),["source","destination"],"inner").select("flightid","flightname","flighttype","source","destination").orderBy("source","destination","flightid").show()
ii)
df_book=spark.read.csv('bookings.txt',header=True,inferSchema=True)
df_cust=spark.read.csv('customers.txt',header=True,inferSchema=True)
df_flight=spark.read.csv('flights.txt',header=True,inferSchema=True)

fdf2=df_flight.groupBy("source","destination").count().filter("count>1")
fdf3=df_flight.join(fdf2,["source","destination"],"inner").select("flightid","flightname","flighttype","source","destination").orderBy("source","destination","flightid").show()











#RDD
example dataset 

cust.txt
cust_id,name,ctype,email,city
101,John doe,Premium,john.doe@email.com,New York

prod.txt
p_id,pname,category,subcategory,price
1001,Laptop Pro,Electronics,Laptops,1200.0

ord.csv
o_id,cust_id,p_id,ord_date,del_date,quantity,ord_amount
2001,101,1001,2025-08-11,2025-08-14,1,1200.0

this is how my data set looks like 
i already typed this below code 

cust_rdd=sc.textFile("cust.txt")
header=cust_rdd.first()
custrdd=cust_rdd.filter(lambda x:x!=header).map(lambda x:x.split(","))
prod_rdd=sc.textFile("prod.txt")
header=prod_rdd.first()
prodrdd=prod_rdd.filter(lambda x:x!=header).map(lambda x:x.split(","))
ord_rdd=sc.textFile("ord.txt")
header=ord_rdd.first()
ordrdd=ord_rdd.filter(lambda x:x!=header).map(lambda x:x.split(","))

the example question is 

Display the details of premium customers who are from "Houston" or "Chicago"

pyspark code solution is 
rdd3=rdd2.filter(lambda x:(x[2]=="Premium" and x[4]=="Houston" or x[4]=="Chicago"))
for i in rdd3.collect():
print(i)

now this is my new data set 

now give me pyspark core solution for this questions without comments and explanation exactly like i showed above also change the header and split lines according to new data set 











