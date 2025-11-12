mowa clear everything and give just 2,4 solutions 

Latest:
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

bookings_rdd = sc.textFile("bookings.txt")
header = bookings_rdd.first()
bookingsrdd = bookings_rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

customers_rdd = sc.textFile("customers.txt")
header = customers_rdd.first()
customersrdd = customers_rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

# Question 2
rdd2 = bookingsrdd.map(lambda x: (x[1], 1))
rdd3 = rdd2.reduceByKey(lambda a, b: a + b).sortByKey()
for i in rdd3.collect():
    print(i)

# Question 4
cust_map = customersrdd.map(lambda x: (x[0].upper(), x[1]))
rdd4 = bookingsrdd.map(lambda x: ((x[2].upper(), x[3]), 1))
rdd5 = rdd4.reduceByKey(lambda a, b: a + b)
rdd6 = rdd5.map(lambda x: (x[0][0], x[0][1], x[1]))
rdd7 = rdd6.map(lambda x: (x[0], next(c[1] for c in customersrdd.collect() if c[0].upper() == x[0]), x[1], x[2]))
rdd8 = rdd7.sortBy(lambda x: (x[0], x[1], x[2]))
for i in rdd8.collect():
    print(i)

























2)Solution
sc = SparkContext.getOrCreate()

bookings_rdd = sc.textFile("bookings.txt")
header = bookings_rdd.first()
bookingsrdd = bookings_rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

flights_rdd = sc.textFile("flights.txt")
header = flights_rdd.first()
flightsrdd = flights_rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

customers_rdd = sc.textFile("customers.txt")
header = customers_rdd.first()
customersrdd = customers_rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

rdd2 = bookingsrdd.map(lambda x: (x[1], 1))
rdd3 = rdd2.reduceByKey(lambda a, b: a + b).sortByKey()
for i in rdd3.collect():
    print(i)






























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

bookings.txt
bookingid,flightid,custid,travelclass,flightcharge,bookingdate
201,F101,C301,Business,12000,22-Mar-18

customers.txt
c301,John

flights.txt
flightid,flightname,flighttype,source,destination
F101,Spice Jet Airlines,Domestic,Mumbai,Kolkata







question 2
display flightid and total number of bookings available for flights which hae atleast 1 booking. Arrage the records based on increasing order of flightid. output must be in tuple format 
a part of sample output is 

('F101', 4)
('F103', 1)





question 4 
display custid,custname,travelclass and total number of bookings based on travel class for each customer. arrange the records based on alphabetical order of custid, custname and travelclass output must be in tupele format 

a part of sample output is 

('C303', 'Robert', 'Business', 2)
('C304', 'Albert', 'Business', 1)
('C304', 'Albert', 'Economy', 2)




now give me pyspark core solution for this questions without comments and explanation exactly like i showed above also change the header and split lines according to new data set 



2 Solution
rdd3=bookrdd.map(lambda x:(x[1],1)).reduceByKey(lambda a,b:a+b).sortByKey()
for i in rdd3.collect():
    print(i)


4 Solution
rdd3=bookrdd.map(lambda x:(x[2]+'_'+x[3],1)).reduceByKey(lambda a,b:a+b) \
    .map(lambda x:(x[0].split('_')[0],x[0].split('_')[1],x[1])) \
    .join(custrdd.map(lambda x:(x[0],x[1]))) \
    .map(lambda x:(x[0],x[1][1],x[1][0][0],x[1][0][1])) \
    .sortBy(lambda x:(x[0],x[1],x[2]))
for i in rdd3.collect():
    print(i)

*******

