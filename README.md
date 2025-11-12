ok 6th occhindhi 5th rale last lo cheddham 5th ill type 1,2,3,4 by 3:25

i gave 1,2,3,4 pls give fast solutions 
































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

flights.txt
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








now give me dataframes solution for this questions without comments and explanation exactly like i showed above also change the inferSchema lines according to the new data set 






5 Solution
df2 = df_flights.groupBy("source", "destination", "flightid", "flightname", "flighttype").count().filter("count > 1").select("flightid","flightname","flighttype","source","destination").orderBy("source","destination","flightid").show()




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

bookings.txt
bookingid,flightid,custid,travelclass,flightcharge,bookingdate
201,F101,C301,Business,12000,22-Mar-18

customers.txt
c301,John

flights.txt
flightid,flightname,flighttype,source,destination
F101,Spice Jet Airlines,Domestic,Mumbai,Kolkata



question 1 
display flightid,flightname and flighttype of glight whose name contains the word 'Jet'(do case insensitive match) or flighttype is 'Domestic' output must be in tuple format 

a part of sample oytput is 

['F101', 'Space Jet Airlines', 'Domestic']
['F102', 'Indian jet Airlines', 'International']
['F103', 'Deccan Airlines', 'Domestic']



question 2
display flightid and total number of bookings available for flights which hae atleast 1 booking. Arrage the records based on increasing order of flightid. output must be in tuple format 
a part of sample output is 

('F101', 4)
('F103', 1)



question 3
display the details of bookins with max flightcharge. arrange the records based on the descending order of booking id out put must be in tuple form 
a part of output is 

['206', 'F105', 'C301', 'Business', '30000', '22-Jan-19'] 
['202', 'F105', 'C302', 'Business', '30000', '17-Sep-18'] 


question 4 
display custid,custname,travelclass and total number of bookings based on travel class for each customer. arrange the records based on alphabetical order of custid, custname and travelclass output must be in tupele format 

a part of sample output is 

('C303', 'Robert', 'Business', 2)
('C304', 'Albert', 'Business', 1)
('C304', 'Albert', 'Economy', 2)




now give me pyspark core solution for this questions without comments and explanation exactly like i showed above also change the header and split lines according to new data set 

1 SOlution
flights_rdd = sc.textFile("flights.txt")
header = flights_rdd.first()
rdd2 = flights_rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

rdd3 = rdd2.filter(lambda x: ("jet" in x[1].lower() or x[2] == "Domestic"))
for i in rdd3.collect():
    print(i)

2 Solution
rdd3=bookrdd.map(lambda x:(x[1],1)).reduceByKey(lambda a,b:a+b).sortByKey()
for i in rdd3.collect():
    print(i)

3 SOlution
book_rdd=sc.textFile("bookings.txt")
header=book_rdd.first()
bookrdd=book_rdd.filter(lambda x:x!=header).map(lambda x:x.split(","))

max_charge=bookrdd.map(lambda x:float(x[4])).max()
rdd3=bookrdd.filter(lambda x:float(x[4])==max_charge).sortBy(lambda x:x[0],ascending=False)
for i in rdd3.collect():
    print(i)










