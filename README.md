#SQL 

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

df_cust.createOrReplaceTempView('cust')
df_cust.createOrReplaceTempView('prod')
df_cust.createOrReplaceTempView('ord')
from pyspark.sql.functions import *

the example question is 

Display the details of premium customers who are from "Houston" or "Chicago"

spark sql solution is 
spark.sql("select * from cust where ctype='Premium' and city ='Houston' or city ='Chicago'").show()

now this is my new data set 


now give me spark.sql solution for this questions without comments and explanation exactly like i showed above also change the inferschema & createOrReplaceTempView lines according to the new data set 















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


now give me dataframes solution for this questions without comments and explanation exactly like i showed above also change the inferSchema lines according to the new data set 

















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











