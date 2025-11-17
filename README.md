no, motham code


#python file handling sample question

my sample data set is movies.txt

M101,Inception,Christopher Nolan,2010,Sci-Fi,8.8
M02,Titanic,James Cameron,1997,Romance,7.9

tasks:
1. Add prefix 'MOV'-before ID.
2. Extract decade from year (e.g, 2010 -> 2010s)
3. Classify as 'Hit' if IMDB>8.5 else 'Average'
4. Count number of movies per Director
5. Sort by IMDB descending

expected output:
M101,Inception,Christopher Nolan,2010,Sci-Fi,8.8

the solution for above question is 

movies:[]
director_count={}
with open("movies.txt","r") as f:
    for line in f:
        data=line.split(",")
        movieid="MOV-"+data[0]
        title=data[1]
        director=data[2]
        year=int(data[3])
        genre=data[4]
        imdb=float(data[5])
        decade=str((year//10)*10)+"s"
        status ="Hit" id imdb >=8.5 else "Average"
        director_count[director]=director_count.get(director,0)+1
        movies.append((movieid,title,director,decade,genre,imdb,status))
        movies=[(m[0],m[1],m[2],m[3],m[4],m[5],m[6], director_count[m[2]])for m in movies]
movies.sort(key=lambda x:x[5], reverse=True)
    for m in movies:
        print(",".join([str(i) for in m]))


now this is the actual dataset and actual question  without any comments or explanation just give solution 


question 1:
data set 

winter_fashion.txt

Brand,Category,Material,Price,Popularity_Score
Nike,Beanie,Wool,710.14,4
Nike,Gloves,Cotton,77.51,6.9

the question is 

create a new column OfferPrice, to display new price (price after discount) based on the following conditions

if the material is wool 5% discount on thr price
if the material is cotton, 7% discount on price
if the material is Cashmere, 6% discount on the price
if the material is leather, 2% discount on the price
if the material is polyester 10% discount on thr price 
note OfferPrice=Price-(Discount*Price/100). round the value to 2 decimal places 

also create a new colum Trend to display the value based on the following conditions
if the Popularity_Score is more than 9 then the trend is Emerging
if the Popularity_Score is more than 7 then the trend is Trending.
if the Popularity_Score is more than 5 then the trend is Classic.
else it is Outdated

also print header and processed data in the following format 
for the sample data given the following records will appear in the output along with other records


a part of SAMPLE OUTPUT is 

Brand,Categroy,Material,Price,Popularity_Score,OfferPrice,Trend
Nike,Beanie,Wool,710.14,4,674.63,Outdated
Nike,Gloves,Cotton,77.51,6.9,72.08,Classic
Nike,Boots,Cashmere,414.74,9.4,389.86,Emerging

Solution:
fashion = []

with open("winter_fashion.txt", "r") as f:
    next(f)
    for line in f:
        data = line.strip().split(",")
        brand = data[0]
        category = data[1]
        material = data[2]
        price = float(data[3])
        score = float(data[4])

        if material == "Wool":
            d = 5
        elif material == "Cotton":
            d = 7
        elif material == "Cashmere":
            d = 6
        elif material == "Leather":
            d = 2
        elif material == "Polyester":
            d = 10
        else:
            d = 0

        offer = round(price - (d * price / 100), 2)

        if score > 9:
            trend = "Emerging"
        elif score > 7:
            trend = "Trending"
        elif score > 5:
            trend = "Classic"
        else:
            trend = "Outdated"

        fashion.append([brand, category, material, price, score, offer, trend])

print("Brand,Categroy,Material,Price,Popularity_Score,OfferPrice,Trend")
for item in fashion:
    print(",".join([str(i) for i in item]))

























--------------------------------------------------------------------------------------------------------------------------------
#this is the python collections sample question

write a function that cleans and normalizes text with rules:
1. if thr string contains only digits -> format as phone number:"1234567890" ->"123-456-7890"
2. Elif string contains only alphabets:
    -if all uppercase ->make lowercase
    -if all lowercase -> make title case
    -else -> swapcase
3. Elif string contains spaces:
    -collapse mutiple spaces -> single spaces
    -trim leading/ trailing spaces
    -ensure the texts ends with a period '.'
4. Else (mixed junk with symbols) -> remove everything expect alphanumeric and spaces.

Test case:

-"1234567890" -> "123-456-7890"
-"HELLO" -> "hello"
-"hello" ->"Hello"
-"HeLLo" ->"hEllO"
-"python is GREAT" -> "python is GREAT"
-"ab@#12cd!!" ->"ab12cd"


the solution is for above question

def normalize_text(s):
    if s.isdigit() and len(s) =10:
            return f"{s[:3]}--{s[3:6]}--{s[6:]}"
    elif s.isalpha():
         if s.isupper():
              return s.lower()
         elif s.islower():
              return s.title()
         else:
              return s
    elif " " in s:
         s=re.sub(r"\s+"," ",s).strip()
         return s if s.endswith(".") else s+ "."
    else:
         return "".join(c for c in s if c.isalnum() or c==" ")
s=input("Enter text:")
print(normalize_text(s))


now this is the actual dataset and actual question  without any comments or explanation just give solution 







------------------------------------------------------------------------------------------------------------





















#MONGO DB 
this is the sample data set 
{_id:1,Name:"Helen",Hobbies:["Listening to Music","Reading books"],Address:{DoorNo:1,Street:"New Street",City:"Bangalore",Pincode:123456},Password:"H@123",Education:"B.E",Salary:25000}
{_id:2,Name:"Micheal",Hobbies:["Playing Cricket","Reading Novels"],Address:{DoorNo:"22B",Street:"Fire Street",City:"Noida",Pincode:234567},Password:"M@123",Education:"Btech",Salary:45000}


the sample question 
display maximum salary of candidates who are in bengalore

the solution for above question is 

db.collections.aggregate({$match:{"Address.City":"Bangalore"},{$group:{_id:"$Maximum Salary":{$max:"$Salary":{$max:"$Salary"}}},{$project:{_id:0,"Maximum Salary":1}}}})

use like either aggregate or find or updatone anything shich is required
now this is the actual dataset and actual question  without any comments or explanation just give solution 


now this is the actual dataset and actual question also db.(this should be datasetname).aggretage take care of this not db.collections all the time change the collections according to data set 




MONGO DB QUESTION 
this is the sample question 
