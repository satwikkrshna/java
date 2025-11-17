SOlution kept,


2nd question pettesa guys  ans pls




















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


now this is the actual question  without any comments or explanation just give solution 

write a function process_data(p_value1,p_value2) in python which has two parameter p_value1 and p_value2. p_value 2 i default parameter with value none

the function must process p_value1,p_value2 and return the data as per following requirements 

a. if the data type of the parameter p_value1 is string and is not empty return the frequency of the ocurence of character "e" in the string 
b. if the data type of the parameter p_value1 is tuple return the last element of the tuple
c. if the data type of the parameter p_value1  and p_value2 is integer retunr a random number between p_value1 and p_value2-1.
d. if the data type of the parameter p_value1 is float return the smallest integer that is greater than or equal to p_value1
e. if the data type of the parameter p_value1 is is list and p_value2 is any data type add p_value2 as the second element of p_value1. return modified list.
f. if the data type of the parameter p_value1 is dictionary, add a new key as M value as Mysuru. return p_value 1.


example

requirement           p_value1              p_value2              output
a                   "Apple"            None                         1
b                 (10,20,30)               None                       30
c                    2,8                   None                       6
d                     2,3                  None                      3
e               [100,200,300]             10              [100,10,200,300]
f              {"I":"India,"A":"USA"}                       {"I":India","A":"USA","M":"Mysuru"}





Solution:
import random
import math

def process_data(p_value1, p_value2=None):
    if isinstance(p_value1, str) and p_value1 != "":
        return p_value1.count("e")
    elif isinstance(p_value1, tuple):
        return p_value1[-1]
    elif isinstance(p_value1, int) and isinstance(p_value2, int):
        return random.randint(p_value1, p_value2 - 1)
    elif isinstance(p_value1, float):
        return math.ceil(p_value1)
    elif isinstance(p_value1, list):
        p_value1.insert(1, p_value2)
        return p_value1
    elif isinstance(p_value1, dict):
        p_value1["M"] = "Mysuru"
        return p_value1

# sample calls
# print(process_data("Apple"))
# print(process_data((10,20,30)))
# print(process_data(2,8))
# print(process_data(2.3))
# print(process_data([100,200,300],10))
# print(process_data({"I":"India","A":"USA"}))




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
