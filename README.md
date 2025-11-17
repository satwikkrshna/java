1st dhi working ill write second question by 2:55





















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
