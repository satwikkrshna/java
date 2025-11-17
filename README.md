ill give mongodb questions by 3:20 or 3:23


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
