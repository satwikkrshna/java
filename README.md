orey adhi kadhu anukunta solution inkemanna chudandi okasari 

Try this:(both try)

db.twowheelers.aggregate([
  {$group: {_id: "$type"}},
  {$sort: {_id: 1}},
  {$group: {_id: null, types: {$push: "$type"}}},
  {$project: {_id: 0, types: 1}}
])

Mongo
Solution
1)

db.twowheelers.aggregate([
  { $group: { _id: "$type" } },
  { $project: { _id: 0, type: "$_id" } }
])

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
the data set is 


twowheelers.txt
{type:"Scooter",name:"Acti125",colors:["Red","Gray","Black","White","Blue"],bodydimension:{length:1850,width:707,height:1160,weight:107,fueltank:5.3},enginedisplacement:123.92,range:55,exshowroomprice:92233}



question 1
display the distinct types of vehicles that are stored in the colelction 
for the sample data given the following records will appear in the output along with other records..

a part of SAMPLE OUTPUT is 

[
"EV",
"Motorcycle",
"Scooter"
]

Answer: db.twowheelers.distinct("type")

question 4 


  
  
  
  


