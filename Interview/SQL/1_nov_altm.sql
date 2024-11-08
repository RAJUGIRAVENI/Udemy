order_id, order_date,customer_name,dob,address,country,product_name,quantity, subtotal, order_id,product_name,order_id, order_date,product_name, total_items, total_amount
 
 
 order_id, order_date,customer_name,dob,address,country,product_name,quantity, subtotal, order_id,product_name,order_id, order_date,product_name, total_items, total_amount
 
 

 
 
 
 -->
 dimenstion, fact.
 
 
 dim:
 
  custid, customer_name,dob,address,country, 
  
  ---
 product:
 
  product id , product_name
  
  
  ---
  
  fact:
  
   order_id, product id, cust_id, order_date, quantity, subtotal, total_items, total_amount
   
   
   
   /fact/order_date=2024-11-08
   
   
   
   
candidate_id, candidate_name , dob 
1001 , XXXX , 1990-01-01
1002 , YYYY , 1990-02-01
1004 , TTTT , 1990-04-01

TGT 
candidate_id, candidate_name , dob
1002 ,    , 1990-01-01
1004 , SS , 1990-01-01
1005 , KKKK  ,


---->
temp 
inner src and tg

---------


insert overwrite tgt
select 

coalesce(src.candidate_id    , tgt.candidate_id     )
, coalesce(src.candidate_name  , tgt.candidate_name   )
, coalesce(src.dob             , tgt.dob              )

from src full outer join tgt 
on src.candidate_id = tgt.candidate_id
;

-----------




candidate_id, candidate_name , dob 
1001 , XXXX , 1990-01-01
1002 , YYYY , 1990-02-01
1004 , TTTT , 1990-04-01

TGT 
candidate_id, candidate_name , dob
1002 ,    , 1990-01-01
1004 , SS , 1990-01-01
1005 , KKKK  ,


---->
temp 
inner src and tg

---------
tgt: hash_id md5(candidate_id, candidate_name , dob)


insert overwrite tgt
select 

coalesce(src.candidate_id    , tgt.candidate_id     )
, coalesce(src.candidate_name  , tgt.candidate_name   )
, coalesce(src.dob             , tgt.dob              )
, current_date() as efft_strt_dt
, "2024-11-08"   as efft_end_dt
,

from src  inner join tgt 
on ( src.candidate_id = tgt.candidate_id and src.hash_id = tgt.hash_id )
;

-----------




candidate_raw_info
 
ID,Name,email,Country,Age,Gender,txn_amt
1001,John Doe,john.doe@example.com,usa,45.33,Male,100.50
1002,Jane Smith,invalid-email-address-1234,uk,70,Female,50.75
1003,Bob Johnson,bob.johnson@example.com,USA,45,Un,75.00
1004,Alice Brown,alice.brown@example.com,Canada,10,Female,120.25
 
 
country_lkp
country_code, count_name 
USA , United State
UK  , United kingdom
 
 
candidate_personal_info
 
ID.   | phone         | pancard    |updatedtime | need_to_encrypt_personal_info
1001  | 91 5534543643 | ESPS54353K |2024-08-08  | yes
1002  | 01 553453     | ESEE54353J |2024-08-10  | no
 
 
Candidate_interview_process  ( multiple entry for each candidate )
 
candidate_id |status     | starttime | endtime  
1001 		 |in-progress| 2024-08-01| NULL
1001 		 |IN-PROGRESS| 2024-08-01| NULL
1001 		 |SKIPPED    | 2024-08-01| 2024-08-04
1001 		 |Completed  | 2024-08-01| 2024-08-10
1002 		 |in-progress| 2024-08-01| NULL
1002 		 |IN-PROGRESS| 2024-08-01| NULL
1002 		 |SKIPPED    | 2024-08-01| 2024-08-04
1002 		 |SKIPPED    | 2024-08-01| 2024-08-04
1002 		 |Completed  | 2024-08-01| 2024-08-08
 
 
 select 
 candidate_id 
 , status as currnt_status 
 from (
 select 
 a.*
 , row_number() over(partition by candidate_id order by endtime desc ) as rn
 from Candidate_interview_process a 
 ) tbl2 
 where tbl2 = 1 
 
  
output :
 
ID,
only_valid_email ( if valid store else store with "invalid-email",
country_name,
category(if age > 60 then senior, if age between 19 to 59 - adult , <18 minor)
phone                 -- mask last 5 digit based on candidate preference 
is_valid_phone_number -- Check the length
country_name          -- based on phone country code 
pancard               -- mask all char to *  based on candidate preference 
current_status        -- latest status for each candidate
no_of_in_progress     -- # count 
no_of_completed       -- # count 
no_of_others          -- # count
has context menu

-- @

current_status        -- latest status for each candidate
no_of_in_progress     -- # count 
no_of_completed       -- # count 
no_of_others          -- # count

insert overwrite wrk_Candidate_interview_process

select 
 tbl2.in_progress
 , tbl2.completed
 , cnt - tbl2.in_progress - tbl2.completed
 
 from (
select 
id
, sum(case when LOWER(status) IN ('in-progress') THEN 1 ELSE 0 END AS in_progress     )
, sum(case when LOWER(status) IN ('completed') THEN 1 ELSE 0 END AS completed         )
, count(*) as cnt
from 
Candidate_interview_process tbl1
group by 
case when LOWER(status) IN ('in-progress') THEN 1 ELSE 0 END AS in_progress 
case when LOWER(status) IN ('completed') THEN 1 ELSE 0 END AS completed ) tbl2



---
select 
cri.ID 
, CASE WHEN cri.email LIKE '%@'% THEN cri.email 
     ELSE "invalid-email" END          AS only_valid_email
,  cl.country_name
, CASE WHEN cri.age 



candidate_raw_info           cri
left join 
country_lkp                  cl on ( cri.Country = cl.country_code)
left join 
candidate_personal_info     cpi on (
left join 
wrk_Candidate_interview_process  cip on (



a_cnt=$(wc -l file1.txt)
b_cnt=$(wc -l file2.txt)

if [ ${a_cnt} -gt ${b_cnt} ]
   return file1.txt
elif [ ${b_cnt} -gt ${a_cnt} ]
   return file2.txt
else
   return 

---   
