Q1. 
a)
 select date_of_event,count(*) as opened_per_day from  pull_request where events='opened' group by date_of_event order by date_of_event ;

b) 
select date_of_event,count(*) as opened_per_day from  pull_request where events=’discussed’ group by date_of_event order by date_of_event ;

Q2. 
select Distinct on(mm) name_of_author from( 
	select* from (
select name_of_author, max(ee) as ee1,mm from (
select name_of_author, count(events) as ee,
date_trunc('month', date_of_event) as mm
from pull_request where events='discussed' group by mm,name_of_author)
as tempp group by name_of_author,mm order by mm DESC) as tempp3 
group by mm,name_of_author,ee1 order by ee1 DESC) as dd order by mm,ee1 DESC 

Q3.
select Distinct on(mm) name_of_author from( 
	select* from (
select name_of_author, max(ee) as ee1,mm from (
select name_of_author, count(events) as ee,
date_trunc('week', date_of_event) as mm
from pull_request where events='discussed' group by mm,name_of_author)
as tempp group by name_of_author,mm order by mm DESC) as tempp3 
group by mm,name_of_author,ee1 order by ee1 DESC) as dd order by mm,ee1 DESC

Q4.
select count(*), 
  date_trunc('week', date_of_event)
from pull_request 
where events = 'opened'
group by 
  date_trunc('week', date_of_event) 
order by date_trunc desc ;

Q5.
select sum(res),Extract(month from date_of_event) as month_in_2010 from(
select Count(*) as res,date_of_event from pull_request where Extract(year from date_of_event)=2010 and events=
'merged' group by date_of_event) as xx group by Extract(month from date_of_event);

Q6.
select count(events),date_of_event from pull_request
 group by EXTRACT(day from date_of_event ),date_of_event
 order by date_of_event;

Q7.
select name_of_author
from(
	select name_of_author,count(events) as 
	pull_request_from_author_in_2011 
	from pull_request where events='opened'
	and EXTRACT(year from date_of_event )='2011' 
	group by name_of_author) as 
	dd order by pull_request_from_author_in_2011 DESC Limit 1; 



