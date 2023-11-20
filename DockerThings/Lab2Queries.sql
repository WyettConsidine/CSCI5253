-- Query 1 How many animals of each type have outcomes?
--I.e. how many cats, dogs, birds etc. Note that this question is asking about number of animals, 
--not number of outcomes, so animals with multiple outcomes should be counted only once.
SELECT count(distinct animal_id) as "Count", 
	animal_type as "Animal Type"
FROM public.animalrecords_fct AS r
	JOIN public.animalinfo_dim AS a 
		USING(animal_id)
	join public.outcomes_dim as o
		using(outcome_id)
where outcome_type is not NULL
GROUP BY animal_type; 


--Q2: How many animals are there with more than 1 outcome?

select count(animal_id) - count(distinct animal_id)
	as "Animals with more than one outcome"
from public.animalrecords_fct;

--q3: What are the top 5 months for outcomes? 
-- Calendar months in general, not months of a particular year. This means answer will be like April, October, etc rather than April 2013, October 2018, 

select d.month, count(*) as "Outcome count"
from public.animalrecords_fct as r
	join public.date_dim as d
	on d.date_id = r.date_id 
group by d."month"
order by count(*) desc
limit 5;

--q4: A "Kitten" is a "Cat" who is less than 1 year old. A "Senior cat" is a "Cat" who is over 10 years old. An "Adult" is a cat who is between 1 and 10 
--years old.

--NOTE FROM STUDENT (Wyett): If the queries return a number, not a percentage, the two queries will return the same thing. 

-- What is the total number of kittens, adults, and seniors, whose outcome is "Adopted"?
with 
	catAges
as
(select EXTRACT(DAY FROM dd.outtime-ad.dob)/365 as "Age upon Outcome", od.outcome_type as outcome
from public.animalrecords_fct af 
	join public.animalinfo_dim ad 
	using(animal_id)
	join public.date_dim dd 
	using(date_id)
	join public.outcomes_dim od 
	using(outcome_id)
where animal_type = 'Cat'
)
select count(*) as "Age upon adoption: 1: Kitten 2: Adult 3: Senior" from catAges
where outcome = 'Adoption'
group by "Age upon Outcome" > 10, "Age upon Outcome" > 1 and "Age upon Outcome" < 10, "Age upon Outcome" < 1;


-- Conversely, among all the cats who were "Adopted", what is the total number of kittens, adults, and seniors?
	
with 
	catAges
as
(select EXTRACT(DAY FROM dd.outtime-ad.dob)/365 as "Age upon Outcome"
from public.animalrecords_fct af 
	join public.animalinfo_dim ad 
	using(animal_id)
	join public.date_dim dd 
	using(date_id)
	join public.outcomes_dim od 
	using(outcome_id)
where animal_type = 'Cat'
and outcome_type = 'Adoption'
)
select 
	count(*) as "Age upon adoption: 1: Kitten 2: Adult 3: Senior"
from catAges
group by  "Age upon Outcome" > 10, "Age upon Outcome" > 1 and "Age upon Outcome" < 10, "Age upon Outcome" < 1;


-- Q-5 For each date, what is the cumulative total of outcomes up to and including this date?

with 
	outComeDays
as (
select count(*) as ct, CAST(outtime AS DATE) as dayOut
		from public.date_dim
		group by CAST(outtime AS DATE) 
		order by CAST(outtime AS DATE)
)
SELECT ct as num_outcomes, dayOut as Day, SUM(ct) over (order by dayOut) AS cumulative_total
from outComeDays ;

