USE shelter;
-- How many animals of each type have outcomes 
select animal_type,count(*) from animal_dim group by animal_type;


-- How many animals are there with more than 1 outcome
select count(duplicate_count) from (SELECT animal_id, COUNT(*) AS duplicate_count
FROM outcome_fct 
GROUP BY animal_id
HAVING COUNT(*) > 1);


-- What are top 5 months for outcomes 
select month,count(*)
from outcome_fct
left  join date_dim  
ON outcome_fct.date_id  = date_dim.date_id
group by month
order by count desc
limit 5;



-- A kitten is a cat who is less than 1 year old. 
-- A senior cat is a cat who is over 10 years old. An adult cat is a cat who is between 1 and 10 years old.
-- what is total number of kittens, adults, and seniors whose outcome is Adopted. 

select count(animal_dim.animal_id)
from animal_dim
left  join outcome_fct
ON outcome_fct.animal_id  = animal_dim.animal_id
left join date_dim 
on outcome_fct.date_id = date_dim.date_id
left join outcome_dim
on outcome_fct.outcome_type_id = outcome_dim.outcome_type_id 
where animal_type = 'Cat' and outcome_type = 'Adoption';

-- conversely among all the cats who were adopted what is total number of kittens, adults and seniors

select count(*), cattype from
(select animal_id, animal_type, outcome_type, age,
case 
	WHEN age < 31556926 THEN 'kitten'
	WHEN age BETWEEN 31556926 AND 315569260 THEN 'adult'
	WHEN age > 315569260 THEN 'senior'
end as cattype
from (select unix_date - unix_dob AS age, animal_type, animal_dim.animal_id, outcome_type
from animal_dim
left  join outcome_fct
ON outcome_fct.animal_id  = animal_dim.animal_id
left join date_dim 
on outcome_fct.date_id = date_dim.date_id
left join outcome_dim
on outcome_fct.outcome_type_id = outcome_dim.outcome_type_id 
where animal_type = 'Cat' and outcome_type = 'Adoption'))
group by cattype;



-- For each date, what is the cumulative total of outcomes up to and including this date?



SELECT
year,month,day,count, SUM(count) OVER (ORDER BY year,month,day) AS cumulative_sum
FROM
(select year, month, day, count(*)
from
(select *
from outcome_fct
left  join animal_dim 
ON outcome_fct.animal_id  = animal_dim.animal_id
left join date_dim 
on outcome_fct.date_id = date_dim.date_id
order by unix_date)
group by year, month, day
order by year, month, day)
ORDER BY
year, month, day;
   
   








