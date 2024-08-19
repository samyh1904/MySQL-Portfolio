# data cleaning project

select *
from layoffs_staging;


create table layoffs_staging
like layoffs;

INSERT layoffs_staging
select *
from layoffs;

# removing duplicates


with duplicate_cte as
(
select *,
row_number() over(partition by 
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1; #identified the duplicates if the row_num > 1 

CREATE TABLE `layoffs_rows` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;





select *
from layoffs_rows
where row_num> 1;

insert into layoffs_rows
select *,
row_number() over(partition by 
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging; #create a copy table with row numbers to be able to delete duplicated rows as a whole

delete
from layoffs_rows
where row_num > 1; #delete duplicates from copy table


#standardising data

update layoffs_rows
set company = trim(company); #removing unecessary spaces infront of the name

select distinct industry
from layoffs_rows
order by 1; # identified issues in the industry collumn

select * 
from layoffs_rows
where industry like 'Crypto%';

update layoffs_rows
set industry = 'Crypto'
where industry like 'Crypto%'; # updated all industries related to 'crypto' to all match

update layoffs_rows
set country = 'United States'
where country like 'United States%';

select *
from layoffs_rows
;

select `date`, # changing the dates from text to actual date
str_to_date(`date`,'%m/%d/%Y')
from layoffs_rows;

update layoffs_rows
set `date` = str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_rows
modify column `date` date;


# working with NULL and missing data by populating


select *
from layoffs_rows
where industry is null
or industry = ''; 

select l1.industry, l2.industry
from layoffs_rows l1
join layoffs_rows l2
	on l1.company = l2.company
where (l1.industry is null)
and l2.industry is not null; #joining the missing values for industry with common companies that already have specified industries

update layoffs_rows
set industry = null
where industry = ''; #change all blanks into NULLs

Update layoffs_rows l1
join layoffs_rows l2
	on l1.company = l2.company
set l1.industry = l2.industry
where (l1.industry is null or l1.industry = '')
and l2.industry is not null; # all NULLs in industry column where filled in depending on the same company already had a specified industry

# removing rows and columns

delete
from layoffs_rows
where total_laid_off is null
and percentage_laid_off is null; # removed all rows that had no data in both these columns


Alter table layoffs_rows
drop column row_num; #removed extra column we added at the start

select *
from layoffs_rows;
