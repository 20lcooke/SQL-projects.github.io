-- Data Cleaning Project 

SELECT * 
FROM layoffs; 

-- 1. Remove Duplicates
-- 2. Standardize Data 
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns

#create table to work with to not mess with raw data 
CREATE TABLE layoffs_staging 
LIKE layoffs; 

SELECT * 
FROM layoffs_staging; 

INSERT layoffs_staging 
SELECT *
FROM layoffs; 

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

# create CTE to identify duplicates
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
) 
SELECT *
FROM duplicate_cte
WHERE row_num > 1; 

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

# copy create statement of layoff_staging to make a new table for deleting the duplicates
CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL, 
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging3
WHERE row_num > 1;

INSERT INTO layoffs_staging3
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

# delete any row that is a copy of another 
DELETE 
FROM layoffs_staging3
WHERE row_num > 1; 

SELECT *
FROM layoffs_staging3; 

-- Standardizing Data 

SELECT company, (TRIM(company))
FROM layoffs_staging3; 

# updates company names to remove unnecessary spaces 
UPDATE layoffs_staging3
SET company = TRIM(company); 

SELECT DISTINCT industry
FROM layoffs_staging3
ORDER BY 1; 

SELECT *
FROM layoffs_staging3
WHERE industry LIKE '%Crypto%';

# updates any industry name containing 'crypto' to be the same labeling 
UPDATE layoffs_staging3 
SET industry = 'Crypto' 
WHERE industry LIKE  '%Crypto%'; 

SELECT DISTINCT location
FROM layoffs_staging3
ORDER BY 1; 

SELECT DISTINCT country
FROM layoffs_staging3
ORDER BY 1; 

# Removes any extra '.' from the country name and spaces
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) 
FROM layoffs_staging3
ORDER BY 1; 

UPDATE layoffs_staging3
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'; 

# reformats dates to correct type in SQL
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') 
FROM layoffs_staging3; 

UPDATE layoffs_staging3
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y'); 

# change data type of `date` from text to date 
ALTER TABLE layoffs_staging3
MODIFY COLUMN `date` DATE; 


-- Null and Blank Values

SELECT *
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 

# change any blank spots to being null for ease of checking 
UPDATE layoffs_staging3
SET industry = NULL 
WHERE industry = ''; 

SELECT *
FROM layoffs_staging3
WHERE industry IS NULL 
OR indistry = ''; 

SELECT * 
FROM layoffs_staging3
WHERE company = 'Airbnb'; 

SELECT t1.industry, t2.industry 
FROM layoffs_staging3 t1
JOIN layoffs_staging3 t2
	ON t1.company = t2.company 
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL; 

#Changing all NULL outputs to the correct industry type 
UPDATE layoffs_staging3 t1
JOIN layoffs_staging3 t2
	ON t1.company = t2.company 
SET t1.industry = t2.industry 
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL; 

SELECT *
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# Delete any rows that don't have any usable information 
DELETE 
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging3; 

# Delete the indicator column from table 
ALTER TABLE layoffs_staging3
DROP COLUMN row_num; 