-- Ensure "code" column is PK
CREATE OR REPLACE TABLE open_food_facts.open_food_facts_deduped AS
WITH dedup AS (
SELECT code, ARRAY_AGG(last_modified_t ORDER BY last_modified_t DESC)[OFFSET(0)] as last_modified_t, ARRAY_AGG(food_groups ORDER BY food_groups DESC)[OFFSET(0)] AS food_groups
FROM open_food_facts.open_food_facts_raw_csv
GROUP BY code)

SELECT DISTINCT a.*  
FROM open_food_factsopen_food_facts_raw_csv AS a
JOIN dedup
USING (code,last_modified_t,food_groups);



-- create ingredients flattened table
CREATE OR REPLACE TABLE open_food_facts.open_food_facts_english_ingredients AS
SELECT code, LOWER(TRIM(REGEXP_REPLACE(REGEXP_EXTRACT(flattened_ingredients, r'^.*\(|.*'),r'[^a-zA-Z0-9\s]',r''))) AS ingredient
FROM open_food_facts.open_food_facts_deduped
CROSS JOIN UNNEST(SPLIT(ingredients_text) ) AS flattened_ingredients
WHERE flattened_ingredients IS NOT NULL 
AND flattened_ingredients <> ''
AND (LOWER(countries_en) LIKE ('%united%') OR LOWER(countries_en) LIKE ('%australia%'))
ORDER BY code;



-- create allergens flattened table
CREATE OR REPLACE TABLE open_food_facts.open_food_facts_allergens_flattened AS
SELECT code, flattened_allergens
FROM `open_food_facts_deduped`
CROSS JOIN UNNEST(SPLIT(allergens)) AS flattened_allergens
WHERE (LOWER(countries_en) LIKE ('%united%') OR LOWER(countries_en) LIKE ('%australia%'))
ORDER BY code;


-- create allergens in english table
CREATE OR REPLACE TABLE open_food_facts.open_food_facts_english_allergens AS
SELECT *, LOWER(REGEXP_EXTRACT(flattened_allergens, r'^en:(.*)')) AS en_allergens
FROM open_food_facts.open_food_facts_allergens_flattened
WHERE REGEXP_EXTRACT(flattened_allergens, r'^en:(.*)') IS NOT NULL
AND flattened_allergens <> "en:none";


-- update allergens table
CREATE OR REPLACE TABLE open_food_facts.open_food_facts_allergens_updated AS
SELECT code, en_allergens AS allergens
FROM
open_food_facts.open_food_facts_english_allergens
UNION DISTINCT
SELECT * FROM (
SELECT code,
CASE 
  WHEN ingredient = 'milk' THEN 'milk'
  WHEN REGEXP_CONTAINS(ingredient,r'soybean|soy.bean') THEN 'soybeans'
  WHEN ingredient LIKE '%egg%' THEN 'eggs'
  WHEN ingredient LIKE '%peanut%' THEN 'peanuts'
  WHEN ingredient LIKE '%nut%' THEN 'nuts'
  WHEN ingredient LIKE '%celery%' THEN 'celery'
  WHEN ingredient LIKE '%sesame%' THEN 'sesame-seeds'
  WHEN ingredient LIKE '%sulphur%' THEN 'sulphur-dioxide-and-sulphites'
  WHEN ingredient LIKE '%mustard%' THEN 'mustard'
  WHEN ingredient LIKE '%fish%' THEN 'fish'
  WHEN ingredient LIKE '%mollusc%' THEN 'molluscs'
  WHEN ingredient LIKE '%coconut%' THEN 'coconut'
  ELSE ''
  END AS allergens
FROM
open_food_facts.open_food_facts_english_ingredients
LEFT JOIN
open_food_facts.open_food_facts_english_allergens AS b
USING (code) 
WHERE b.code IS NULL)
;



-- allergens cleaned and grouped
CREATE OR REPLACE TABLE open_food_facts.open_food_facts_english_agg_allergens AS
SELECT code, STRING_AGG((INITCAP(allergens)),', ') AS allergens
FROM open_food_facts.open_food_facts_allergens_updated
WHERE allergens <> ''
GROUP BY code;


-- Look at MOST COMMON ALLERGENS
SELECT allergens, count(*) as cnt
FROM
open_food_facts.open_food_facts_allergens_updated
GROUP BY allergens
ORDER BY cnt DESC;



-- create english foods table
CREATE OR REPLACE TABLE open_food_facts.open_food_facts_foods AS
SELECT DISTINCT code, url, product_name, generic_name, ROUND(energy_kcal_100g,1) AS energy_kcal_100g,
ROUND(energy_from_fat_100g,1) AS energy_from_fat_100g,
ROUND(fat_100g,1) AS fat_100g, saturated_fat_100g, monounsaturated_fat_100g, polyunsaturated_fat_100g, omega_3_fat_100g, omega_9_fat_100g, trans_fat_100g,
ROUND(cholesterol_100g,1) AS cholesterol_100g,
ROUND(carbohydrates_100g,1) AS carbohydrates_100g,
ROUND(sugars_100g,1) AS sugars_100g,
ROUND(proteins_100g,1) AS proteins_100g
, _sucrose_100g, _glucose_100g, _fructose_100g, _lactose_100g, _maltose_100g, _maltodextrins_100g, starch_100g, polyols_100g, fiber_100g, soluble_fiber_100g, insoluble_fiber_100g, casein_100g, serum_proteins_100g, nucleotides_100g, salt_100g, sodium_100g, alcohol_100g, vitamin_a_100g, beta_carotene_100g, vitamin_d_100g, vitamin_e_100g, vitamin_k_100g, vitamin_c_100g, vitamin_b1_100g, vitamin_b2_100g, vitamin_pp_100g, vitamin_b6_100g, vitamin_b9_100g, folates_100g, vitamin_b12_100g, biotin_100g, pantothenic_acid_100g, silica_100g, bicarbonate_100g, potassium_100g, chloride_100g, calcium_100g, phosphorus_100g, iron_100g, magnesium_100g, zinc_100g, copper_100g, manganese_100g, fluoride_100g, selenium_100g, chromium_100g, molybdenum_100g, iodine_100g, caffeine_100g, taurine_100g, ph_100g, fruits_vegetables_nuts_100g, fruits_vegetables_nuts_dried_100g, fruits_vegetables_nuts_estimate_100g, fruits_vegetables_nuts_estimate_from_ingredients_100g, 
IF (proteins_100g > 10, TRUE, FALSE) is_high_protein,
IF (carbohydrates_100g BETWEEN 0 AND 10, TRUE, FALSE) is_low_carb,
IF (proteins_100g > 10 AND carbohydrates_100g BETWEEN 0 AND 10,TRUE, FALSE) is_high_protein_low_carb,
ROUND(LOG10((0.001+proteins_100g)/(carbohydrates_100g+0.001))/5,3) AS high_protein_low_carb_score,
ROUND(proteins_100g*4/energy_kcal_100g,2) AS high_protein_low_cal_score,
main_category_en AS category,
ingredients_text AS ingredients,
allergens.allergens 
FROM
open_food_facts.open_food_facts_deduped
LEFT JOIN
 open_food_facts.open_food_facts_english_agg_allergens AS allergens
USING (code)
WHERE (LOWER(countries_en) LIKE ('%united%') OR LOWER(countries_en) LIKE ('%australia%'))
AND (alcohol_100g < 1 OR alcohol_100g IS NULL)
AND energy_kcal_100g > (proteins_100g*3.8 + fat_100g*8 + carbohydrates_100g*3.8)
;



-- DEDUPE ON PRODUCT NAME (these are actually different products but it makes things easier for the dashboard)
CREATE OR REPLACE TABLE open_food_facts.multiname_dedup_on_allergens AS
SELECT 
product_name, 
ARRAY_AGG(allergens ORDER BY LENGTH(allergens) DESC)[offset(0)] AS allergens,
ARRAY_AGG(code ORDER BY LENGTH(allergens) DESC)[offset(0)] AS code,
FROM 
open_food_facts.open_food_facts_foods
GROUP BY product_name;


-- REMOVE DUPLICATE NAMES FROM MAIN TABLE
CREATE OR REPLACE TABLE open_food_facts.open_food_facts_foods AS
SELECT a.* except(allergens), a.allergens AS allergen_list FROM
open_food_facts.open_food_facts_foods AS a
JOIN
open_food_facts.multiname_dedup_on_allergens
USING(code);


-- useful for dashboard
CREATE OR REPLACE TABLE open_food_facts.open_food_facts_only_allergens AS
SELECT allergens, code
FROM
open_food_facts.open_food_facts_allergens_updated
WHERE allergens <> '';

