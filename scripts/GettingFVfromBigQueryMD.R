## Code template to connect to CSW using R

library(bigrquery)
project <- 'bcs-breeding-datasets'

## Ensure you have a password.json file
bq_auth(path='password.json')


sql <- paste("
SELECT distinct planting_year 
, crops
, country
, state
, county
, l3_name
, site_name
, field_coordinates
, sub_site_id
, organization 

 FROM `bcs-global-md-fi-lake.field_trial.v_velocity_br_all` 
 where planting_year in (2025) 
 and crops='Corn'
 and organization in ('Breeding')
 and crop_material_stage like '%Pre-Commercial 4%'
 and country='United States of America'
") 

# Get the results into a dataframe

tb <- bq_project_query(project, sql)
df <- bq_table_download(tb)



#library(dplyr)

#df <- df %>%
#  mutate(yield_bu_ac = yield_kg_ha * 0.016)

#filtered_df <- df %>%
#  filter(yield_bu_ac >= 0)



write.csv(df, "/Users/gosrt/Library/CloudStorage/OneDrive-Bayer/Documents/project_2/BrandTargetArea/MD_brand_target_area.csv", row.names = FALSE)

