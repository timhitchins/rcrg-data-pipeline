###############################
# AIRTABLE TO NODE
###############################
# This script is currently in development.
# Still need the logic of pushing
# listings and phone to NODE
# see the enc of the script for detauls
###############################
options(digits=7)
options(stringsAsFactors = F)

if(!require(pacman)) install.packages("pacman"); require(pacman) 
if(!require(airtabler)) devtools::install_github("bergant/airtabler", dependencies=TRUE)
if(!require(tidyr)) install.packages("tidyverse")
if(!require(ggmap)) devtools::install_github("dkahle/ggmap")
if(!require(sf)) install.packages("sf")
if(!require(leaflet)) install.packages("leaflet")
if(!require(rstudioapi)) install.packages("rstudioapi")
if(!require(dotenv)) install.packages("dotenv")
# if(!require(geojsonio)) install.packages("geojsonio")  ## This package is deprecated and needs a replacement
p_load(airtabler, tidyverse, ggmap, sf, leaflet, rstudioapi, dotenv)


##################
## Env constants 
#################
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
dotenv::load_dot_env(file = "rcrg.env")

AIRTABLE_API_KEY = Sys.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = Sys.getenv("AIRTABLE_BASE_ID")
GOOGLE_API_KEY = Sys.getenv("GOOGLE_API_KEY")
NODE_API_KEY = Sys.getenv("NODE_API_KEY") ##this is the key that will be needed to upload data to NODE
##register the google geocoding API key
register_google(GOOGLE_API_KEY)

##load in the base
rcrg <- airtabler::airtable(base=AIRTABLE_BASE_ID, tables=c("listings","address", "phone"))

##and the individual tables
listings <- rcrg$listings$select_all()
address <- rcrg$address$select_all()
phone <- rcrg$phone$select_all()
# servicesText <- rcrg$services_text$select_all()
# contact <- rcrg$contacts$select_all()


###################
## Wrangling Phase
##################
listings$street <- as.character(listings$street)
listAddress <- listings %>% left_join(address, by=c("street" = "id")) %>% 
  select(-c(street)) %>% rename(street=street.y) %>% 
  arrange(main_category)

listAddress$full_address <- ifelse(is.na(listAddress$street),
                                   NA,
                                   paste(listAddress$street, 
                                         listAddress$city, "OR", listAddress$postal_code, sep=", "))

###GEOCODE
##now geocode
geo <- function(x){
  if(is.na(x)){
    return(NA)
  }else{
    return(geocode(x, output = "more"))
  }
}

gc_results <- sapply(listAddress$full_address, geo)
for(i in seq(gc_results)){
  gc_results[[i]] <- as.data.frame(gc_results[[i]])
}
listAddressGC <- bind_rows(gc_results) %>% bind_cols(listAddress)


##add in the new borader category
###########################################
# Nick and/or Spenser, it is possible this 
# next set of cleaup logic will need 
# to be stepped through as it has been a 
# while and there 
##########################################

##GENERAL CATEGORY
categorizer <- function (x){
  
  ##create the categories
  food <- c("Food Boxes", "Meals", "Food & Grocery Assistance")
  housingAndShelter <- c("Housing Services", "Renters Services", 
                         "Severe Weather Emergency Shelter", "Shelters",
                         "Winter Shelter")
  goods <- c("Clothing", "Clothing, Food and Hygiene Items")
  transit <- c("Ride Connection")
  healthAndWellness <- c("Recovery Services", "Health Care/Dental" , 
                         "HIV/AIDS", "Health Care", "Counseling/Mediation",
                         "Syringe Exchange", "Student Health Centers")
  money <- c("Utility Assistance", "Financial Assistance") 
  careAndSafety <- c("Pet Care", "Domestic Violence & Sexual Assault")
  work <- c("Employment & Training", "Employment, Social Security")
  legal <- c("Legal Services", "Know your Rights", "Property Recovery")
  dayServices <- c("Portland Restrooms", "Day Services/Hospitality", "Mail Distribution")
  specAssistance <- c("Youth Services", "GLBTQI Resources", "Veterans Services")
  
  if(x %in% food) return("Food")
  if(x %in% housingAndShelter) return("Housing & Shelter")
  if(x %in% goods) return("Goods")
  if(x %in% transit) return("Transit")
  if(x %in% healthAndWellness) return("Health & Wellness")
  if(x %in% money) return("Money")
  if(x %in% careAndSafety) return("Care & Safety")
  if(x %in% work) return("Work")
  if(x %in% legal) return("Legal")
  if(x %in% dayServices) return("Day Services")
  if(x %in% specAssistance) return("Specialized Assistance")
  ##more considional logic will beed to be added below
  else return(NA) ## adding this as a catch all
  
  
}
listAddressGC$general_category <- unlist(lapply(listAddressGC$main_category, categorizer)) ##This wont work
##NOTE:clean up the domestic violence category in airtable

##clean up the cols names and order
##some of these may have changeed

GCAddressReorder <- c("id", "general_category", "main_category", "parent_organization",
                      "listing", "street", "street2", "city", "neighborhood", 
                      "postal_code", "county",
                      "website", "hours", "lon", "lat")
listAddressGC <- listAddressGC[, GCAddressReorder] %>%
  arrange(general_category, main_category)

##CLEAN UP SPECIFIC RECORDS
##fix coords for restrooms 
##hawthorn bridge fix
lon <- -122.673447
lat <- 45.513892
listAddressGC$lon[listAddressGC$id=="recputpXitjqJT3Ao"] <- lon
listAddressGC$lat[listAddressGC$id=="recputpXitjqJT3Ao"] <- lat
##remove the school health centers that are outdated
##Harrison Parrk & Lane removed
remSchoolsIds <- c("recL9K2SFVxSGFP44", "rec4D1buynnjhcKjf")
listAddressGC <- listAddressGC[!listAddressGC$id %in% remSchoolsIds,]


####PHONE TABLE
##now clean the phone table
phoneKeepCols <- c("id", "phone", "phone2", "type")
phone <- phone[,phoneKeepCols]

##create a join table for phone numbers and join it back to phone
listingPhoneJoin <- listings[,c("id", "phone")] %>%  
  filter(!map_lgl(phone, is.null)) %>% 
  unnest() %>% 
  right_join(phone, by=c("phone" = "id")) 

listPhoneJoinKeepCols <- c("id" ,"phone.y", "phone2", "type")
listingPhoneJoin <- listingPhoneJoin[, listPhoneJoinKeepCols] %>% 
  rename(phone=phone.y)

##clean the phone numbers parens
listingPhoneJoin$phone <- gsub("\\(", "", listingPhoneJoin$phone)
listingPhoneJoin$phone <- gsub("\\) ", "-", listingPhoneJoin$phone)


##now nest the phone data and join it to the listAddresses
phoneNested <- as_tibble(listingPhoneJoin) %>% 
  nest(-id)
listAddressPhone <- listAddressGC %>% 
  left_join(phoneNested, by=c("id" = "id")) %>% 
  rename(phone_number=data) %>% 
  arrange(main_category)

##now use sf and convert to geojson and save
##this data does not contain the phones
listAddressNonGeo <- listAddressGC[is.na(listAddressGC$lat),]
listAddressGeo <- listAddressGC[!is.na(listAddressGC$lat),] %>% 
  mutate(lat=as.numeric(lat), lon=as.numeric(lon)) %>% 
  ##convert to sf
  st_as_sf(coords = c("lon", "lat"), crs=4326, agr="constant")


##do the same thing with the nested full dataset
listAddressPhoneGeo <- listAddressPhone[!is.na(listAddressPhone$lat),] %>% 
  mutate(lat=as.numeric(lat), lon=as.numeric(lon)) %>% 
  ##convert to sf
  st_as_sf(coords = c("lon", "lat"), crs=4326, agr="constant")

listAddressGeo_table <- listAddressPhone[!is.na(listAddressPhone$lat),] 


######################
# File Saves
#####################

write.csv(listingPhoneJoin, "phone.csv", row.names = F)
write.csv(listAddressGC, "listings.csv", row.names = F)
# geojson_write(listAddressPhoneGeo, file="rcrg_wphone.geojson") # this  is deprecated
# geojson_write(listAddressGeo, file="rcrg.geojson") # this is deprecated


#######################
# OPTIONAL MAPPING 
##########################
##now map it
leaflet(listAddressGeo_table) %>% 
  setView(lng=-122.676483, lat=45.523064, zoom=10) %>%
  addProviderTiles(providers$Stamen.Toner, group = "Toner") %>%
  addProviderTiles(providers$Stamen.TonerLite, group = "Toner Lite") %>%
  addTiles(group = "OSM") %>% 
  addMarkers(lng = ~lon, lat = ~lat, 
             popup = paste0("<h3><b>", listAddressGeo_table$listing, "</b></h3>", 
                            "<p><b>Category:</b> ", listAddressGeo_table$main_category, "</p>"
             ), 
             clusterOptions = markerClusterOptions(removeOutsideVisibleBounds = F)) %>%
  addLayersControl(baseGroups = c("Toner", "Toner Lite", "OSM"))


#####################
# Push to NODE Phase
###################

###################################################################
# Pushing to NODE can go below here.  It needs to use the CKAN API
# with the Data Store API.  I beleive the upsert or DataStore request will work.  
# https://docs.ckan.org/en/2.8/maintaining/datastore.html#ckanext.datastore.logic.action.datastore_search_sql
# 
# Below is a link of how I pull the data down in the to the app.  You can see how to build the URIs
# You can look at some examples o fpulling the data down here
# https://github.com/timhitchins/rcrg-online/blob/master/app/utils/api.js
##################################################################

