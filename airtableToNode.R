#######################################
# AIRTABLE TO NODE ETL PIPELINE
# 
# This script pulls data from airtable, 
# transforms it and then posts it to the 
# Northwest Open Data Exchange (NODE).
# 
# Last update: 04/14/2020
#######################################
options(digits=7)
options(stringsAsFactors = F)

if(!require(pacman)) install.packages("pacman"); require(pacman) 
if(!require(airtabler)) devtools::install_github("bergant/airtabler", dependencies=TRUE)
if(!require(ggmap)) devtools::install_github("dkahle/ggmap")
if(!require(tidyr)) install.packages("tidyverse")
if(!require(sf)) install.packages("sf")
if(!require(lubridate)) install.packages("lubridate")
if(!require(ckanr)) instll.packages("ckanr")
if(!require(leaflet)) install.packages("leaflet")
if(!require(rstudioapi)) install.packages("rstudioapi")
if(!require(dotenv)) install.packages("dotenv")
p_load(airtabler, tidyverse, ggmap, sf, lubridate, ckanr, leaflet, rstudioapi, dotenv)


##############################
# WRANGLING PREP TO CREATE 
# LISTINGS WITH JOINED ADDRESS
##############################
createListingsTable <- function(listings, address){
  
  print("Creating listings with addresses...")
  listings$street <- as.character(listings$street)
  listAddress <- listings %>% left_join(address, by=c("street" = "id")) %>% 
    select(-c(street)) %>% rename(street=street.y) %>% 
    arrange(main_category)
  
  listAddress$full_address <- ifelse(is.na(listAddress$street),
                                     NA,
                                     paste(listAddress$street, 
                                           listAddress$city, "OR", listAddress$postal_code, sep=", "))
  print("Done creating listings with addresses.")
  return(listAddress)  
}

####################
# GEOCODE LISTINGS
####################
geocodeListAddress <- function(listAddress){
  print("Geocoding listings...")
  geo <- function(x){
    if(is.na(x)){
      return(NA)
    }else{
      return(geocode(x, output = "more"))  ##note this can be changes to "all" to get more results
    } 
  }
  
  gcResults <- tryCatch({
    sapply(listAddress$full_address, geo)
  }, error = function(e){
    print("There was an error geocoding: ", e)
  })
  
  ## clean up
  for(i in seq(gcResults)){
    gcResults[[i]] <- as.data.frame(gcResults[[i]])
  }
  listAddressGC <- bind_rows(gcResults) %>% bind_cols(listAddress)
  
  ##reorder cols
  GCAddressReorder <- c("id", "general_category", "main_category", "parent_organization",
                        "listing", "service_description", "street", "street2", "city", "neighborhood", 
                        "postal_code", "county",
                        "website", "hours", "lon", "lat")
  listAddressGC <- listAddressGC[, GCAddressReorder] %>%
    arrange(general_category, main_category)
  
  print("Geocoding complete.")
  return(listAddressGC)
}


#######################
# CREATE PHONE TABLE
#######################
## clean the phone table
createPhoneJoinTable <- function(phone, listings){
  print("Creating phone join table...")
  phoneKeepCols <- c("id", "phone", "phone2", "type")
  phone <- phone[,phoneKeepCols]
  
  ## create a join table for phone numbers and join it back to phone
  listingPhoneJoin <- listings[,c("id", "phone")] %>%  
    filter(!map_lgl(phone, is.null)) %>% 
    unnest(cols = c(phone)) %>% 
    right_join(phone, by=c("phone" = "id")) %>%
    rename(phone_id = phone) %>%
    rename(phone = phone.y)
  
  listPhoneJoinKeepCols <- c("id", "phone_id", "phone", "phone2", "type")
  listingPhoneJoin <- listingPhoneJoin[, listPhoneJoinKeepCols]
  
  ## clean the phone numbers parens
  listingPhoneJoin$phone <- gsub("\\(", "", listingPhoneJoin$phone)
  listingPhoneJoin$phone <- gsub("\\) ", "-", listingPhoneJoin$phone)

  print("Done creating phone join table.")
  return( listingPhoneJoin)
}

## create the geoJSON data
createGeoJson <- function(listingPhoneJoin, listAddressGC){
  
  print("Creating geoJSON data...")
  ## nest the phone data and join it to the listAddresses
  phoneNested <- as_tibble(listingPhoneJoin) %>% 
    nest(data = c(phone, phone2, type))
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

  print("Done creating geoJSON data.")
  return(listAddressPhoneGeo)
}


######################
# FILE SAVES
# (this can be moved to an AWS dump)
#####################

# function to create a file name by date
buildFileName <- function(workDir, tableName, ext){
  
  date <- lubridate::today()
  filename <- paste0(workDir, "/", tableName, "-", date, ext)
  print(paste("Created file name:", filename))
  return(filename)
}

# create the data dir
createDataDirectory <- function(workDir){
  dataDir <- paste0(workDir, "/data")
  if(!dir.exists(dataDir)){
    print(paste("Creating", dataDir, "..."))
    dir.create(dataDir)
  }else{
    print(paste("Directory", dataDir, "already exists..."))
  }
  return(dataDir)
}


#####################
# Push to NODE Phase
###################
## documentation for ckanr:
## https://cloud.r-project.org/web/packages/ckanr/ckanr.pdf

##create a new package if it doesn't exist
checkPackage <- function(packageName){
  print(paste0("Checking for package: ", packageName))
  packageShowRes <- try(package_show(packageName))
  if("try-error" %in% class(packageShowRes)){
    res <- package_create(name = packageName, 
                          title = "Rose City Resource - Dev",
                          author="Mapping Action Collective", 
                          owner_org = MAC_id,
                          maintainer = "Tim Hitchins",
                          maintainer_email= "tim@mappingaction.org",
                          license_id = "cc-nc",
                          version = "1.0",
                          state = "active",
                          notes = "# General Information 
                      This dataset contains a structured digital version of the Street Roots Rose City Resource (RCR),  
                      a small paper guide that is the most comprehensive, updated list of services for people experiencing 
                      homelessness and poverty in Multnomah and Washington counties. More than 160,000 guides are published 
                      annually by Street Roots and distributed to more than 250 organizations and entities in the Portland region.  
                      More information about the RCR can be found at http://streetroots.org/about/work/resourceguide."
    )
    print(paste0("Package not found.  Creating...: ", res$id))
    return(res)
  }else{
    print(paste0("Package found with id: ", packageShowRes$id))
    packageShowRes
  }
  
}

## create or update the resources
handleNODEResources <- function(resourceNameList, resourceIdList, resourceFilePathsList, packageResponse){

  if(length(resourceNameList) > 0){
  for (i in seq(resourceNameList)){
    print(paste0("Updating ", resourceNameList[i], "..." ))
    res <- resource_update(id = resourceIdList[[i]],
                           path = resourceFilePathsList[[i]])
    print(paste0("Updated resource ", res$name, " with id: ", res$id))
  }  

  }else{
    ### Create Resources 
    listingsResource <- resource_create(package_id = packageResponse$id, 
                                        name = "Service Listings",
                                        description = "The _Service Listings_ resource contains a structured version of 
                the service listings found in the print version of the Street Roots Rose City Resource. 
                All service listings provided with a full address include additional geographic information 
                in the form of lon/lat coordinates and associated neighborhood (where applicable). Confidential 
                locations, telephone support services, and listings without a provided address exclude additional 
                geographic information. Refer to the _Field Dictionary_ resource for details regarding each of the 
                fields in this resource. An additional _Phone_ resource is also available in the dataset and 
                can be joined to the Service Listing resource.",
                                        upload = listingsFileName, ## gloabl var
                                        format="CSV",
    ) 
    
    
    phoneResource <- resource_create(package_id = packageResponse$id, 
                                     name = "Phone",
                                     description = "The _Phone_ resource includes all the related, 
                                 joinable phone numbers of the service listings found in the _Service Listings_ resource.",
                                     upload = phoneFileName, ## gloabl var
                                     format="CSV",
                                     related_item = listingsResource$id
    )
    # upload the geojson
    geoJsonResource <- resource_create(package_id = packageResponse$id, 
                                       name="RCR Geo Data",
                                       upload = geoJsonFileName, ## gloabl var
                                       format="GeoJSON")
    
    print(paste0("Created new resources in package", packageResponse$id))
    }
  
}
##################
## Env constants 
#################
# setwd to the same dir as this script
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
wd <- getwd()
dotenv::load_dot_env(file = "rcrg.env") ##system env vars

AIRTABLE_API_KEY = Sys.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = Sys.getenv("AIRTABLE_BASE_ID")
GOOGLE_API_KEY = Sys.getenv("GOOGLE_API_KEY")
NODE_API_KEY = Sys.getenv("NODE_API_KEY") ##this is the key that will be needed to upload data to NODE

###############################
#  Airtable and CKAN constants
##############################
##load in the airtable base
rcrg <- airtabler::airtable(base=AIRTABLE_BASE_ID, tables=c("listings","address", "phone"))
##and the individual tables
airListings <- rcrg$listings$select_all()
airAddress <- rcrg$address$select_all()
airPhone <- rcrg$phone$select_all()
# servicesText <- rcrg$services_text$select_all()
# contact <- rcrg$contacts$select_all()

##name of the CKAN package to be updated
package_name <- "rose-city-resource-dev2"  ##this will need to change or be added to the sys envs

######################
# RUN PHASE
####################
##register the google geocoding API key
register_google(GOOGLE_API_KEY)
## register CKAN
ckanr_setup(url = "https://opendata.imspdx.org/", key = NODE_API_KEY)
# wrangle and create new tables
listingAddress <- createListingsTable(airListings, airAddress ) 
geocodeListingResults <- geocodeListAddress(listingAddress) 
phoneJoinTable <- createPhoneJoinTable(airPhone, airListings)
# create geoJSON data
geoJsonData <- createGeoJson(phoneJoinTable, geocodeListingResults)  ## This data does not contain NA lon/lat
# create data directory
dataDir <- createDataDirectory(wd) 
# create new file names
phoneFileName <- buildFileName(dataDir, "rcr-phone", ".csv")
listingsFileName <- buildFileName(dataDir, "rcr-listings", ".csv")
geoJsonFileName <- buildFileName(dataDir, "rcr-listings", ".geojson")
# write files 
write_csv(phoneJoinTable, phoneFileName)
write_csv(geocodeListingResults, listingsFileName)
st_write(geoJsonData, dsn = geoJsonFileName, driver = "GeoJSON", delete_dsn = TRUE)
## Create or update the resources
packageRes <- checkPackage(package_name)
resourceNames <- lapply(packageRes$resources, function(x) return( x$name ))
resourceIds <- lapply(packageRes$resources, function(x) return( x$id ))
resourceFilePaths <- list(listingsFileName, phoneFileName, geoJsonFileName)
handleNODEResources(resourceNames, resourceIds, resourceFilePaths, packageRes)
print("Done...")


##########################
# OPTIONAL MAPPING 
# Use this to check on the 
# quality of the geojson
##########################
##  COMMENTED OUT 
## DO NOT ERASE / USE FOR QA\

# NOTE THIS CODE WILL BE NEED TO BE UPDATED
# listAddressGC is now geocodeListingResults 

# listAddressPhone <- listAddressGC %>% 
#   left_join(phoneNested, by=c("id" = "id")) %>% 
#   rename(phone_number=data) %>% 
#   arrange(main_category)

## FOR THE LEAFLET MAP
#listAddressGeo_table <- listAddressPhone[!is.na(listAddressPhone$lat),] 
# 
# leaflet(listAddressGeo_table) %>% 
#   setView(lng=-122.676483, lat=45.523064, zoom=10) %>%
#   addProviderTiles(providers$Stamen.Toner, group = "Toner") %>%
#   addProviderTiles(providers$Stamen.TonerLite, group = "Toner Lite") %>%
#   addTiles(group = "OSM") %>% 
#   addMarkers(lng = ~lon, lat = ~lat, 
#              popup = paste0("<h3><b>", listAddressGeo_table$listing, "</b></h3>", 
#                             "<p><b>Category:</b> ", listAddressGeo_table$main_category, "</p>"
#              ), 
#              clusterOptions = markerClusterOptions(removeOutsideVisibleBounds = F)) %>%
#   addLayersControl(baseGroups = c("Toner", "Toner Lite", "OSM"))

