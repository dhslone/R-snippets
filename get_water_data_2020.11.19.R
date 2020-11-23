# scripts to download tides, temperatures, etc. 
# from NOAA USGS and SFWMD gauges
# Daniel Slone, Research Ecologist, USGS
# dslone@usgs.gov
# It's a work in progress!

rm(list=ls())
setwd("~/R/Tides")


# Library calls -----------------------------------------------------------

library(data.table) # data.table, rbindlist
library(dbhydroR) # get_dbkey, get_hydro
library(httr) # content, GET, modify_url
library(jsonlite) # fromJSON
library(scales) # date_format
library(tidyverse) # aes, arrange, do, facet_wrap, filter, geom_line, 
# geom_point, ggplot, glimpse, group_by, mutate, mutate_at, mutate_if, 
# read_csv, scale_x_datetime, select, starts_with, str_count, type_convert, 
# glimpse
library(waterData) # importDVs, siteInfo, tellMeURL
library(XML) # readHTMLTable

# functions ---------------------------------------------------------------

# check to see if data exists before binding
HasLength <- function(x) {length(x) > 1}

# convert decimal degrees to 
# DBHYDRO style degrees-minute-seconds (ddmmss.sss)
# dd_in is numeric
dd_to_dms <- function(dd_in){
  d = floor(abs(dd_in))
  m = floor((abs(dd_in) - d) * 60)
  s = round((abs(dd_in) - d - m/60) * 3600, digits = 3)         
  paste(d,paste(str_pad(c(m,s),2,pad="0"),collapse=""),sep="")
}

# convert DBHYDRO style degrees-minute-seconds (ddmmss.sss)
# to decimal degrees
# dms_in is character or numeric 
dms_to_dd <- function(dms_in){
  d <- ifelse(as.numeric(dms_in)<=1000000,
              ifelse(as.numeric(dms_in)<=100000, 1, 2), 3)        
  as.numeric(substr(as.character(dms_in),1,d)) +
    as.numeric(substr(as.character(dms_in),(d+1),(d+2)))/60 +
    as.numeric(substr(as.character(dms_in),(d+3),100))/3600 
}



# helper function to automate noaa data retrieval
noaa_list_build <- function(url,product) 
{
  # tide_df <- do.call(cbind, lapply(jsonlite::fromJSON(url), as.data.frame, stringsAsFactors = FALSE)) 
  tide_df <- as.data.frame(jsonlite::fromJSON(url), stringsAsFactors = FALSE) 
  if( nrow(tide_df)<=1){
    tide_df <- data.frame(product=product)
  }else{
    tide_df$product = product
  }
  return(tide_df)
}

# query dbHydro and return a list
# $meta is metadata about each station
# $data is a data frame
dbhydro_data <- function(dbkeys,start_date,end_date){
  # Example PSRP stage
  # dbkeys = "OB422/OB421/PT071/PT075"
  # start_date = as.Date("2000-01-01", "%Y-%m-%d") 
  # end_date = as.Date("2019-12-31", "%Y-%m-%d") 
  # PSRP_stage <- dbhydro_data(dbkeys,start_date,end_date)
  
  query <- list(
    v_period = "uspec",
    v_start_date = format(start_date,"%Y%m%d"),
    v_end_date = format(end_date,"%Y%m%d"),
    v_report_type = "format6",
    v_target_code = "file_csv",
    v_run_mode = "onLine",
    v_js_flag = "Y",
    v_dbkey = dbkeys
  )
  
  # how many stations are queried
  num_keys_in <- str_count(dbkeys,"/") + 1
  
  path = "dbhydroplsql/web_io.report_process" 
  
  url <- modify_url("https://my.sfwmd.gov/",path = path, query =  query)
  
  resp = GET(url)
  
  resp2 =content(resp, "raw", encoding = "UTF-8")
  
  # check the format of the table
  raw <- read_csv(resp2, n_max = (num_keys_in+5), skip = 1,
                  col_names = FALSE)
  
  # how many stations were retrieved
  num_keys <- min(which(is.na(raw[,7]))) - 2
  
  meta <- read_csv(resp2, n_max = num_keys,
                   skip = 1, col_names = TRUE)
  
  skip <- ifelse(raw[(num_keys+2),1] == "Station",
                 num_keys+3, num_keys+2)
  
  resp3 <- read_csv(resp2, skip = skip,
                    col_names = FALSE, col_types = "cccncc")
  names(resp3) <- c("Station", "DBKEY", "Daily_Date",
                    "Data_Value", "Qualifer", "Revision_Date")
  
  data <- merge(resp3,meta %>% 
                  select("DBKEY", "TYPE", "UNITS", "STAT"),
                by.x = "DBKEY", by.y ="DBKEY") %>% 
    mutate_at(c("Daily_Date", "Revision_Date"),
              lubridate::dmy) %>% 
    arrange(DBKEY,Daily_Date)
  
  return(setNames(list(meta,data), c("meta","data")))
}


# NOAA sites --------------------------------------------------------------


# NOAA api help
# https://tidesandcurrents.noaa.gov/api/

# NOAA site
#https://tidesandcurrents.noaa.gov/api/

#Example call:
#https://tidesandcurrents.noaa.gov/api/datagetter?product=hourly_height&application=NOS.COOPS.TAC.WL&begin_date=19870101&end_date=19981231&datum=MLLW&station=8720030&time_zone=GMT&units=english&format=csv

# Application
# If you are a CO-OPS developer, please include the application name calling the API.
# If you are an external user, please provide the name of your organization for this parameter when calling this API. 
# 
# products
# Option 	Description
# water_level 	Preliminary or verified water levels, depending on availability.
# air_temperature 	Air temperature as measured at the station.
# water_temperature 	Water temperature as measured at the station.
# wind 	Wind speed, direction, and gusts as measured at the station.
# air_pressure 	Barometric pressure as measured at the station.
# air_gap 	Air Gap (distance between a bridge and the water's surface) at the station.
# conductivity 	The water's conductivity as measured at the station.
# visibility 	Visibility from the station's visibility sensor. A measure of atmospheric clarity.
# humidity 	Relative humidity as measured at the station.
# salinity 	Salinity and specific gravity data for the station.
# hourly_height 	Verified hourly height water level data for the station.
# high_low 	Verified high/low water level data for the station.
# daily_mean 	Verified daily mean water level data for the station.
# monthly_mean 	Verified monthly mean water level data for the station.
# one_minute_water_level 	One minute water level data for the station.
# predictions 	6 minute predictions water level data for the station.
# datums 	datums data for the stations.
# currents 	Currents data for currents stations.

### some stations

# Gulf of Mexico
# 8728130 ST. MARKS LHTSE., APALACHEE BAY, FL

# Florida Atlantic
# 8721604 Trident Pier, Port Canaveral, FL

# Mid Atlantic
# 8720218  Mayport (Bar Pilots Dock), FL
# 8720030 Fernandina Beach, FL
# 8670870 Fort Pulaski, GA
# 8665530 Charleston, Cooper River Entrance, SC
# 8658120 Wilmington, NC

my_begin_date = as.Date("2017-01-01", "%Y-%m-%d") # overall start date
my_end_date = as.Date("2019-01-31", "%Y-%m-%d") # overall end date

stations = "8658120" # 
datum = "NAVD" # "MLLW" = mean low low, "STND" = station, "NAVD"
units = "metric"
time_zone = "gmt"
application = "USGS_WARC_GNV" # change this to your 
interval = "h" # h = hour
format = "json" # json, xml, csv

# automatic NOAA api call --------------------------------------------------

# see product list above
products = c("hourly_height","water_temperature","air_temperature")

days_in_split <- 300 # must be less than 1 year download for hourly data

date_splits <- data.table(
  begin = seq(from=my_begin_date,to=my_end_date,by=days_in_split)) %>% 
  mutate(end = c(tail(begin,-1)-1,my_end_date),
         begin_date = format(begin,"%Y%m%d"),
         end_date = format(end,"%Y%m%d"),
         num_days = end-begin+1,
         row_query = NA,
         url = NA)

product_list <- vector(mode = "list", length = length(products))
station_list <-  vector(mode = "list", length = length(stations))

# pr_num=1
# st_num = 1
for (st_num in seq_along(stations)){
  message("station: ",stations[st_num])
for (pr_num in seq_along(products)){

  product <-  products[pr_num]
  print(product)
  date_splits %<>% 
    mutate(
      row_query = paste0("begin_date=",begin_date,
                     "&end_date=",end_date,
                     "&station=",stations[st_num],
                     "&product=",product,
                     "&datum=",datum,
                     "&units=", units,
                     "&time_zone=",time_zone,
                     "&application=",application,
                     "&interval=",interval,
                     "&format=",format  )) %>% 
    group_by(begin) %>% 
    do( mutate(.,
      url = modify_url("https://tidesandcurrents.noaa.gov/api/datagetter",
                       query =  row_query))
    )
  noaa_func_list <- try(lapply(seq_len(nrow(date_splits)),
                      function(x) noaa_list_build(date_splits$url[x],product)))
  							
     product_list[[pr_num]] <- rbindlist(Filter(HasLength,noaa_func_list),fill=TRUE) 
  } # pr_num
  station_list[[st_num]] <- rbindlist(Filter(HasLength,product_list),fill=TRUE) 
} # st_num


noaa_data <- rbindlist(Filter(HasLength,station_list),fill=TRUE) %>% 
  type_convert()

# cut down missing data
noaa_data_cut <- noaa_data %>% 
  filter(!is.na(data.v),
         !is.na(data.t))

glimpse(noaa_data_cut)
attributes(noaa_data_cut$data.t)

ggplot(noaa_data_cut,aes(x=data.t,y=data.v,color=product,group=product))+
  geom_line()
ggplot(noaa_data_cut,aes(x=data.t,y=data.v,color=metadata.name))+
  geom_line()+
  scale_x_datetime(labels = date_format("%m/%y"))+
  facet_wrap(~product)

file_name <- paste(c(products,stations,noaa_data_cut$metadata.name[1],
                     datum,units,
                     format(my_begin_date,"%Y%m%d"),
                     format(my_end_date,"%Y%m%d")),collapse=" ")
save(noaa_data_cut,file=paste0(file_name,".rdat"))


# manual NOAA api call ---------------------------------------------------

my_begin_date = as.Date("2019-01-01", "%Y-%m-%d") # overall start date
num_days <- 3

begin_date = format(my_begin_date, "%Y%m%d")
end_date = format(my_begin_date+num_days, "%Y%m%d")
product = "hourly_height"
#product = "water_temperature"

noaa_query = paste0("begin_date=",begin_date,
               "&end_date=",end_date,
               "&station=",stations,
               "&product=",product,
               "&datum=",datum,
               "&units=", units,
               "&time_zone=",time_zone,
               "&application=",application,
               "&interval=",interval,
               "&format=",format
)


url <- modify_url("https://tidesandcurrents.noaa.gov/api/datagetter", 
                  query =  noaa_query)
url

noaa_data <- as.data.frame(jsonlite::fromJSON(url), stringsAsFactors = FALSE)

glimpse(noaa_data)


#   USGS -----------------------------------------------------------------------

my_begin_date = as.Date("2019-01-01", "%Y-%m-%d") # overall start date
my_end_date = as.Date("2019-01-31", "%Y-%m-%d") # overall end date

#station to analyze
station = '02323500'  
#station = "02231291"

#get site name to use in plot titles and such
siteInfo(station)

#read entire time series
dis   = importDVs(staid=station,code='00060',stat='00003', 
                  sdate= "1987-01-01", edate= "1987-01-31") 
head(dis)
#dis

# This will show a properly formatted query
url <- tellMeURL(staid=station,code='00060',stat='00003', 
                 sdate= "1987-01-01", edate= "1987-01-31") 
url


# USGS json ---------------------------------------------------------------

# USGS help
# https://waterservices.usgs.gov/rest/IV-Service.html

# site
# https://waterservices.usgs.gov/nwis/

#Location filters (choose only 1 type, all others "")
# site https://maps.waterdata.usgs.gov/mapper/index.html
# station = "02323500" 

# multiple sites comma separated
# station = "02323500,02231291" 

# state
# stateCd = "FL" 

# hydrologic unit codes  
# https://water.usgs.gov/GIS/huc_name.html
# huc = "01" 

# boundary box xmin,ymin,xmax,ymax
# bBox = "-83,36.5,-81,38.5" 

# County code 
# https://help.waterdata.usgs.gov/code/county_query?fmt=html
# countyCd = "51059" 

# ~~~~location selection
# q_loc_type <- "stateCd"
# q_loc_type <- "huc"
# q_loc_type <- "bBox" 
# q_loc_type <- "countyCd"

station = "02326550,02323592,02327031" # USGS tide stations GOM
 q_loc_type <- "site"
 q_loc <- station 

# this gets passed to the query
q_loc_string <- paste0("&",q_loc_type,"=",q_loc)

# Data type
# code for the parameter of interest 
# https://help.waterdata.usgs.gov/codes-and-parameters/parameters
# can be comma separated

# ~~~~only certain parameters
# ~~~~only certain parameters
# 00060 streamflow
# 00065 gage height
# 00010 Temperature, water	
# 00060 Discharge	
# 00065 Gage height
# 00480 Salinity
# 63160 Stream level, NAVD
# 63158 Stream level, NGVD
# 62620 Elevation, ocean/est, NAVD88

# 62610	Groundwater level above NGVD 1929, feet
# 62611	Groundwater level above NAVD 1988, feet
# 62612	Groundwater level above NGVD 1929, meters
# 62613	Groundwater level above NAVD 1988, meters
# 63158	Stream water level elevation above NGVD 1929, in feet
# 63159	Stream water level elevation above NGVD 1929, in meters
# 63160	Stream water level elevation above NAVD 1988, in feet
# 63161	Stream water level elevation above NAVD 1988, in meters
# 72019	Depth to water level, feet below land surface
# 72150	Groundwater level relative to Mean Sea Level (MSL), feet
# 72170	Stage, tidally filtered, above datum, feet
# 72171	Stage, tidally filtered, above datum, meters
# 62619	Estuary or ocean water surface elevation above NGVD 1929, feet
# 62620	Estuary or ocean water surface elevation above NAVD 1988, feet
# 62621	Estuary or ocean water surface elevation above NGVD 1929, meters
# 62622	Estuary or ocean water surface elevation above NAVD 1988, meters
# 62623	Tide stage, above datum, feet
# 62624	Tide stage, above datum, meters

# q_par <- "00060,00065"
# q_par_string <- paste0("&parameterCd=",q_par)

# ~~~~all available parameters
q_par_string <- ""

# Timing (period of record, -or- start and end times)
# All records in station time zone unless "Z" appended for UTC
# Period is up to most recent data
# https://en.wikipedia.org/wiki/ISO_8601#Durations
# period = "P7D"
# Dates
# startDT = "2010-11-22"
# endDT = "2010-11-23" # blank for most recent
# Date/Time
# &startDT ="2010-11-22T12:00"
# endDT = "2010-11-22T18:00" 

# ~~~~period
# q_time <- "P1D"
# q_time_string <- paste0("&period=",q_time)

# ~~~~start/end
#startDT = "2019-01-01T00:00"
#endDT = "2019-01-01T01:00"
startDT <- my_begin_date
endDT <- my_end_date

# this is passed to the query
q_time_string <- paste0("&startDT=",startDT,"&endDT=",endDT)

# not sure if these are needed?
# datum = "NAVD" # "MLLW" = mean low low, "STND" = station, "NAVD"
# units = "metric"

# ~~~~Frequency of data
#usgs_path = "nwis/dv/" # daily values
usgs_path = "nwis/iv/" # hourly values

format = "json" 

usgs_query = paste0("&format=",format,q_loc_string,q_par_string,
                    q_time_string)


url <- modify_url("https://waterservices.usgs.gov/",path = usgs_path,
                  query =  usgs_query)
url
resp <- jsonlite::fromJSON(url)
#str(resp,max.level = 2)

resp_2 <- resp$value$timeSeries
# names(unlist(resp_2))
# resp_2$values[[1]]$method
# resp_2$sourceInfo$siteProperty
# str(resp_2,max.level = 2)

usgs_list <- vector(mode = "list", 
                    length = length(resp_2$variable$variableCode))
usgs_df_list <- usgs_list

#i=1
for(i in seq_len(length(usgs_list))){
  data_i <- resp_2$values[[i]]$value[[1]]
  if (length(data_i) != 0){
    usgs_list[[i]]$siteName <- resp_2$sourceInfo$siteName[[i]]
    usgs_list[[i]]$loc <- resp_2$sourceInfo$geoLocation$geogLocation[i,]
    usgs_list[[i]]$code <- resp_2$variable$variableCode[[i]]
    usgs_list[[i]]$USGS <- resp_2$name[[i]]
    usgs_list[[i]]$variable <- resp_2$variable$variableName[[i]]
    usgs_list[[i]]$data <- data_i
    usgs_list[[i]]$stat <- resp_2$variable$options$option[[i]]
    usgs_list[[i]]$unit <- resp_2$variable$unit[[1]][[i]]
    
    #this part generates row name warnings. OK to ignore
    usgs_df_list[[i]] <- as.data.frame(usgs_list[[i]], row.names = NULL,
                                       stringsAsFactors = FALSE )
  }
}

usgs_data <- rbindlist(Filter(HasLength,usgs_df_list),fill=TRUE) %>% 
  mutate_if(is.list, sapply, paste, collapse = ",") %>% 
  type_convert() 

attributes(usgs_data$data.dateTime)

glimpse(usgs_data)

# cut down missing data
usgs_data_cut <- usgs_data %>% 
  filter(!is.na(data.value)) 

ggplot(usgs_data_cut,
       aes(x=data.dateTime,y=data.value,color=variable, group=variable))+
  geom_line()+facet_wrap(~siteName)

file_name <- make.names(
  paste(c("nwis",q_loc_string,q_time_string,q_par_string),collapse=""))

save(usgs_data_cut,file=paste0(file_name,".rdat"))

# DBHYDRO -----------------------------------------------------------------
# Install dbhydro tools (not needed for the XML query)
#devtools::install_github("ropensci/dbhydroR")

# DBHydro get data -----------------------------------------------------

hydro_data <- get_hydro(dbkey = c("OR084", "DU537", "DU533", "OB421"),
          date_min = "2017-01-01", date_max = "2017-01-31")

glimpse(hydro_data)

# PSRP stage
# stations chosen after mapping all stage stations in PSRP area
# then choosing FU-1 weir and monitoring well series 2 and 3
# dbkeys = "OB422/OB421/PT071/PT075/PT063/PT065/PT069/PT067/PT061/PT055/PT057/PT059/PT051/PT053"
# start_date = as.Date("2000-01-01", "%Y-%m-%d") 
# end_date = as.Date("2019-12-31", "%Y-%m-%d") 
# PSRP_stage <- dbhydro_data(dbkeys,start_date,end_date)

# My function
# PSRP rainfall
dbkeys = "OR084/DU537/DU533"
start_date = as.Date("2019-01-01", "%Y-%m-%d") 
end_date = as.Date("2019-12-31", "%Y-%m-%d") 
PSRP_rain <- dbhydro_data(dbkeys,start_date,end_date)

rain_meta <- PSRP_rain$meta
glimpse(rain_meta)
rain_data <- PSRP_rain$data
glimpse(rain_data)

ggplot(rain_data, aes(x=Daily_Date,y=Data_Value,group=DBKEY))+
  geom_line(aes(color=DBKEY))


# DBHydro get stations near location -----------------------------------

# https://my.sfwmd.gov/dbhydroplsql/show_dbkey_info.show_station_info?v_station=FAKA_T&v_lat=255736.26&v_longitude=813034.26&v_distance=5
db_path = "dbhydroplsql/show_dbkey_info.show_station_info" # don't change this

db_query = "v_lat=260500&v_longitude=813000&v_distance=10" # distance in km

#db_query = paste0("&format=",format,q_loc_string,q_par_string,q_time_string)
#db_query = "v_period=uspec&v_start_date=20130101&v_end_date=20130202&v_report_type=format6&v_target_code=file_csv&v_run_mode=onLine&v_js_flag=Y&v_dbkey=15081%2F15069"

url <- modify_url("https://my.sfwmd.gov/",path = db_path, query =  db_query)
# url <- "https://my.sfwmd.gov/dbhydroplsql/web_io.report_process?v_period=uspec&v_start_date=20191215&v_end_date=20200103&v_report_type=format6&v_target_code=file_csv&v_run_mode=onLine&v_js_flag=Y&v_dbkey=VV433"

# paste into browser to get sites 
# https://my.sfwmd.gov/dbhydroplsql/show_dbkey_info.show_dbkeys_matched?display_start=1&display_quantity=2000&v_js_flag=Y&v_basin=COAST&v_basin=FAKA+UNI&v_basin=FAKAHATC&v_county=COL
#resp <- jsonlite::fromJSON(url)


resp = GET(url)

resp2 <- sub(".*(<table class=\"grid\".*?>.*</table>).*", 
             "\\1", suppressMessages(resp))

resp3 <- XML::readHTMLTable(resp2, stringsAsFactors = FALSE,
                            encoding = "UTF-8", 
                            skip.rows = 1:2, 
                            header = TRUE)[[3]] %>% 
  type_convert() 


names(resp3) <- make.names(gsub("\\n", "", names(resp3)))

resp4 <- resp3 %>% 
  mutate(Latitude = select(.,starts_with("Latitude")) %>% 
           unlist %>% dms_to_dd,
         Longitude = select(.,starts_with("Longitude")) %>% 
           unlist %>% dms_to_dd %>% `*`(.,-1))



ggplot(resp4)+
  geom_point(aes(x=Longitude, y=Latitude,
                 color=Type))



# DBHydro get info about stations by station name --------------------------
get_dbkey(stationid = "FAKA%", category = "WEATHER", detail.level = "full")
#categories: WEATHER, SW, GW, WQ

# dbhydroplsql/show_dbkey_info.show_dbkeys_matched
#v_js_flag=Y
#v_category=SW
#v_data_type=STG88
#v_data_type=STG
#v_active_dbkeys=Y
#v_dbkey_list_flag=Y
#v_order_by=STATION

# make a spatial box for query
lower_lat = dd_to_dms(25.9)
upper_lat = dd_to_dms(26.2)
lower_long = dd_to_dms(-81.45)
upper_long = dd_to_dms(-81.6)

# query for stations within box with data types
db_query = list(
#v_basin = "FAKA%UNI",
#v_group_name = "FAKA%",
v_statistic_type = "MEAN",
v_data_type = "STG88",
v_data_type = "STG",
v_data_type = "WELL",
v_active_dbkeys = "Y",
v_lower_lat = lower_lat,
v_upper_lat = upper_lat,
v_lower_long = lower_long,
v_upper_long = upper_long,
v_order_by = "STATION")

# query for a list of stations
db_query = list(v_dbkey = "OB422/OB421/PT071/PT075/PT063/PT065/PT069/PT067/PT061/PT055/PT057/PT059/PT051/PT053")

# just the basins
# db_path = "dbhydroplsql/show_dbkey_info_detail.show_basin_info"

# most detail
db_path = "dbhydroplsql/show_dbkey_info.show_dbkeys_matched"

url <- modify_url("https://my.sfwmd.gov/",path = db_path, query =  db_query)
url

resp = GET(url)

resp2 <- sub(".*(<table class=\"grid\".*?>.*</table>).*", 
             "\\1", suppressMessages(resp))

resp3 <- XML::readHTMLTable(resp2, stringsAsFactors = FALSE,
                            encoding = "UTF-8", 
                            skip.rows = 1:2, 
                            header = TRUE)[[3]] %>% 
  type_convert() 

names(resp3) <- make.names(gsub("\\n", "", names(resp3)))

resp4 <- resp3 %>% 
  mutate(Latitude = select(.,starts_with("Latitude")) %>% 
           unlist %>% dms_to_dd,
         Longitude = select(.,starts_with("Longitude")) %>% 
           unlist %>% dms_to_dd %>% `*`(.,-1)) %>% 
  mutate_at(c("Start.Date", "End.Date"),
            lubridate::dmy) %>% 
  arrange(Latitude)

glimpse(resp4)

ggplot(resp4)+
  geom_point(aes(x=Longitude, y=Latitude,
                 color=Data.Type))


# write_csv(resp4,"PSRP_selected_stage_stations.csv")

# paste the dbkeys into a format used for dbhydro_data() function
paste(resp4$Dbkey %>% unlist(),collapse="/")

#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#

