
library(shiny)
library(leaflet)
library(sf)
library(dplyr)
library(DT)
library(arrow)
library(aws.s3)

# S3 bucket configuration
# AWS credentials should be set as environment variables:
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
# Set these in .Renviron (local) or Posit Connect environment variables
S3_BUCKET <- "brand-target"

# Cache directory for S3 data (works on both local and Posit Connect)
CACHE_DIR <- file.path(tempdir(), "s3_cache")
if (!dir.exists(CACHE_DIR)) dir.create(CACHE_DIR, recursive = TRUE)

# Cache duration in seconds (1 hour = 3600, 24 hours = 86400)
CACHE_DURATION_SECONDS <- 86400

#' Load a CSV or Parquet file from S3 with local caching
#'
#' @param bucket  S3 bucket name
#' @param file    Object key (path) within the bucket
#' @param colClasses Optional column classes passed to read.csv (ignored for parquet)
#' @param cache_duration Cache validity in seconds (default: 1 hour)
#' @return A data.frame
load_data_from_s3 <- function(bucket, file, colClasses = NA, cache_duration = CACHE_DURATION_SECONDS) {
  # Create cache file path
  cache_file <- file.path(CACHE_DIR, gsub("/", "_", file))
  cache_meta <- paste0(cache_file, ".meta")
  
  # Check if cached file exists and is still valid
  use_cache <- FALSE
  if (file.exists(cache_file) && file.exists(cache_meta)) {
    cached_time <- as.numeric(readLines(cache_meta, n = 1))
    if ((as.numeric(Sys.time()) - cached_time) < cache_duration) {
      use_cache <- TRUE
      message("Using cached data for: ", file)
    }
  }
  
  if (use_cache) {
    # Load from cache
    return(readRDS(cache_file))
  }
  
  # Download from S3
  message("Downloading from S3: ", file)
  if (grepl("\\.parquet$", file, ignore.case = TRUE)) {
    data <- aws.s3::s3read_using(
      FUN = arrow::read_parquet,
      bucket = bucket,
      object = file
    )
  } else {
    data <- aws.s3::s3read_using(
      FUN = read.csv,
      colClasses = colClasses,
      bucket = bucket,
      object = file
    )
  }
  
  # Save to cache
  saveRDS(data, cache_file)
  writeLines(as.character(as.numeric(Sys.time())), cache_meta)
  
  return(data)
}

# Load pre-saved shapefiles (simplified for faster loading)
# Exclude Alaska (02) and Hawaii (15)
counties_base <- readRDS("data/counties_sf_simple.rds")
counties_base <- counties_base[!counties_base$STATEFP %in% c("02", "15"), ]
states_base <- readRDS("data/states_sf_simple.rds")
states_base <- states_base[!states_base$STATEFP %in% c("02", "15"), ]

# Read parquet files from S3
df_p4_file <- as.data.frame(load_data_from_s3(bucket = S3_BUCKET, file = "NA_market_P4_breeding_locationsII.parquet"))
df_FV_file <- as.data.frame(load_data_from_s3(bucket = S3_BUCKET, file = "FV_brand_acreage.parquet"))
# normalize coordinates same as below so unique-ification works
if("centroid_lon" %in% names(df_p4_file)) df_p4_file$lon <- as.numeric(df_p4_file$centroid_lon)
if("centroid_lat" %in% names(df_p4_file)) df_p4_file$lat <- as.numeric(df_p4_file$centroid_lat)
if("centroide_lan" %in% names(df_p4_file)) df_p4_file$lat <- as.numeric(df_p4_file$centroide_lan)
if(!("lon" %in% names(df_p4_file)) && ("long" %in% names(df_p4_file))) df_p4_file$lon <- as.numeric(df_p4_file$long)
if(!("lat" %in% names(df_p4_file)) && ("latitude" %in% names(df_p4_file))) df_p4_file$lat <- as.numeric(df_p4_file$latitude)

# derive a table of unique p4 locations (site_name or lon/lat) - used for mapping regardless of market
# first compute how many markets each site serves
site_market_counts <- df_p4_file %>%
  group_by(site_name) %>%
  summarise(num_markets = n_distinct(market), 
            markets_served = paste(unique(market), collapse = ", "),
            .groups = "drop")

p4_unique <- df_p4_file %>%
  mutate(loc_id = case_when(
    "site_name" %in% names(.) & !is.na(site_name) & site_name != "" ~ as.character(site_name),
    all(c("lon","lat") %in% names(.)) & !is.na(lon) & !is.na(lat) ~ paste0(formatC(round(as.numeric(lon),6), format='f', digits=6),",",formatC(round(as.numeric(lat),6), format='f', digits=6)),
    TRUE ~ as.character(row_number())
  )) %>%
  distinct(loc_id, .keep_all = TRUE) %>%
  left_join(site_market_counts, by = "site_name")

# normalize and prepare ID/coord columns (handle various possible column names)
if("fips_code" %in% names(df_FV_file)){
  df_FV_file$fips <- sprintf("%05d", as.integer(df_FV_file$fips_code))
} else if("fips" %in% names(df_FV_file)){
  df_FV_file$fips <- sprintf("%05s", df_FV_file$fips)
}

# standardize P4 lon/lat column names into `lon` and `lat`
if("centroid_lon" %in% names(df_p4_file)) df_p4_file$lon <- as.numeric(df_p4_file$centroid_lon)
if("centroid_lat" %in% names(df_p4_file)) df_p4_file$lat <- as.numeric(df_p4_file$centroid_lat)
if("centroide_lan" %in% names(df_p4_file)) df_p4_file$lat <- as.numeric(df_p4_file$centroide_lan)

# try other common variants if still missing
if(!("lon" %in% names(df_p4_file)) && ("long" %in% names(df_p4_file))) df_p4_file$lon <- as.numeric(df_p4_file$long)
if(!("lat" %in% names(df_p4_file)) && ("latitude" %in% names(df_p4_file))) df_p4_file$lat <- as.numeric(df_p4_file$latitude)

# application ui and server
ui <- fluidPage(
  titlePanel("Brand Target Area Map"),
  sidebarLayout(
    sidebarPanel(
      h4("FieldView"),
      selectizeInput("brand_name", "Brand Name:",
                     choices = c("All" = "All"),
                     selected = "All",
                     options = list(placeholder = "Type to search...")),
      selectInput("crop_year", "Crop Year:",
                  choices = c("All", sort(unique(df_FV_file$crop_year))),
                  selected = "All"),
      hr(),
      h4("P4 Locations 2025"),
      selectizeInput("market", "Market:",
                     choices = c("All" = "All"),
                     selected = "All",
                     options = list(placeholder = "Type to search...")),
      selectInput("RM_Recomme", "County RM:",
                  choices = c("All", sort(unique(df_p4_file$RM_Recomme))),
                  selected = "All")
    ),
    mainPanel(
      leafletOutput("map", height = "800px")
    )
  )
)

server <- function(input, output, session) {
  # reactive brand counts based on crop_year and RM_Recomme filters
  reactive_brand_counts <- reactive({
    df <- df_FV_file
    crop_sel <- if(is.null(input$crop_year)) "All" else input$crop_year
    rm_sel <- if(is.null(input$RM_Recomme)) "All" else input$RM_Recomme
    if(crop_sel != "All") df <- filter(df, crop_year == crop_sel)
    if(rm_sel != "All" && "RM_Recomme" %in% names(df)){
      df <- filter(df, RM_Recomme == rm_sel)
    }
    df %>%
      group_by(brand_name) %>%
      summarise(n = n(), .groups = "drop") %>%
      arrange(brand_name)
  })
  
  # reactive market counts based on RM filter and unique locations
  reactive_market_counts <- reactive({
    df <- df_p4_file
    rm_sel <- if(is.null(input$RM_Recomme)) "All" else input$RM_Recomme
    if(rm_sel != "All") df <- filter(df, RM_Recomme == rm_sel)
    # create a stable location identifier: prefer site_name, otherwise rounded lon/lat
    df <- df %>% mutate(
      loc_id = case_when(
        "site_name" %in% names(df) & !is.na(site_name) & site_name != "" ~ as.character(site_name),
        all(c("lon","lat") %in% names(df)) & !is.na(lon) & !is.na(lat) ~ paste0(formatC(round(as.numeric(lon),6), format = 'f', digits = 6), ",", formatC(round(as.numeric(lat),6), format = 'f', digits = 6)),
        TRUE ~ as.character(row_number())
      )
    )
    df %>%
      distinct(market, loc_id, .keep_all = FALSE) %>%
      group_by(market) %>%
      summarise(n = n(), .groups = "drop") %>%
      arrange(market)
  })
  
  # Update Brand Name choices when filters change
  observe({
    counts <- reactive_brand_counts()
    current_selection <- isolate(input$brand_name)
    if(is.null(counts) || nrow(counts) == 0){
      choices <- c("All" = "All")
    } else {
      brand_choices <- setNames(counts$brand_name, paste0(counts$brand_name, " (", counts$n, ")"))
      choices <- c("All" = "All", brand_choices)
    }
    # Only update if current selection is still valid, otherwise keep it
    selected <- if(is.null(current_selection) || current_selection == "All" || current_selection %in% counts$brand_name) {
      current_selection
    } else {
      "All"
    }
    updateSelectizeInput(session, "brand_name", choices = choices, selected = selected)
  })
  
  # Update Market choices when filters change
  observe({
    counts <- reactive_market_counts()
    current <- isolate(input$market)
    if(is.null(counts) || nrow(counts) == 0){
      choices <- c("All" = "All")
    } else {
      market_choices <- setNames(counts$market, paste0(counts$market, " (", counts$n, ")"))
      choices <- c("All" = "All", market_choices)
    }
    selected <- if(is.null(current) || current == "All" || current %in% counts$market) {
      current
    } else {
      "All"
    }
    updateSelectizeInput(session, "market", choices = choices, selected = selected)
  })
  
  # reactive filtered tables
  filtered_p4 <- reactive({
    # Start from full data to properly filter by market (since a location can serve multiple markets)
    df <- df_p4_file
    market_sel <- if(is.null(input$market)) "All" else input$market
    rm_sel <- if(is.null(input$RM_Recomme)) "All" else input$RM_Recomme
    
    # Filter by market on the full dataset (before deduplication)
    if(market_sel != "All") df <- filter(df, market == market_sel)
    if(rm_sel != "All") df <- filter(df, RM_Recomme == rm_sel)
    
    # Now deduplicate to unique locations
    df <- df %>%
      mutate(loc_id = case_when(
        "site_name" %in% names(.) & !is.na(site_name) & site_name != "" ~ as.character(site_name),
        all(c("lon","lat") %in% names(.)) & !is.na(lon) & !is.na(lat) ~ paste0(formatC(round(as.numeric(lon),6), format='f', digits=6),",",formatC(round(as.numeric(lat),6), format='f', digits=6)),
        TRUE ~ as.character(row_number())
      )) %>%
      distinct(loc_id, .keep_all = TRUE) %>%
      left_join(site_market_counts, by = "site_name")
    
    df
  })
  filtered_FV <- reactive({
    df <- df_FV_file
    brand_sel <- if(is.null(input$brand_name)) "All" else input$brand_name
    crop_sel <- if(is.null(input$crop_year)) "All" else input$crop_year
    rm_sel <- if(is.null(input$RM_Recomme)) "All" else input$RM_Recomme
    if(brand_sel != "All") df <- filter(df, brand_name == brand_sel)
    if(crop_sel != "All") df <- filter(df, crop_year == crop_sel)
    # apply the same Rm_recomme filter to FV acreage data if selected
    if(rm_sel != "All" && "RM_Recomme" %in% names(df)){
      df <- filter(df, RM_Recomme == rm_sel)
    }
    df
  })

  # prepare counties polygons with acreage
  counties_sf <- reactive({
    cnts <- counties_base %>%
      mutate(GEOID = as.character(GEOID))
    fv <- filtered_FV()
    # ensure fv has the expected fips column
    if(!"fips" %in% names(fv) & "fips_code" %in% names(fv)) fv$fips <- sprintf("%05d", as.integer(fv$fips_code))
    cnts <- left_join(cnts, fv, by = c("GEOID" = "fips"))
    cnts
  })

  # Helper function to create county layer
  add_county_layer <- function(map, cnts) {
    if(!"sum_acreage" %in% names(cnts)) cnts$sum_acreage <- NA
    acreage_values <- cnts$sum_acreage[!is.na(cnts$sum_acreage)]
    if(length(acreage_values) == 0) {
      pal <- colorNumeric(palette = "YlOrRd", domain = c(0, 1), na.color = "transparent")
    } else {
      pal <- colorNumeric(palette = "YlOrRd", domain = cnts$sum_acreage, na.color = "transparent")
    }
    map %>%
      addPolygons(data = cnts,
                  group = "counties",
                  fillColor = ~pal(sum_acreage),
                  fillOpacity = 0.7,
                  color = "#444444",
                  weight = 0.2,
                  popup = ~paste0(NAME, ", ", STATEFP, "<br>Acreage: ", ifelse(is.na(sum_acreage), "0", sum_acreage)),
                  options = pathOptions(pane = "counties_pane")) %>%
      addLegend(position = "bottomright", pal = pal,
                values = cnts$sum_acreage,
                title = "Total Acreage",
                layerId = "acreage_legend")
  }
  
  # Helper function to create P4 markers
  add_p4_markers <- function(map, df) {
    if(!("lon" %in% names(df) && "lat" %in% names(df))) return(map)
    df <- df %>% filter(!is.na(lon) & !is.na(lat))
    if(nrow(df) == 0) return(map)
    
    df <- df %>% mutate(
      num_markets_safe = ifelse(is.na(num_markets), 1, num_markets),
      marker_color = case_when(
        num_markets_safe >= 5 ~ "#8B4513",
        num_markets_safe == 4 ~ "#FF69B4",
        num_markets_safe == 3 ~ "#9400D3",
        num_markets_safe == 2 ~ "#228B22",
        TRUE ~ "#0000CD"
      ),
      marker_radius = 2 + pmin(num_markets_safe, 5)
    )
    
    map %>%
      addCircleMarkers(data = df,
                       group = "p4_markers",
                       lng = ~lon,
                       lat = ~lat,
                       radius = ~marker_radius,
                       color = ~marker_color,
                       fillColor = ~marker_color,
                       stroke = TRUE,
                       weight = 1,
                       fillOpacity = 0.8,
                       popup = ~paste0(
                         "<b>Site:</b> ", site_name, "<br>",
                         "<b>Markets Served:</b> ", num_markets, "<br>",
                         "<span style='font-size:11px'>", markets_served, "</span><br>",
                         "<b>County RM:</b> ", RM_Recomme
                       ),
                       options = pathOptions(pane = "markers_pane")) %>%
      addLegend(position = "topright",
                colors = c("#0000CD", "#228B22", "#9400D3", "#FF69B4", "#8B4513"),
                labels = c("1 market", "2 markets", "3 markets", "4 markets", "5+ markets"),
                title = "P4 Sites by Markets",
                opacity = 0.8,
                layerId = "p4_legend")
  }

  # Initialize map with initial data
  output$map <- renderLeaflet({
    cnts <- isolate(counties_sf())
    p4_df <- isolate(filtered_p4())
    
    leaflet() %>%
      addProviderTiles(providers$CartoDB.Positron) %>%
      setView(lng = -95.7129, lat = 37.0902, zoom = 4) %>%
      addMapPane("counties_pane", zIndex = 410) %>%
      addMapPane("states_pane", zIndex = 420) %>%
      addMapPane("markers_pane", zIndex = 430) %>%
      add_county_layer(cnts) %>%
      addPolygons(data = states_base,
                  fill = FALSE,
                  color = "#000000",
                  weight = 1,
                  options = pathOptions(pane = "states_pane")) %>%
      add_p4_markers(p4_df)
  })
  
  # Update county polygons when FV filters change
  observeEvent(list(input$brand_name, input$crop_year, input$RM_Recomme), {
    cnts <- counties_sf()
    
    leafletProxy("map") %>%
      clearGroup("counties") %>%
      removeControl("acreage_legend") %>%
      add_county_layer(cnts)
  }, ignoreInit = TRUE)

  # Update P4 markers when filters change
  observeEvent(list(input$market, input$RM_Recomme), {
    df <- filtered_p4()
    
    leafletProxy("map") %>%
      clearGroup("p4_markers") %>%
      removeControl("p4_legend") %>%
      add_p4_markers(df)
  }, ignoreInit = TRUE)
}

shinyApp(ui, server)
