
library(shiny)
library(leaflet)
library(sf)
library(dplyr)
library(tigris)
library(DT)
library(arrow)  # For reading Parquet files

options(tigris_use_cache = TRUE)

df_p4_file <- as.data.frame(read_parquet("data/NA_market_P4_breeding_locationsII.parquet"))
# normalize coordinates same as below so unique-ification works
if("centroid_lon" %in% names(df_p4_file)) df_p4_file$lon <- as.numeric(df_p4_file$centroid_lon)
if("centroid_lat" %in% names(df_p4_file)) df_p4_file$lat <- as.numeric(df_p4_file$centroid_lat)
if("centroide_lan" %in% names(df_p4_file)) df_p4_file$lat <- as.numeric(df_p4_file$centroide_lan)
if(!("lon" %in% names(df_p4_file)) && ("long" %in% names(df_p4_file))) df_p4_file$lon <- as.numeric(df_p4_file$long)
if(!("lat" %in% names(df_p4_file)) && ("latitude" %in% names(df_p4_file))) df_p4_file$lat <- as.numeric(df_p4_file$latitude)

# derive a table of unique p4 locations (site_name or lon/lat) - used for mapping regardless of market
p4_unique <- df_p4_file %>%
  mutate(loc_id = case_when(
    "site_name" %in% names(.) & !is.na(site_name) & site_name != "" ~ as.character(site_name),
    all(c("lon","lat") %in% names(.)) & !is.na(lon) & !is.na(lat) ~ paste0(formatC(round(as.numeric(lon),6), format='f', digits=6),",",formatC(round(as.numeric(lat),6), format='f', digits=6)),
    TRUE ~ as.character(row_number())
  )) %>%
  distinct(loc_id, .keep_all = TRUE)

# rename incoming column to match UI/filter

df_FV_file <- as.data.frame(read_parquet("data/FV_brand_acreage.parquet"))

# Static brand counts no longer needed - will be reactive

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
      uiOutput("brand_name_ui"),
      selectInput("crop_year", "Crop Year:",
                  choices = c("All", sort(unique(df_FV_file$crop_year))),
                  selected = "All"),
      hr(),
      h4("P4 Locations 2025"),
      uiOutput("market_ui"),
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
  
  # dynamic Brand Name UI
  output$brand_name_ui <- renderUI({
    counts <- reactive_brand_counts()
    current_selection <- if(is.null(input$brand_name)) "All" else input$brand_name
    if(is.null(counts) || nrow(counts) == 0){
      selectInput("brand_name", "Brand Name:", choices = c("All" = "All"), selected = current_selection)
    } else {
      choices <- setNames(counts$brand_name, paste0(counts$brand_name, " (", counts$n, ")"))
      selectInput("brand_name", "Brand Name:", choices = c("All" = "All", choices), selected = current_selection)
    }
  })
  
  # dynamic Market UI with location counts
  output$market_ui <- renderUI({
    counts <- reactive_market_counts()
    current <- if(is.null(input$market)) "All" else input$market
    if(is.null(counts) || nrow(counts) == 0){
      selectInput("market", "Market:", choices = c("All" = "All"), selected = current)
    } else {
      choices <- setNames(counts$market, paste0(counts$market, " (", counts$n, ")"))
      selectInput("market", "Market:", choices = c("All" = "All", choices), selected = current)
    }
  })
  
  # reactive filtered tables
  filtered_p4 <- reactive({
    # always work from the pre-deduplicated set
    df <- p4_unique
    market_sel <- if(is.null(input$market)) "All" else input$market
    rm_sel <- if(is.null(input$RM_Recomme)) "All" else input$RM_Recomme
    if(market_sel != "All") df <- filter(df, market == market_sel)
    if(rm_sel != "All") df <- filter(df, RM_Recomme == rm_sel)
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
    cnts <- counties(cb = TRUE, year = 2022, class = "sf") %>%
      mutate(GEOID = as.character(GEOID))
    fv <- filtered_FV()
    # ensure fv has the expected fips column
    if(!"fips" %in% names(fv) & "fips_code" %in% names(fv)) fv$fips <- sprintf("%05d", as.integer(fv$fips_code))
    cnts <- left_join(cnts, fv, by = c("GEOID" = "fips"))
    cnts
  })

  output$map <- renderLeaflet({
    cnts <- counties_sf()
    # if sum_acreage missing, use 0
    if(!"sum_acreage" %in% names(cnts)) cnts$sum_acreage <- NA
    pal <- colorNumeric(palette = "YlOrRd", domain = cnts$sum_acreage, na.color = "transparent")
    
    # Get state boundaries for thicker lines
    states_sf <- states(cb = TRUE, year = 2022, class = "sf")

    leaflet() %>%
      addProviderTiles(providers$CartoDB.Positron) %>%
      setView(lng = -95.7129, lat = 37.0902, zoom = 4) %>%
      addPolygons(data = cnts,
                  fillColor = ~pal(sum_acreage),
                  fillOpacity = 0.7,
                  color = "#444444",
                  weight = 0.2,
                  popup = ~paste0(NAME, ", ", STATEFP, "<br>Acreage: ", ifelse(is.na(sum_acreage), "0", sum_acreage))) %>%
      addPolygons(data = states_sf,
                  fill = FALSE,
                  color = "#000000",
                  weight = 1) %>%
      addLegend(position = "bottomright", pal = pal,
                values = cnts$sum_acreage,
                title = "Total Acreage")
  })

  # update map when filters change to add points
  observe({
    df <- filtered_p4()
    # ensure lon/lat exist
    if(!("lon" %in% names(df) && "lat" %in% names(df))){
      return()
    }

    df <- df %>% filter(!is.na(lon) & !is.na(lat))
    
    leafletProxy("map") %>%
      clearMarkers() %>%
      addCircleMarkers(data = df,
                       lng = ~lon,
                       lat = ~lat,
                       radius = 2,
                       color = "blue",
                       fill = TRUE,
                       fillOpacity = 0.8,
                       popup = ~paste0("Market: ", market, "<br>County RM: ", RM_Recomme))
  })
}

shinyApp(ui, server)
