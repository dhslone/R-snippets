# Raster convolution to migrate habitats
rm(list = ls())

setwd("~/R/Convolution") # set to appropriate directory to save rasters
tmp_dir <- tmpDir() # get R temporary raster directory; alternately specify a path

library(raster) # focal, raster
rasterOptions(maxmemory = 5e9) # 5gb of RAM allowed per file
rasterOptions(tmptime = 1) # 1 hour life for temp files

byr_pal <- colorRampPalette(c("gray80", "blue", "yellow", "red"))

# Test data ---------------------------------------------------------------
yr_table <- data.frame(
  years = c(2000, 2010, 2020, 2030, 2040), # first year is data, others model output periods
  H1_add = c(NA, 10, 11, 12, 13), # pixels per year to add to first habitat per period
  H2_add = c(NA, 10, 8, 8, 8)) # pixels per year to add to second habitat per period

loc <- "test_location"
habs <- c("Habitat1", "Habitat2", "Habitat3")
num_habs <- length(habs)

H1 <- raster(ncol = 100, nrow = 100, xmn = 0, xmx = 100, ymn = 0, ymx = 100)
values(H1) <- 0
H3 <- H2 <- H1
H1[6:40, 6:95] <- 1
H1[41:45, 41:60] <- 1
H2[41:50, 6:40] <- 1
H2[41:50, 71:95] <- 1
H3[51:95, 6:95] <- 1
H3[46:50, 41:60] <- 1

hab_stack <- stack(H1, H2, H3)
names(hab_stack) <- habs


# View rasters ------------------------------------------------------------
plot(hab_stack[[1]] + hab_stack[[2]] * 2 + hab_stack[[3]] * 3, col = byr_pal(100))
# gray is fixed habitat, blue-yellow-red are habitats 1-3 that are shifting
plot(hab_stack)
cellStats(hab_stack, "sum")


# Save initial habitat layers ---------------------------------------------
writeRaster(hab_stack,
  filename = paste0(loc, "_", habs, "_", yr_table$years[1], ".tif"),
  bylayer = TRUE, format = "GTiff", overwrite = TRUE, options = c("TFW=YES")
)

# Make filter kernel ------------------------------------------------------
# Exact equivalent to ArcMap Focal Stastistics Circle 3
w <- matrix(data = c(
  0, 0, 0, 1, 0, 0, 0,
  0, 1, 1, 1, 1, 1, 0,
  0, 1, 1, 1, 1, 1, 0,
  1, 1, 1, 1, 1, 1, 1,
  0, 1, 1, 1, 1, 1, 0,
  0, 1, 1, 1, 1, 1, 0,
  0, 0, 0, 1, 0, 0, 0
), nrow = 7, ncol = 7, byrow = TRUE)
wpad <- floor(nrow(w) / 2)
w <- w / sum(w) # make it a PDF

# pt <- proc.time() # start timer
# Time loop ---------------------------------------------------------------
for (yr in 2:(nrow(yr_table))) { # This will run the entire set of years
  #  for (yr in 2:3) { # This will run the first 2 time periods (change this to manually run from table)

  # create unique filepath for temp directory
  dir.create(file.path(paste0(tmp_dir, "timeloop/")), showWarnings = FALSE)
  # set temp directory
  rasterOptions(tmpdir = file.path(paste0(tmp_dir, "timeloop/")))

  sy <- yr_table$years[yr - 1] # start year of this portion
  ey <- yr_table$years[yr] # end year of this portion
  message("working on ", sy, " - ", ey)
  # reloading the stack each time step because it may be wiped out later with temp files
  hab_stack <- stack()
  for (i in 1:num_habs) {
    hab_stack <- stack(hab_stack, raster(paste0(loc, "_", habs[i], "_", sy, ".tif")))
  }

  # Habitat loop ------------------------------------------------------------
  for (hab in (num_habs - 1):1) { # Run from top down
    px_needed <- yr_table[yr, hab + 1] * (ey - sy) # how many pixels are needed?
    if (hab == 1) {
      from_hab <- hab_stack[[hab]] # from_hab is manipulated in the loop
    } else {
      from_hab <- hab_stack[[hab]] + hab_stack[[hab - 1]] # add the habitat below
    }

    # Grow Loop  --------------------------------------------------------------
    px_comp <- cellStats(hab_stack[[hab + 1]], "sum") / px_needed # needs to be >= 1
    try(if (px_comp < 1) stop(paste0("Not enough ", habs[hab + 1], " available!")))
    while (px_comp > 1) {
      # Grow source habitat until new pixels > required pixels
      from_hab2 <- focal(from_hab, w, pad = TRUE, padValue = wpad, fun = sum) # Apply kernel one time

      # Trim growth to new habitat
      from_hab_pix <- from_hab2 * hab_stack[[hab + 1]]

      # compare new habitat to needed habitat
      new_px <- cellStats(from_hab_pix, "sum")
      px_comp <- px_needed / new_px
      from_hab <- from_hab + from_hab_pix
    } # End Grow Loop

    # Scale new pixels to match required number
    from_hab_pix <- from_hab_pix * px_comp

    # Add in new pixels to source habitat
    hab_stack[[hab]] <- hab_stack[[hab]] + from_hab_pix

    # Subtract new pixels from new habitat
    hab_stack[[hab + 1]] <- hab_stack[[hab + 1]] - from_hab_pix

    # Trim to 0-1 (eliminates slight computer rounding errors)
    for (m in 0:1) {
      message(min(hab_stack[[hab + m]]@data@values, na.rm = TRUE)) # uncomment If you want to check the values before scaling
      message(max(hab_stack[[hab + m]]@data@values, na.rm = TRUE))
      hab_stack[[hab + m]]@data@values[hab_stack[[hab + m]]@data@values < 0] <- 0
      hab_stack[[hab + m]]@data@values[hab_stack[[hab + m]]@data@values > 1] <- 1
    }
  } # End Habitat Loop

  # Save habitat layers -----------------------------------------------------
  # you can also keep the stack layers together if desired
  names(hab_stack) <- habs
  writeRaster(hab_stack,
    filename = paste0(loc, "_", habs, "_", ey, ".tif"),
    bylayer = TRUE, format = "GTiff", overwrite = TRUE,
    options = c("TFW=YES")
  )

  # remove temp directory and files to immediately recover HDD space
  # comment this out before running the script if you want to view layers (below)
  unlink(file.path(paste0(tmp_dir, "timeloop/")), recursive = TRUE)
} # End Years loop
# pt2 <- proc.time() # end timer

# pt2-pt # how long did it take?
#####################################################################

# View rasters ------------------------------------------------------------
plot(hab_stack[[1]] + hab_stack[[2]] * 2 + hab_stack[[3]] * 3, col = byr_pal(100))
plot(hab_stack)
cellStats(hab_stack, "sum")
