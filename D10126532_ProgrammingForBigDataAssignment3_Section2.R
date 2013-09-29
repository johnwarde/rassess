# Identificaiton ########################
#
# Student ID:   D10126532
# Student Name: John Warde
# Course Code:  DT230B
# 
# Programming for Big Data Assignment 3
# 
# Section 2 Stock Performance
# 
#########################################
# Set-up ------------------------------------------------------------------


# Load the required libraries
library(foreach, quietly = TRUE)


# Non Parallel Solution ---------------------------------------------------

cat("Using Non-parallel solution ... \n")


# Read in the main data file as a data frame, cater for a header row and comma 
# as separator
stockData <- read.table("stocksNumeric.csv", header=T, sep=",")

allStockCodes <- 
  c("AAPL","GOOG","ORCL","INTC","SYMC","FB","CSCO","XRX","IBM","MSFT")

# Apply the stock codes to the numerical values, 
#  if Stock Codes change or are added/delete then 
#  the next line would need to be modified.
stockData$stock <- factor(as.factor(stockData$stock), labels=allStockCodes)

# Get the average gain for all stocks in supplied data 
# for day for 1 to lastNdays
getMarketAverage <- function(thisStockData, lastNdays = 90) {
  # Filter the gain data (column 5) that has a day number in the range of
  # 1 to lastNdays and get the average
  mean(thisStockData[thisStockData$day==1:lastNdays,5])
}

# Return the average for each stock contained in the stock data for the last
# 1 to N days
getAveragesPerStock <- function(dfStock, lastNdays = 90) {
  # Determine the stock codes from supplied data
  stockNamesAsLevels <- levels(as.factor(dfStock$stock))
  # Iterate over the different stocks to get the different averages
  lapply(stockNamesAsLevels, 
          FUN=function(thisStock, dfStockForLoop=dfStock,endRange=lastNdays) {
            # First filter the data for the current stock code
            dfForStock <- dfStockForLoop[dfStockForLoop$stock==thisStock,]
            # Then get the average for the specified day range
            mean(dfForStock[dfForStock$day==1:endRange, 5])
          })
}

# Determine the performing stocks by calculating which stocks out perform the
# average of the entire stock portfolio data over the last N days
performingStocks <- function(stockData, allStockCodes, lastNDays = 90) {
  # Get the average for all stocks
  mrktAvg <- getMarketAverage(stockData, lastNDays)
  # Get averages of all stock codes
  avgByStock <- getAveragesPerStock(stockData, lastNDays)
  # Loop through to see which stock are performing better than the average
  results <- foreach (i=1:length(avgByStock)) %do% {
    if (avgByStock[i] > mrktAvg) {
      allStockCodes[i]
    }
  }
  # Filter out the NULLs (under performing stocks from the list to leave 
  # only the performing stocks codes
  results[results!='NULL']
}

# Inform the user
cat("The peforming stocks are: \n", paste(
  performingStocks(stockData, allStockCodes, 30)),"\n")



# Parallel Solution ------------------------------------------------------

cat("Using Parallel solution ... \n")

# Load the parallel library 
library(parallel, quietly = TRUE)
# Load the Simple Network of Worstations library for parallelisation 
library(doSNOW, quietly = TRUE)

# Make a cluster with one less than the number of logical processors on this
# machine to prevent lockups
clu <- makeCluster(detectCores() - 1)

# Register the new cluster
registerDoSNOW(clu)

# Return the average for each stock contained in the stock data for the last
# 1 to N days
getAveragesPerStock <- function(dfStock, lastNdays = 90) {
  # Determine the stock codes from supplied data
  stockNamesAsLevels <- levels(as.factor(dfStock$stock))
  # Iterate over the different stocks to get the different averages
  foreach (i=1:length(stockNamesAsLevels)) %dopar% {
      # First filter the data for the current stock code
      dfForStock <- dfStock[dfStock$stock==stockNamesAsLevels[i],]
      # Then get the average for the specified day range
      mean(dfForStock[dfForStock$day==1:lastNdays, 5])
  }
}

# Determine the performing stocks by calculating which stocks out perform the
# average of the entire stock portfolio data over the last N days
performingStocks <- function(stockData, allStockCodes, lastNDays = 90) {
  # Get the average for all stocks
  mrktAvg <- getMarketAverage(stockData, lastNDays)
  # Get averages of all stock codes
  avgByStock <- getAveragesPerStock(stockData, lastNDays)
  # Loop through to see which stock are performing better than the average
  results <- foreach (i=1:length(avgByStock)) %dopar% {
    if (avgByStock[i] > mrktAvg) {
      allStockCodes[i]
    }
  }
  # Filter out the NULLs (under performing stocks) from the list to leave 
  # only the performing stocks codes
  results[results!='NULL']
}

# Inform the user
cat("The peforming stocks are: \n", paste(
  performingStocks(stockData, allStockCodes, 30)),"\n")

# Release the cluster resources
stopCluster(clu)


# Out-of-Memory Solution ------------------------------------------------------

# Create a new cluster and register it
clu <- makeCluster(detectCores() - 1)
registerDoSNOW(clu)

cat("Using Out-of-Memory solution (SQLite) ... \n")

library(RSQLite, quietly = TRUE)

StockDbName <- "stocks.sqlite"

# Get the average gain for all stocks in supplied data 
# for day for 1 to lastNdays
getMarketAverage <- function(StockDbName, lastNdays = 90) {
  # Connect to the database
  dbCon <- dbConnect("SQLite", dbname=StockDbName)

  # How many observations are we working with?
  dbResult <- dbGetQuery(dbCon, "SELECT COUNT(gain) FROM stock_gains")
  totalRows <- dbResult[1,1]
  
  # Filter the gain data that has a day number in the range of
  # 1 to lastNdays and get the average, using ORDER BY clause in the SQL
  # statment in the hope that the database won't need to scan the entire
  # table when the lastNdays is less than the default
  SQL <- sprintf("SELECT gain FROM stock_gains WHERE day <= %d ORDER BY day", 
                 lastNdays)
  dbResults <- dbSendQuery(dbCon, SQL)
  dbRows <- fetch(dbResults, n=totalRows)
  result <- sapply(dbRows, mean)
  dbClearResult(dbResults)
  dbDisconnect(dbCon)
  result
}



# Return the average for each stock contained in the stock data for the last
# 1 to N days
getPerformingStocks <- function(StockDbName, mrktAvg, lastNdays = 90) {  
  # Connect to the database
  dbCon <- dbConnect("SQLite", dbname=StockDbName)
  
  dbResult <- dbGetQuery(dbCon, "SELECT DISTINCT stock FROM stock_gains")
  stockNames <- simplify2array(dbResult)
  dbDisconnect(dbCon)  
  allResults <-foreach (i=1:length(stockNames)) %dopar% {
    # We need to load the libray here becuase it is in a different process
    library(RSQLite, quietly = TRUE)
    dbCon <- dbConnect("SQLite", dbname=StockDbName)
    SQL <- sprintf(
      "SELECT gain FROM stock_gains WHERE stock = '%s' AND day <= %d ORDER BY day", 
      stockNames[i], lastNdays)
    dbResults <- dbSendQuery(dbCon, SQL)
    dbRows <- fetch(dbResults, n=dbGetRowCount(dbResults))
    result <- sapply(dbRows, mean)
    dbClearResult(dbResults)
    dbDisconnect(dbCon)
    if (result > mrktAvg) {
      stockNames[i]    
    }
  }
  allResults[allResults!='NULL']
}

lastNDays <- 30
# Inform the user
cat("The peforming stocks are: \n", paste(
  getPerformingStocks(StockDbName, 
                      getMarketAverage(StockDbName, lastNDays), lastNDays)),"\n")


stopCluster(clu)
