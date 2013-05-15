#########################################
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

# TODO: 

# Notes from PDF:
# * Your solution should contain a textual description (ca. 1 page pdf) of your 
#   approach to the problem, including any assumptions you make.
# * Your code must be runnable, in other words it should be possible to source 
#   your script in a brand new R session with the data set in the working 
#   directory. If you use any R libraries (you may or may not), add a comment 
#   stating which functions you are using from that library.
# * You will get more marks if your code is readable, well-indented, and above 
#   all well-commented.
# * You will get more marks for appropriate use of the built-in functions of R 
#   and its libraries.
# Marks
# * Correctly solve the problem for the 90 day data: up to 20 points
# * Be able to limit the analysis to a certain number of days: 5 points
# * If your solution is parallelised: extra 20 points
# * If your solution uses out-of-memory data: extra 20 points

# TODO: comment out/delete next line before submission
setwd("C:/JB/Home/Docs/JobHunt/Courses/MSc/Programming for Big Data/R/rassess")


# Non Parallel Solution ---------------------------------------------------


# Load the required libraries
library(foreach)

# Read in the main data file as a data frame, cater for a header row and comma 
# as separator
stockData <- read.table("stocksNumeric.csv", header=T, sep=",")

allStockCodes <- 
  c("AAPL","GOOG","ORCL","INTC","SYMC","FB","CSCO","XRX","IBM","MSFT")

# Apply the stock codes to the numerical values, 
#  if Stock Codes change or are added/delete then 
#  the next line would need to be modified.
stockData$stock <- factor(as.factor(stockData$stock), labels=allStockCodes)

# TODO: Testing here, remove redundant before submission
#stockData[stockData$stock=="MSFT", ]
#mean(stockData[stockData$stock=="MSFT", 5])

# THE TASK
# Identify stocks whose daily average gain, in a certain time period, is 
# higher than the overall average daily gain of the entire stock exchange in
# that time.

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

mrktAvg <- getMarketAverage(stockData)

avgByStock <- getAveragesPerStock(stockData)

results <- foreach (i=1:length(avgByStock)) %do% {
  if (avgByStock[i] > mrktAvg) {
    allStockCodes[i]
  }
}

performingStocks <- results[results!='NULL']
performingStocks

# Parallel Solution ------------------------------------------------------



