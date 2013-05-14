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

# Read in the main data file as a data frame, cater for a header row and comma 
# as separator
stockData <- read.table("stocksNumeric.csv", header=T, sep=",")

# Apply the stock codes to the numerical values, 
#  if Stock Codes change or are added/delete then 
#  the next line would need to be modified.
stockData$stock <- factor(as.factor(stockData$stock), 
    labels=c("AAPL","GOOG","ORCL","INTC","SYMC","FB","CSCO","XRX","IBM","MSFT"))

# TODO: Testing here, remove redundant before submission
#stockData[stockData$stock=="MSFT", ]
#mean(stockData[stockData$stock=="MSFT", 5])



# Identify stocks whose daily average gain, in a certain time period, is 
# higher than the overall average daily gain of the entire stock exchange in
# that time.


getAverages <- function() {
 stockNamesAsLevels <- levels(as.factor(stockData[,2]))
 lapply(stockNamesAsLevels, 
                    FUN=function(stock) {
                      mean(stockData[stockData[,2]==stock,5])
                    })
}

results <- getAverages()



