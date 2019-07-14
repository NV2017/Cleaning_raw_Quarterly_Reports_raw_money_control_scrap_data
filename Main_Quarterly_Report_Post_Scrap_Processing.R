#### Tool file to convert moneycontrol quarterly report scrapping into ML & Time series worthy !

# Author: Arunabha Sarkar, NISM, NCBS, Mumbai University, IISER-K
# Date: 13th July 2019

# This is the code to process primary quarterly reports data collected by my custom scraper.
# Custom scraper collected data from Python scrapper. As R was idle, this was thus made in R.

# The Python scrapper code can be found here: 
# https://github.com/NV2017/Money_Control_Quarterly_Report_Full_Time_Series_Scrap

# As of writing/executing/debugging this this, scrapper in Python is running in the background.

# This program utilises the function from the file 'Tools_Quarterly_Report_Post_Scrap_Processing.R'

# The 2 code files need to be in a folder called 'Main'
# The raw .csv files need to dumped into another folder 'Resource' in the same directory.

# Outout & Log folders are made by script on their own if not present (not overwritten)
# Output follows two formats, one in convenient transpose format, 
# another, it's transpose, ideal for time series format.

# Working directory to be set as that of folder containing 'Main' folder on line # 23

# Why was this written in R? Because my Python was busy scrapping the date :)

# For Github purpose, 2 raw files are supplied. They are:
## 'Adani Ports and Special Economic Zone Ltd. Q_Report_2019_07_08_14_25_50.csv'
## 'Asian Paints Ltd. Q_Report_2019_07_08_14_30_12.csv'

# Their respective output files (4) are also supplied as sample. They are:
## 'Adani Ports and Special Economic Z Q_Report_Accounting_Format_2019_07_13_17_21_48.csv'
## 'Asian Paints Q_Report_Accounting_Format_2019_07_13_17_21_49.csv'
## 'Adani Ports and Special Economic Z Q_Report_Time_Series_2019_07_13_17_21_48.csv'
## 'Asian Paints Q_Report_Time_Series_2019_07_13_17_21_49'

rm(list=ls())
Main_folder_path <- "~/QRS/Capstones/Project Zero Sigma Hedge/Web_Scrap/Moneycontrol/NIFTY50_raw_Quarterly_data_clean_20190708 - Pro"
setwd(Main_folder_path)
source(paste0(Main_folder_path,"/Main/","Tools_Quarterly_Report_Post_Scrap_Processing.R",sep=''))
Log_dataframe <- data.frame(Date_Time=character(), Log_Entry=character(), stringsAsFactors=F)
Establish_Data_plus_Folders <- Establish_Data_and_Folders(Main_folder_path,Log_dataframe)
Log_dataframe <- Establish_Data_plus_Folders[[2]]
if (Establish_Data_plus_Folders[[1]] == T)
{
  Log_Output <- Process_all_Quarterly_Reports_Raw_Files(Main_folder_path,Log_dataframe)

  Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                   Log_Entry = Log_Output, stringsAsFactors=F))
  
  Concluding_message <- c("All files processed. Nice to do business with you.")
  Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                   Log_Entry = Concluding_message, stringsAsFactors=F))
}else
{
  Error_message <- c("Encountered some unexpected error.")
  Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                   Log_Entry = Error_message, stringsAsFactors=F))
}
Write_and_Save_Log(Main_folder_path,Log_dataframe)
rm(list=ls())