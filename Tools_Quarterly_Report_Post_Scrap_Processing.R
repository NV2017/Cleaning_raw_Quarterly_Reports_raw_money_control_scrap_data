#### Tool file to convert moneycontrol quarterly report scrapping into ML & Time series worthy !

# Author: Arunabha Sarkar, NISM, NCBS, Mumbai University, IISER-K
# Date: 13th July 2019

# This is function containing file for 'Main_Quarterly_Report_Post_Scrap_Processing.R'

# The 2 code files need to be in a folder called 'Main'
# The raw .csv files need to dumped into another folder 'Resource' in the same directory.

# Outout & Log folders are made by script on their own if not present (not overwritten)
# Output follows two formats, one in convenient transpose format, 
# another, it's transpose, ideal for time series format.

# Why was this written in R? Because my Python was busy scrapping the date :)

# The Python scrapper code can be found here: 
# https://github.com/NV2017/Money_Control_Quarterly_Report_Full_Time_Series_Scrap

#Main_folder_path <- "~/QRS/Capstones/Project Zero Sigma Hedge/Web_Scrap/Moneycontrol/NIFTY50_raw_Quarterly_data_clean_20190708 - Pro"

# For Github purpose, 2 raw files are supplied. They are:
## 'Adani Ports and Special Economic Zone Ltd. Q_Report_2019_07_08_14_25_50.csv'
## 'Asian Paints Ltd. Q_Report_2019_07_08_14_30_12.csv'

# Their respective output files (4) are also supplied as sample. They are:
## 'Adani Ports and Special Economic Z Q_Report_Accounting_Format_2019_07_13_17_21_48.csv'
## 'Asian Paints Q_Report_Accounting_Format_2019_07_13_17_21_49.csv'
## 'Adani Ports and Special Economic Z Q_Report_Time_Series_2019_07_13_17_21_48.csv'
## 'Asian Paints Q_Report_Time_Series_2019_07_13_17_21_49'


####################################################################################
####################################################################################
################################ FUNCTION START ####################################

# This function makes all the required folders for the main program, if required, no overwriting.
# This also checks if 'Resource' folder has data to be processed or not !

Establish_Data_and_Folders <- function(Main_folder_path, Log_dataframe) 
{
  Directories <- dir(path = Main_folder_path)
  Check_Resource_folder <- which("Resource" == Directories)
  if (length(Check_Resource_folder) == 0)
  {
    No_resource_folder_output <- c("Can't detect any resource folder, aborting...")
    print(No_resource_folder_output)
    Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                      Log_Entry = No_resource_folder_output, stringsAsFactors=F))
    return(list(F, Log_dataframe))
  }else
  {
    Resource_folder_output <- c("Resource folder found.")
    print(Resource_folder_output)
    Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                     Log_Entry = Resource_folder_output, stringsAsFactors=F))
    
    Check_Log_folder <- which("Log" == Directories)
    if (length(Check_Log_folder) == 0)
    {
      # Making Log Folder
      Make_Log_Folder <- c("Making 'Log' folder.")
      print(Make_Log_Folder)
      Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                       Log_Entry = Make_Log_Folder, stringsAsFactors=F))
      mainDir <- Main_folder_path
      subDir <- c("Log")
      dir.create(file.path(mainDir, subDir))
    }else
    {
      Log_Folder_Present <- c("'Log' folder found.")
      print(Log_Folder_Present)
      Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                       Log_Entry = Log_Folder_Present, stringsAsFactors=F))
    }
    
    
    
    Check_Output_folder <- which("Output" == Directories)
    if (length(Check_Output_folder) == 0)
    {
      # Making Output Folder
      Make_Output_Folder <- c("Making 'Output' folder.")
      print(Make_Output_Folder)
      Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                       Log_Entry = Make_Output_Folder, stringsAsFactors=F))
      mainDir <- Main_folder_path
      subDir <- c("Output")
      dir.create(file.path(mainDir, subDir))
      
      Sys.sleep(2)
      
      # Making Accounting_Format Folder
      Make_Accounting_Format_Folder <- c("Making 'Accounting_Format' folder.")
      print(Make_Accounting_Format_Folder)
      Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                       Log_Entry = Make_Accounting_Format_Folder, stringsAsFactors=F))
      mainDir2 <- paste0(Main_folder_path,'/','Output',sep='')
      subDir2 <- c("Accounting_Format")
      dir.create(file.path(mainDir2, subDir2))
      
      # Making Time_Series Folder
      Make_Time_Series_Folder <- c("Making 'Time_Series' folder.")
      print(Make_Time_Series_Folder)
      Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                       Log_Entry = Make_Time_Series_Folder, stringsAsFactors=F))
      mainDir3 <- paste0(Main_folder_path,'/','Output',sep='')
      subDir3 <- c("Time_Series")
      dir.create(file.path(mainDir3, subDir3))
    }else
    {
      Output_Folder_Present <- c("'Output' folder found.")
      print(Output_Folder_Present)
      Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                       Log_Entry = Output_Folder_Present, stringsAsFactors=F))
      
      Output_Directory <- dir(path = paste0(Main_folder_path,'/Output',sep=''))
      
      Check_Accounting_Format_Output_folder <- which("Accounting_Format" == Output_Directory)
      if (length(Check_Accounting_Format_Output_folder) == 0)
      {
        # Making Accounting_Format Folder
        Make_Accounting_Format_Folder <- c("Making 'Accounting_Format' folder.")
        print(Make_Accounting_Format_Folder)
        Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                         Log_Entry = Make_Accounting_Format_Folder, stringsAsFactors=F))
        mainDir4 <- paste0(Main_folder_path,'/','Output',sep='')
        subDir4 <- c("Accounting_Format")
        dir.create(file.path(mainDir4, subDir4))
      }else
      {
        Accounting_Format_Folder_Present <- c("'Accounting_Format' folder found.")
        print(Accounting_Format_Folder_Present)
        Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                         Log_Entry = Accounting_Format_Folder_Present, stringsAsFactors=F))
      }
        
        
      Check_Time_Series_Output_folder <- which("Time_Series" == Output_Directory)
      if (length(Check_Time_Series_Output_folder) == 0)
      {
        # Making Time_Series Folder
        Make_Time_Series_Folder <- c("Making 'Time_Series' folder.")
        print(Make_Time_Series_Folder)
        Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                         Log_Entry = Make_Time_Series_Folder, stringsAsFactors=F))
        mainDir5 <- paste0(Main_folder_path,'/','Output',sep='')
        subDir5 <- c("Time_Series")
        dir.create(file.path(mainDir5, subDir5))
      }else
      {
        Time_Series_Folder_Present <- c("'Time_Series' folder found.")
        print(Time_Series_Folder_Present)
        Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                         Log_Entry = Time_Series_Folder_Present, stringsAsFactors=F))
      }
    }
    return(list(T, Log_dataframe))
  }
}

################################# FUNCTION END #####################################
####################################################################################


####################################################################################
####################################################################################
################################ FUNCTION START ####################################

# At the end of run of the program, this function will save all the step by step output.
# Will save it as a .csv with date & time in the 'Log' folder

Write_and_Save_Log <- function(Main_folder_path,Log_dataframe)
{
  Log_of_Log <- c("Saving Log Output.")
  print(Log_of_Log)
  Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                   Log_Entry = Log_of_Log, stringsAsFactors=F))
  
  Exiting_Processing_Program <- c("Exiting Processing Program.")
  print(Exiting_Processing_Program)
  Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                   Log_Entry = Exiting_Processing_Program, stringsAsFactors=F))
  
  Log_Output_filename <- paste0('Log_',Sys.time(),'.csv',sep = '')
  Log_Output_filename <- gsub(pattern='-', replacement='_', x = Log_Output_filename)
  Log_Output_filename <- gsub(pattern=':', replacement='_', x = Log_Output_filename)
  Log_Output_filename <- gsub(pattern=' ', replacement='_', x = Log_Output_filename)
  
  write.csv(x = Log_dataframe, file = paste0(Main_folder_path,'/Log/',
                Log_Output_filename, sep = ''),col.names = T,row.names = T,sep=',')

  print('----x----x----x----x----x----x----')
}

################################# FUNCTION END #####################################
####################################################################################


####################################################################################
####################################################################################
################################ FUNCTION START ####################################

# This function starts processing all the files in the 'Reesource' folder one by one

Process_all_Quarterly_Reports_Raw_Files <- function(Main_folder_path,Log_dataframe)
{
  path_resource <- paste0(Main_folder_path,'/Resource',sep='')
  path_output_accounting_format <- paste0(getwd(),'/Output/Accounting_Format',sep='')
  path_output_time_series_format <- paste0(getwd(),'/Output/Time_Series',sep='')
  path_log <- paste0(getwd(),'/Log',sep='')
  
  Resource_files <- list.files(path = path_resource, pattern = 'Q_Report')
  
  if (length(Resource_files) != 0)
  {
    Found_Quarterly_Report_Raw_Data <- c("Found quarterly report raw data.")
    print(Found_Quarterly_Report_Raw_Data)
    Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                     Log_Entry = Found_Quarterly_Report_Raw_Data, stringsAsFactors=F))
    Log_Processing <- Processing_Files_One_by_One(Resource_files,path_resource,
                      Log_dataframe,path_output_accounting_format,path_log,
                      path_output_time_series_format)
    Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                     Log_Entry = Log_Processing, stringsAsFactors=F))
    All_processing_of_raw_data_done <- c("All processing of raw data done.")
    print(All_processing_of_raw_data_done)
    Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                     Log_Entry = All_processing_of_raw_data_done, stringsAsFactors=F))
  }else
  {
    Not_Found_Quarterly_Report_Raw_Data <- c("No quarterly report raw data. Nothing to do.")
    print(Not_Found_Quarterly_Report_Raw_Data)
    Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                     Log_Entry = Not_Found_Quarterly_Report_Raw_Data, stringsAsFactors=F))
  }
  return(Log_dataframe)
}

################################# FUNCTION END #####################################
####################################################################################



####################################################################################
####################################################################################
################################ FUNCTION START ####################################

# This function is the processing of each file from the parent function 'Process_all_Quarterly_Reports_Raw_Files'

Processing_Files_One_by_One <- function(Resource_files,path_resource,
                            Log_dataframe,path_output_accounting_format,path_log,
                            path_output_time_series_format)
{
  # Running over each file
  for (i in seq(1,length(Resource_files)))
  {
    temp_file <- read.csv(file=paste0(path_resource,'/',Resource_files[i],sep=''),
                          header = F,stringsAsFactors = F)
    
    temp_company <- strsplit(Resource_files[i], split = ' Q_Report')[[1]][1]
    temp_company <- gsub(pattern=" Ltd.",replacement="",x=temp_company)
    
    # Finding columns with word 'category' in first row for the dataframe
    # This marks the starting position of one moneycontrol page collection
    
    start_page_positions <- which("Category" == temp_file[1,])
    
    end_page_positions <- as.integer(c(c(as.character(start_page_positions[-1]-1)),
                                       c(as.character(dim(temp_file)[2]))))
    
    # Now, subsetting and storing in a list
    
    page_wise_data <- list()
    
    for (j in seq(1,length(start_page_positions)))
    {
      page_wise_data[[j]] <- temp_file[,c(start_page_positions[j]:end_page_positions[j])]
    }
    
    # Now cleaning the dataframes & finding all the unique row names
    
    all_the_unique_row_names = c()
    all_the_unique_column_names = c()
    
    Temp_Output <- Cleaning_Dataframes_and_Finding_unique_rows_columns(page_wise_data,
                   all_the_unique_column_names, all_the_unique_row_names)
    
    page_wise_data <- Temp_Output[[1]]
    all_the_unique_column_names <- Temp_Output[[2]]
    all_the_unique_row_names <- Temp_Output[[3]]
    
    # temp_company raw data processed
    temp_company_raw_data_processed <- paste0(temp_company,' raw data processed.', sep = '')
    print(temp_company_raw_data_processed)
    Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                     Log_Entry = temp_company_raw_data_processed, stringsAsFactors=F))
    
    # Making Output_company_dataframe
    
    Log_temp <- Making_Output_company_dataframe(Log_dataframe,all_the_unique_column_names,
                     all_the_unique_row_names,page_wise_data,temp_company,
                     path_output_accounting_format,path_output_time_series_format)
    
    Log_dataframe <- rbind(Log_dataframe, Log_temp)
    
  }
  return(Log_dataframe)
}

################################# FUNCTION END #####################################
####################################################################################



####################################################################################
####################################################################################
################################ FUNCTION START ####################################

# This function linked from 'Processing_Files_One_by_One', makes unique row names of the accounting names
# Returns all the unique column and row names required for the particular file

Cleaning_Dataframes_and_Finding_unique_rows_columns <- function(page_wise_data,
                    all_the_unique_column_names, all_the_unique_row_names)
{
  for (k in seq(1,length(page_wise_data)))
  {
    all_the_row_names <- c()
    all_the_column_names <- c()
    
    temp_sub_dataframe <- page_wise_data[[k]]
    temp_sub_dataframe[temp_sub_dataframe==""] <- NA
    temp_sub_dataframe[temp_sub_dataframe=="--"] <- NA
    temp_sub_dataframe <- temp_sub_dataframe[rowSums(is.na(temp_sub_dataframe)) != dim(temp_sub_dataframe)[2],]
    
    temp_sub_dataframe <- temp_sub_dataframe[!grepl("Notes", temp_sub_dataframe[,1]),]
    temp_sub_dataframe <- temp_sub_dataframe[!grepl("Source :", temp_sub_dataframe[,1]),]
    
    Basic_EPS_row_number <- which("Basic EPS" == temp_sub_dataframe[,1])
    if(length(Basic_EPS_row_number) != 0)
    {
      temp_sub_dataframe[Basic_EPS_row_number[1],1] <- c("Basic EPS- EPS Before Extra Ordinary")
      temp_sub_dataframe[Basic_EPS_row_number[2],1] <- c("Basic EPS- EPS After Extra Ordinary")
    }
    
    Diluted_EPS_row_number <- which("Diluted EPS" == temp_sub_dataframe[,1])
    if(length(Diluted_EPS_row_number) != 0)
    {
      temp_sub_dataframe[Diluted_EPS_row_number[1],1] <- c("Diluted EPS- EPS Before Extra Ordinary")
      temp_sub_dataframe[Diluted_EPS_row_number[2],1] <- c("Diluted EPS- EPS After Extra Ordinary")
    }
    
    Number_of_shares_row_number = which("- Number of shares (Crores)" == temp_sub_dataframe[,1])
    if(length(Number_of_shares_row_number) != 0 )
    {
      temp_sub_dataframe[Number_of_shares_row_number[1],1] <- c("- Number of shares (Crores)- Pledged_Encumbered")
      temp_sub_dataframe[Number_of_shares_row_number[2],1] <- c("- Number of shares (Crores)- Non_encumbered")
    }
    
    Percent_share_promoter_row_number = which("- Per. of shares (as a % of the total sh. of prom. and promoter group)" == temp_sub_dataframe[,1])
    if(length(Percent_share_promoter_row_number) != 0 )
    {
      temp_sub_dataframe[Percent_share_promoter_row_number[1],1] <- c("- Per. of shares (as a % of the total sh. of prom. and promoter group)- Pledged_Encumbered")
      temp_sub_dataframe[Percent_share_promoter_row_number[2],1] <- c("- Per. of shares (as a % of the total sh. of prom. and promoter group)- Non_encumbered")
    }
    
    Percent_share_total_row_number = which("- Per. of shares (as a % of the total Share Cap. of the company)" == temp_sub_dataframe[,1])
    if(length(Percent_share_total_row_number) != 0 )
    {
      temp_sub_dataframe[Percent_share_total_row_number[1],1] <- c("- Per. of shares (as a % of the total Share Cap. of the company)- Pledged_Encumbered")
      temp_sub_dataframe[Percent_share_total_row_number[2],1] <- c("- Per. of shares (as a % of the total Share Cap. of the company)- Non_encumbered")
    }
    
    all_the_column_names <- as.character(temp_sub_dataframe[1,])
    all_the_unique_column_names <- c(all_the_unique_column_names, all_the_column_names[-1])
    colnames(temp_sub_dataframe) <- all_the_column_names
    
    temp_sub_dataframe <- temp_sub_dataframe[-1,]
    
    all_the_row_names <- as.character(temp_sub_dataframe[,1])

    all_the_unique_row_names <- c(all_the_unique_row_names,all_the_row_names)
    rownames(temp_sub_dataframe) <- all_the_row_names
    
    temp_sub_dataframe <- data.frame(temp_sub_dataframe[,-1],
                                     row.names = all_the_row_names)
    
    colnames(temp_sub_dataframe) <- all_the_column_names[-1]
    
    page_wise_data[[k]] <- temp_sub_dataframe
  }
  all_the_unique_row_names <- unique(all_the_unique_row_names)
  all_the_unique_column_names <- unique(all_the_unique_column_names)

  return(list(page_wise_data,all_the_unique_column_names, all_the_unique_row_names))
}
################################# FUNCTION END #####################################
####################################################################################


####################################################################################
####################################################################################
################################ FUNCTION START ####################################

# This function functions like VLOOKUP of Excel, this collates all the Quarterly reports
# data from different pages of moneycontrol to a single dataframe. 
# All rows and columns are kept track of. No loss and duplication made sure.

Making_Output_company_dataframe <- function(Log_dataframe,all_the_unique_column_names,
                                   all_the_unique_row_names,page_wise_data,temp_company,
                                   path_output_accounting_format,path_output_time_series_format)
{
  Output_company_dataframe <- data.frame(matrix("", ncol = length(all_the_unique_column_names), 
                              nrow = length(all_the_unique_row_names)), stringsAsFactors = F)
  colnames(Output_company_dataframe) <- all_the_unique_column_names
  row.names(Output_company_dataframe) <- all_the_unique_row_names
  
  Data_Time_Stack <- dim(page_wise_data[[1]])[2]
  for (l in seq(2,length(page_wise_data)))
  {
    Data_Time_Stack <- c(Data_Time_Stack,sum(tail(Data_Time_Stack, n=1),dim(page_wise_data[[l]])[2]))
  }
  # Data_Time_Stack
  # While filling in columns, access page_wise_data element using which on all_the_unique_column_names position
  
  # Example:
  # which( 46  <= Data_Time_Stack)[1] 
  # While filling in the 6th column of Output_company_dataframe, access the '2' element of page_wise_data
  
  # Easier to fill in one column at a time
  for (m in seq(length(all_the_unique_column_names)))
  {
    correct_pagewise <- which(m <= Data_Time_Stack)[1] 
    for (n in seq(length(all_the_unique_row_names)))
    {
      correct_parent_dataframe <- page_wise_data[[correct_pagewise]]
      
      Row_id_to_check <- row.names(Output_company_dataframe)[n]
      Column_id_to_check <- colnames(Output_company_dataframe)[m]
      
      pagewise_correct_columm <- which(Column_id_to_check == colnames(correct_parent_dataframe))[1]
      pagewise_correct_row <- which(Row_id_to_check == row.names(correct_parent_dataframe))[1]
      
      if (!is.na(pagewise_correct_columm) & !is.na(pagewise_correct_row))
      {
        correct_element <- correct_parent_dataframe[pagewise_correct_row,pagewise_correct_columm]
        Output_company_dataframe[n,m] <- correct_element
      }
    }
  }
  
  Output_company_dataframe[] <- lapply(Output_company_dataframe, gsub, pattern=',', replacement='')
  
  colnames(Output_company_dataframe) <- gsub(pattern="'",replacement="",
                                        x=colnames(Output_company_dataframe))
  
  ## Saving !!!
  Log_dataframe <- Saving(path_output_accounting_format, temp_company, Output_company_dataframe,
                          path_output_time_series_format,Log_dataframe)
  
  return(Log_dataframe)
}

################################# FUNCTION END #####################################
####################################################################################



####################################################################################
####################################################################################
################################ FUNCTION START ####################################

# Once the ML & time series ready/capable format is generated by processing the raw data,
# this function saves the data in twin format in the correct destination.

Saving <- function(path_output_accounting_format, temp_company, Output_company_dataframe,
                   path_output_time_series_format,Log_dataframe)
{
  Time_Now <- Sys.time()
  Time_Now <- gsub(pattern="-",replacement="_", x = Time_Now)
  Time_Now <- gsub(pattern=":",replacement="_", x = Time_Now)
  Time_Now <- gsub(pattern=" ",replacement="_", x = Time_Now)
  
  if(length(unlist(strsplit(temp_company, split = ''))) > 33)
  {
    temp_company <- paste(unlist(strsplit(temp_company, split = ''))[1:34],collapse='')
  }
  
  accounting_data_filename <- paste0(path_output_accounting_format,'/',temp_company,
                                     ' Q_Report_Accounting_Format_', Time_Now,'.csv',sep='')
  
  write.csv(x = Output_company_dataframe, file = accounting_data_filename,
            row.names = T, na='')
  
  time_series_data_filename <- paste0(path_output_time_series_format,'/',temp_company,
                                      ' Q_Report_Time_Series_',Time_Now,'.csv',sep='')
  
  write.csv(x = t(Output_company_dataframe), file = time_series_data_filename,
            row.names = T, na='')
  
  Log_dataframe <- rbind(Log_dataframe, data.frame(Date_Time = Sys.time(),
                   Log_Entry = paste0(temp_company, ' data saved.',sep=''), stringsAsFactors=F))
  
  return(Log_dataframe)
}


################################# FUNCTION END #####################################
####################################################################################