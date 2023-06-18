import os
import pandas as pd
import pandas_ta as TA
from datetime import time
import csv

pd.options.mode.chained_assignment = None

class Backtest:

    def __init__(self) -> None:
        
        self.folder_path    = '/home/mayank/Documents/HistoricalData/banknifty/'
        self.current_data   = None
    
        #CE dataframes
        self.CE_option      = None
        self.prev_CE        = None

        #PE dataframes 
        self.PE_option      = None
        self.prev_PE        = None

        #finalDf
        self.finalDF        = None
        self.AnalyticsDF    = None

        #If to skip day
        self.skip_day       = False

        #time after which candles are removed from DF
        self.exit_time      = time(15, 11)

        #total premium decay
        self.totalDecay     = 0
    def sortDates(self):
        files = os.listdir(self.folder_path)

        def get_date_from_filename(filename):
            # Extract the date portion from the file name
            date_string = filename.split(".")[0]  # Remove the file extension
            return date_string

        # Sort the file names based on the extracted date
        sorted_files = sorted(files, key=get_date_from_filename)

        return sorted_files

    def parquetToDF(self,file):
         return pd.read_parquet(self.folder_path + file, engine='pyarrow')

    def sortData(self, DF):
        
        #converting to time
        DF['datetime'] = pd.to_datetime(DF['datetime'])
        #DF['time'] = [time.time() for time in DF['datetime']]

        #sort based on time
        DF = DF.sort_values(by=['datetime'])

        #reset index
        DF.reset_index(inplace=True,drop=True)

        return DF
        
    def latestExpiryOptionsChain(self):
        
        #picking up minimum expiry date
        minimumExpiryDate = min(self.current_data['expiry_date'])
        
        #filtering out all non minimum expiry date options
        self.current_data = self.current_data[self.current_data['expiry_date'] == minimumExpiryDate]

        #filtering out all values greater than given exit time 
        self.current_data = self.current_data[self.current_data['time'] < self.exit_time]
    def resampleFrequency(self):
        """
        resample data from 1 minute to 5 minute 15 minute etc
        """
        pass
    
    def option_picker(self):
        '''
        Picks up the closest option based on the premium given 
        '''
        #filter to only have candles at 09:15:00
        time_value = time(9, 15)

        data_9_15 = self.current_data[self.current_data['time'] == time_value]

        #Picking up PE and CE instruments
        new_df_PE = data_9_15  [(data_9_15 ['instrument_type'].str.contains("PE" , case=False)) & (data_9_15 ['instrument_name'] == 'BANKNIFTY')]
        new_df_CE = data_9_15  [(data_9_15 ['instrument_type'].str.contains("CE" , case=False)) & (data_9_15 ['instrument_name'] == 'BANKNIFTY')]
        
        # adding column for abs diff
        new_df_PE['diff_50'] = abs(new_df_PE['close'] - 50)
        new_df_CE['diff_50'] = abs(new_df_CE['close'] - 50)

        #picking up min value CE
        try:
            self.CE_option = new_df_CE[new_df_CE['diff_50'] == min(new_df_CE['diff_50'])]['ticker'].head(1)
            self.CE_option = self.CE_option.iloc[0]

            #picking up min vale PE
            self.PE_option = new_df_PE[new_df_PE['diff_50'] == min(new_df_PE['diff_50'])]['ticker'].head(1)
            self.PE_option = self.PE_option.iloc[0]
        
        except Exception as ex:
            print(ex)
            self.skip_day = True

    def previous_day_data_picker(self):
        '''
        Picks up the last 10 candles of given symbol
        '''
        prev_CE = self.prev_day_data[self.prev_day_data['ticker'] == self.CE_option]
        self.prev_CE = self.sortData(prev_CE).tail(10)

        prev_PE = self.prev_day_data[self.prev_day_data['ticker'] == self.PE_option]
        self.prev_PE = self.sortData(prev_PE).tail(10)

    def option_filter(self, option, toJoinDF):
        '''
        filter out the DF to only contain given option and joins with prev candles
        '''
        filtered_df = self.current_data[self.current_data['ticker'].str.contains(option , case=False)]

        new_df = pd.concat([toJoinDF, filtered_df])
        
        return new_df
    
    def dataframe_creator(self):
        '''
        |time| PE close | CE close | combined close | supertrend | Buy_signal | Sell_signal |
        '''
        #join the dataframes based on date time
        new_df = pd.merge(self.PE_DF,  self.CE_DF, on='datetime')

        new_df = self.sortData(new_df)

        #find combined OPEN, HIGH, LOW, CLOSE
        new_df['combined_OPEN']  = new_df['open_x']  + new_df['open_y']
        new_df['combined_HIGH']  = new_df['high_x']  + new_df['high_y']
        new_df['combined_LOW']   = new_df['low_x']   + new_df['low_y']
        new_df['combined_CLOSE'] = new_df['close_x'] + new_df['close_y']

        # create supertrend
        new_df['supertrend'] = TA.supertrend(new_df['combined_HIGH'], new_df['combined_LOW'], new_df['combined_CLOSE'], length=10, multiplier=3)['SUPERT_10_3.0']
        new_df['Buy_signal'] = 0
        new_df['Sell_signal'] = 0
        n= 10
        for i in range(n,len(new_df)):
            if new_df['combined_CLOSE'][i-1] < new_df['supertrend'][i-1] and new_df['combined_CLOSE'][i] > new_df['supertrend'][i]:
                new_df['Buy_signal'][i] = 1
            if new_df['combined_CLOSE'][i-1] > new_df['supertrend'][i-1] and new_df['combined_CLOSE'][i] < new_df['supertrend'][i]:
                new_df['Sell_signal'][i-1] = 1
    
        selected_columns = ['datetime' , 'time_x','ticker_x', 'ticker_y' ,'combined_CLOSE', 'supertrend', 'Buy_signal', 'Sell_signal']

        self.finalDF  = new_df[selected_columns]

    def analyze(self,date):
        '''
        find the trades per day and analyze P&L
        '''
        #exit_row = self.finalDF[365:366]
        exit_row = self.finalDF[self.finalDF['time_x'] == time(15, 10)]

        #find all the buy and sell signals
        self.AnalyticsDF = self.finalDF[(self.finalDF['Buy_signal'] == 1) | (self.finalDF['Sell_signal'] == 1)]
        self.AnalyticsDF.reset_index(inplace=True,drop=True)

        #skipahead id df is empty
        if self.AnalyticsDF.empty:
            return

        # row count
        row_count = self.AnalyticsDF.shape[0]
        
       
        # buy  sell buy  sell buy  | if odd and first is buy then remove 1 st buy
        if row_count % 2 == 1 and self.AnalyticsDF.loc[0]['Buy_signal'] == 1:
            self.AnalyticsDF = self.AnalyticsDF.drop(0)

        # sell buy  sell buy  sell | if odd and first is sell then add exit time candle as buy  given sell signal not at exit time 
        
        elif row_count % 2 == 1 and self.AnalyticsDF.loc[0]['Sell_signal'] == 1 and exit_row.head(1)['Sell_signal'].item() != 1:
            self.AnalyticsDF = pd.concat([self.AnalyticsDF, exit_row])
            self.AnalyticsDF.at[self.AnalyticsDF.index[-1], 'Buy_signal'] = 1
        
        #drop last sell signal if collides with exit time
        elif row_count % 2 == 1 and self.AnalyticsDF.loc[0]['Sell_signal'] == 1 and exit_row.head(1)['Sell_signal'].item() == 1:
            self.AnalyticsDF = self.AnalyticsDF.iloc[:-1]

        # sell buy  sell buy  | if even and 1st is sell do nothing
        

        # buy  sell buy  sell | if even and 1st is buy remove first buy and add exit time candle as buy  given sell signal not at exit time  
        elif row_count % 2 == 0 and self.AnalyticsDF.loc[0]['Buy_signal'] == 1 and exit_row.head(1)['Sell_signal'].item() != 1:
            self.AnalyticsDF = self.AnalyticsDF.drop(0)
            self.AnalyticsDF = pd.concat([self.AnalyticsDF, exit_row])
            self.AnalyticsDF.at[self.AnalyticsDF.index[-1], 'Buy_signal'] = 1

        #drop last sell signal if collides with exit time
        elif row_count % 2 == 0 and self.AnalyticsDF.loc[0]['Buy_signal'] == 1 and exit_row.head(1)['Sell_signal'].item() == 1:
            self.AnalyticsDF = self.AnalyticsDF.iloc[:-1]

        #self.AnalyticsDF.to_csv('output/' + date + '.csv')
        

    def PLreport(self, date):

        self.AnalyticsDF.reset_index(inplace=True,drop=True)

        trades = []
        trade_count = 0

        for index, row in self.AnalyticsDF.iterrows():
            result = 0
            if index % 2 == 0:
                result += row['combined_CLOSE']  
            else:
                result -= row['combined_CLOSE']  
            
            trades.append(result)
            trade_count += 1
    
        day_PL = sum(trades)
        
        self.totalDecay += day_PL
        
        with open('backtest.csv','a') as fd:
            wr = csv.writer(fd, dialect='excel')
            wr.writerow([date.split('.')[0], day_PL, trade_count/2])

    def iterator_engine(self):
        dates = self.sortDates()

        for index, date in enumerate(dates):

            
            if index == 0:
                #store prev. day all future data 
                self.prev_day_data = self.parquetToDF(date)
                #skip
                continue
            
            else:
                self.prev_day_data = self.parquetToDF(dates[index-1])

            #setting the DF to current date
            self.current_data  = self.parquetToDF(date)
        
            #filtering out all except latest expiry
            self.latestExpiryOptionsChain()

            #loading up data for most resent BNF Future
            #self.BNFtoDF()

            #option picker based on closest premium
            self.option_picker()
            
            #skip day due to data issue
            if self.skip_day == True:
       
                self.skip_day = False
                continue 
            
            #pickup last 10 observations from previous day for given options
            self.previous_day_data_picker()
            
            #filter out all except given option and join with prev data
            self.PE_DF = self.option_filter(self.PE_option, self.prev_PE) 
            self.CE_DF = self.option_filter(self.CE_option, self.prev_CE)
            
            #create new dataframe
            self.dataframe_creator()

            #find trades profit and loss
            self.analyze(date)

            #pl report generate
            self.PLreport(date)

        #stats
        print('Total premium decay:', self.totalDecay)

if __name__ == '__main__':
    supertrend = Backtest()

    supertrend.iterator_engine()