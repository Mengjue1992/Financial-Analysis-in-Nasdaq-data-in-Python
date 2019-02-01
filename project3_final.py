import pandas as pd
import numpy as np
import os 

class Config(object):
    in_path = 'G:\\out\\'
    out_path = 'G:\\out\\outputs\\'
    time_intervals = ['15min']
    
    def __init__(self,cap):
        self.cap = cap
        if cap == 1:
            self.tickers = ['AAPL','CVX','T','JNJ','PFE','JPM','KO','PM','MRK','INTC','VZ',
                'WMT','ORCL','CSCO','ABT','BAC','AMZN','V','BA','EBAY']
        elif cap == 2:
            self.tickers = ['AMT','TWC','PRU','PCP','AFL','BLK','STT','WMB','SE','CMI','DFS',
                'BRCM','ALL','BEN','TROW','CCL','PCAR','WFM','IP','AET']
        elif cap == 3:
            self.tickers = ['RAI','CTXS','DISCA','NUE','STJ','FAST','GWW','PGR','CERN','KSS','FTI',
                'MTB','HOG','CHK','ROK','MUR','SNDK','XRX','AA','MAR']
        elif cap == 4:
            self.tickers = ['HIG','JWN','NVDA','DVA','LIFE','BWA','PFG','LLTC','KMX','NI',
               'DRI','MCHP','RL','AKAM','CPB','CMS','QEP','TSN','HRS','LEN']
        elif cap == 5:
            self.tickers = ['TMK','SWY','EA','FTR','PBCT','PHM','EXPE','RHI','SNI','ATI','ZION',
               'HCBK','AIV','APOL','AVY','AIZ','GT','PDCO','DF','FII']

class Orderbook:
    def __init__(self,config):
        self.config = config
        self.out_path = config.out_path
        self.cap = config.cap
        

    def generateFilenames(self,ticker):
         # create path if not exist
        self.ticker = ticker
        self.sub_path = self.config.in_path+ self.ticker+'\\'
       
        if not os.path.exists(self.sub_path):
            os.makedirs(self.sub_path)
        return os.listdir(self.sub_path)
        
        
    def readfile(self,filename):
        self.date = filename[-12:-4]
        fullPath = self.sub_path + filename
        try:
            use_col = ['mtime','xcomment','price','shares']
            self.data = pd.read_csv(fullPath,usecols = use_col)
            self.data.index =  pd.to_datetime(self.data['mtime'], format='%H:%M:%S')
            self.data['dollars'] = self.data['price']*self.data['shares']
        except IOError:
            print('file not exist:filename')
        
        return self.data

    def cancelRate(self):
        if self.data.empty: print('empty data:'+self.ticker+self.date)
        cancel = self.data.loc[self.data['xcomment'].isin(['D','X'])]
        cancel = cancel.drop(['xcomment'],axis = 1)
        cancel_cnt = cancel.groupby(pd.TimeGrouper(self.time_interval))['price'].count().rename("num")
        total_events = self.data.groupby(pd.TimeGrouper(self.time_interval))['price'].count().rename("totalNum")

        cancel_shares = cancel.groupby(pd.TimeGrouper(self.time_interval))['shares'].sum()
        total_shares = self.data.groupby(pd.TimeGrouper(self.time_interval))['shares'].sum()
        cancel_values = cancel['dollars'].groupby(pd.TimeGrouper(self.time_interval)).sum()
        total_values = self.data['dollars'].groupby(pd.TimeGrouper(self.time_interval)).sum()

        df = pd.DataFrame()
        df['Number of Cancel'] = cancel_cnt
        df['Number of Trades'] = total_events
        df['Percent of Trades'] = cancel_cnt/total_events
        df['Number of Cancel Share'] = cancel_shares
        df['Total Share Vol'] = total_shares
        df['Total Share Pct'] = cancel_shares/total_shares
        df['Value of Cancel'] = cancel_values
        df['Total Dollar Val'] = total_values
        df['Total Dollar Pct'] = cancel_values/total_values
        
        return df

    def action(self):
        self.result = self.cancelRate()
        #self.result['cancelrate'] = self.cancelRate()
        #self.result['#runs'],self.result['zscore'] = self.runpercent()
        self.result['ticker'] = self.ticker
        self.result['date'] = self.date
        return self.result
        
    def getResults(self,time_interval):
        results = []
        self.time_interval = time_interval
        for ticker in self.config.tickers:
            filenames = self.generateFilenames(ticker)
            for filename in filenames:
                print(filename)
                data = self.readfile(filename)
                df = self.action()
                 
                results.append(df)      
            
            self.results = pd.concat(results).dropna()
            self.results.to_csv(self.out_path+'%strade_size_res.csv'%(self.cap))

        return self.results

	def getAveStat(results,o):
		test_avg = pd.DataFrame()

		test_avg['Number of Trades'] = [np.mean(results['Number of Trades'])]
		test_avg['Number of Cancel'] = [np.mean(results['Number of Cancel'])]
		test_avg['Percent of Trades'] = [np.mean(results['Percent of Trades'])]
		test_avg['Total Share Vol'] = [np.mean(results['Total Share Vol'])]
		test_avg['Number of Cancel Share'] = [np.mean(results['Number of Cancel Share'])]
		test_avg['Total Share Pct'] = [np.mean(results['Total Share Pct'])]
		test_avg['Total Dollar Val'] = [np.mean(results['Total Dollar Val'])]
		test_avg['Value of Cancel'] = [np.mean(results['Value of Cancel'])]
		test_avg['Total Dollar Pct'] = [np.mean(results['Total Dollar Pct'])]

		test_avg.to_csv(o.out_path+'%s%s_avg_stat.csv'%(o.cap,o.time_interval))
		return test_avg

def Run(Cap):
    if Cap == 1:
        config = Config(1)
    elif Cap == 2:
        config = Config(2)
    elif Cap == 3:
        config = Config(3)
    elif Cap == 4:
        config = Config(4)
    else:
        config = Config(5)
        
    o = Orderbook(config)
    df_avgs = []
    
    for time in config.time_intervals:
  
        results = o.getResults(time)
      
        df_avg = getAveStat(results,o)
        df_avgs.append(df_avg)

    df_avgs = pd.concat(df_avgs)
    df_avgs.to_csv(o.out_path+'%savestat%s.csv'%(o.cap,o.cap))
    
    
    return results,df_avgs 
        
if __name__ == "__main__":

    results,stat = Run(5)
    results,stat = Run(4)
    results,stat = Run(3)
    results,stat = Run(2)
    results,stat = Run(1)
    

