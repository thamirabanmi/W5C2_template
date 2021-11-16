import pandas

class AccountManager():

    def __init__(self,
                 symbol: str,
                 trades_df: pandas.DataFrame,
                 starting_capital: float,
                 allocation_amount: float):


        self.trades_df = trades_df
        self.current_date = trades_df.index[0]

        self.symbol = symbol
        self.position = 0
        self.starting_capital = starting_capital
        self.allocation_amount = allocation_amount
        self.cash = self.starting_capital


        self.order_history = []  # Data Frame

        self.dates = []

        self.account_value = []
        self.position_change = []
        self.positions = []
        self.cash_balances = []
        self.fees = []
        # self.dates = trades_df.index

        self.final_df = None

    def create_trade(self,
                     date:pandas.Timestamp,
                     size:float,
                     cost:float=1.0,
                     field='Open'):


        price = self.trades_df.loc[date][field]
        cash_change = (size * price *-1) - cost
        fees = cost
        position_change = size

        if size>0:
            trade_type ='BUY'
        else:
            trade_type ='SELL'

        return {'cash_change':cash_change,
                'fees':fees,
                'position_change':position_change,
                'symbol':self.symbol,
                'order_df': pandas.DataFrame(data={
                                             "symbol":self.symbol.upper(),
                                             "date":str(date),
                                             'time':f"At Market {field}",
                                             "order":trade_type,
                                             "order_type":"MARKET",
                                             "price": price,
                                             'size':abs(size),
                                             "exchange_fees":cost},index=[date])
                }

    def close_position(self ,date, field='Open'):

        if self.position == 0:
            return None
        else: #self.position > 0:
            # Close the position
            res = self.create_trade(date=date,size=self.position*-1,field=field)
            return res

    def open_position(self, direction:str,
                      date:pandas.Timestamp,
                      field:str='Open'):

        trade_cash = self.cash * self.allocation_amount
        size = trade_cash/self.trades_df.loc[date][field]
        size = round(size,2)

        if direction == 'Long':
            res = self.create_trade(date=date,size=size)
            return res

        elif direction=='Short':
            dir_mod = -1
            res = self.create_trade(date=date,
                                    size=size*dir_mod)
            return res
        else:
            raise Exception(f"Unexpected Direction: {direction} @ {date}")

    def get_account_value(self,
                          date:pandas.Timestamp,
                          field:str='Close') -> float:

        value = self.cash + (self.position * self.trades_df.loc[date][field])
        return value

    def run(self) -> pandas.DataFrame:

        for idx in range(len(self.trades_df)):

            current_date = self.trades_df.index[idx]
            self.dates.append(current_date)

            position_change = 0
            fees = 0

            if self.trades_df.loc[current_date]['trades'] != 'None':

                if self.position != 0:
                    res = self.close_position(current_date)

                    self.position = 0

                    position_change += res['position_change']

                    self.cash += res['cash_change']
                    fees += res['fees']
                    self.order_history.append(res['order_df'])


                trade_direction = self.trades_df.loc[current_date]['trades']

                res = self.open_position(direction=trade_direction,
                                         date=current_date)
                 # += res['position_change']
                position_change += res['position_change']
                self.position = res['position_change']
                self.cash += res['cash_change']
                fees += res['fees']
                self.order_history.append(res['order_df'])



            #update position changes:
            self.position_change.append(position_change)

            self.position = self.position

            #update position at EOD
            self.positions.append(self.position)

            #update cash

            self.cash_balances.append(self.cash)

            #update account value
            self.account_value.append(self.get_account_value(date=current_date))
            #add fees
            self.fees.append(fees)

        ## Last day

        if self.position != 0:
            res = self.close_position(current_date,field='Close')
            self.trades_df.iloc[-1,self.trades_df.columns.get_loc('trades')] = ('Long' if res['position_change']>0 else "Short")
            self.positions[-1] = 0

            self.position_change[-1] += res['position_change']

            self.cash_balances[-1] += res['cash_change']
            self.account_value[-1] = self.get_account_value(date=current_date,field='Close')
            self.fees[-1] += res['fees']
            self.order_history.append(res['order_df'])




        self.final_df = pandas.DataFrame(data={
            'account_val':self.account_value,
            'cash_bal':self.cash_balances,
            'position':self.positions,
            'position_changes':self.position_change,
            'fees':self.fees
        },index=self.dates)


        return {"result_df":pandas.concat([self.trades_df,self.final_df],axis=1),"broker_orders":pandas.concat(self.order_history)}
