import blpapi
import datetime
import pandas as pd
import numpy as np

def check_date_time(value):
    if not isinstance(value, datetime.datetime):
        raise ValueError('The dates have to be datetime objects')
    return None

def check_overrides(value):
    if value != None:
        if type(value) != dict:
            raise ValueError('The overrides has to be a dictionary')
    return None

def check_other_param(value):
    if value != None:
        if type(value) != dict:
            raise ValueError('The other_param argument has to be a dictionary')
    return None

class BLP():

    def __init__(self):
        self.boo_getIntradayBar = False
        self.boo_getIntradayTick = False
        self.boo_getRefData = False
        self.boo_getHistoData = False
        self.dictData = {}
        self.list_df_buffer = []  # Used to store the temporary dataframes

        self.BAR_DATA = blpapi.Name("barData")
        self.BAR_TICK_DATA = blpapi.Name("barTickData")
        self.CATEGORY = blpapi.Name("category")
        self.CLOSE = blpapi.Name("close")
        self.FIELD_DATA = blpapi.Name("fieldData")
        self.FIELD_ID = blpapi.Name("fieldId")
        self.HIGH = blpapi.Name("high")
        self.LOW = blpapi.Name("low")
        self.MESSAGE = blpapi.Name("message")
        self.NUM_EVENTS = blpapi.Name("numEvents")
        self.OPEN = blpapi.Name("open")
        self.RESPONSE_ERROR = blpapi.Name("responseError")
        self.SECURITY_DATA = blpapi.Name("securityData")
        self.SECURITY = blpapi.Name("security")
        self.SESSION_TERMINATED = blpapi.Name("SessionTerminated")
        self.TIME = blpapi.Name("time")
        self.VALUE = blpapi.Name("value")
        self.VOLUME = blpapi.Name("volume")
        self.TICK_DATA = blpapi.Name("tickData")
        self.TICK_SIZE = blpapi.Name("size")
        self.TYPE = blpapi.Name("type")

        # Create a Session
        self.session = blpapi.Session()

        # Start a Session
        if not self.session.start():
            print("Failed to start session.")
            return None


    def printErrorInfo(self, leadingStr, errorInfo):
        print ("%s%s (%s)" % (leadingStr, errorInfo.getElementAsString(self.CATEGORY),
                             errorInfo.getElementAsString(self.MESSAGE)))
        return None

    def check_service(self, service):
        # Open service to get historical data from
        if not (self.session.openService(service)):
            print("Failed to open {}".format(service))
        return None

    def set_other_param(self, other_param, request):
        if other_param != None:
            for k, v in other_param.items():
                request.set(k, v)
        return request


    def set_overrides(self, overrides, request):
        if overrides != None:
            req_overrides = request.getElement("overrides")

            list_overrides = []
            for fieldId, value in overrides.items():
                list_overrides.append(req_overrides.appendElement())
                list_overrides[-1].setElement("fieldId", fieldId)
                list_overrides[-1].setElement("value", value)

        return request

    def eventLoop(self, session):
        done = False
        while not done:
            event = session.nextEvent(20)
            if event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                self.processResponseEvent(event)
            elif event.eventType() == blpapi.Event.RESPONSE:
                self.processResponseEvent(event)
                done = True
            else:
                for msg in event:
                    if event.eventType() == blpapi.Event.SESSION_STATUS:
                        if msg.messageType() == self.SESSION_TERMINATED:
                            done = True
        return None

    def processResponseEvent(self, event):
        for msg in event:
            if msg.hasElement(self.RESPONSE_ERROR):
                self.printErrorInfo("REQUEST FAILED: ", msg.getElement(self.RESPONSE_ERROR))
                continue

            if self.boo_getIntradayBar:
                self.process_msg_intradaybar(msg)
            elif self.boo_getIntradayTick:
                self.process_msg_intradaytick(msg)
            elif self.boo_getRefData:
                self.process_msg_refdata(msg)
            elif self.boo_getHistoData:
                self.process_msg_histodata(msg)

        return None

    def get_intradaybar(self, security, event, start_date, end_date, barInterval, other_param):
        self.boo_getIntradayBar = True

        try:
            self.check_service("//blp/refdata")

            refDataService = self.session.getService("//blp/refdata")
            request = refDataService.createRequest("IntradayBarRequest")

            # Only one security/eventType per request
            request.set("security", security)
            request.set("eventType", event)
            request.set("interval", barInterval)

            # All times are in GMT
            request.set("startDateTime", start_date)
            request.set("endDateTime", end_date)

            # Append other parameters if there are
            request = self.set_other_param(other_param, request)

            self.session.sendRequest(request)
            self.eventLoop(self.session) # Wait for events from session

        finally:
            # Stop the session
            self.session.stop()
            df_buffer = pd.DataFrame.from_dict(self.dictData,
                                               orient='index',
                                               columns=['open', 'high', 'low', 'close', 'volume', 'numEvents', 'value'])
            df_buffer['ticker'] = security
            df_buffer = df_buffer.reset_index(level=0).rename(columns={'index': 'time'}).set_index(['time', 'ticker'])

            return df_buffer.fillna(value=np.nan)


    def process_msg_intradaybar(self, msg):
        data = msg.getElement(self.BAR_DATA).getElement(self.BAR_TICK_DATA)

        for bar in data.values():
            time = bar.getElementAsDatetime(self.TIME)
            open = bar.getElementAsFloat(self.OPEN)
            high = bar.getElementAsFloat(self.HIGH)
            low = bar.getElementAsFloat(self.LOW)
            close = bar.getElementAsFloat(self.CLOSE)
            numEvents = bar.getElementAsInteger(self.NUM_EVENTS)
            volume = bar.getElementAsInteger(self.VOLUME)
            value = bar.getElementAsInteger(self.VALUE)

            self.dictData[time] = [open, high, low, close, volume, numEvents, value]  # Increment rows in a dictionary

        return None

    def get_refdata(self, security, fields, overrides, other_param):
        self.boo_getRefData = True
        self.fields = fields

        try:
            self.check_service("//blp/refdata")

            refDataService = self.session.getService("//blp/refdata")
            request = refDataService.createRequest("ReferenceDataRequest")

            # Append securities to request
            for ticker in security:
                request.append("securities", ticker)

            # Append fields to request
            for field in fields:
                request.append("fields", field)

            # Append other parameters if there are
            request = self.set_other_param(other_param, request)

            # Add overrides if there are
            request = self.set_overrides(overrides, request)

            self.session.sendRequest(request)
            self.eventLoop(self.session)        # Wait for events from session.

        finally:
            self.session.stop()

        df_buffer = pd.DataFrame.from_dict(self.dictData, orient='index', columns=fields).fillna(value=np.nan)

        return df_buffer


    def process_msg_refdata(self, msg):
        data = msg.getElement(self.SECURITY_DATA)

        for securityData in data.values():
            field_data = securityData.getElement(self.FIELD_DATA)  # Element that contains all the fields
            security_ticker = securityData.getElementAsString(self.SECURITY)  # Get Ticker
            self.dictData[security_ticker] = []  # Create list of fields

            for my_field in self.fields:
                if field_data.hasElement(my_field):  # Check if the field exists for this particular ticker
                    self.dictData[security_ticker].append(field_data.getElement(my_field).getValue())
                else:
                    self.dictData[security_ticker].append(None)

        return None


    def get_histodata(self,security, fields, start_date, end_date, overrides, other_param):

        self.boo_getHistoData = True
        self.fields = fields

        try:
            self.check_service("//blp/refdata")

            # Obtain previously opened service
            refDataService = self.session.getService("//blp/refdata")

            # Create and fill the request for the historical data
            request = refDataService.createRequest("HistoricalDataRequest")

            # Append securities to request
            for ticker in security:
                request.getElement("securities").appendValue(ticker)

            # Append fields to request
            for field in fields:
                request.getElement("fields").appendValue(field)

            request.set("startDate", start_date.strftime('%Y%m%d'))
            request.set("endDate", end_date.strftime('%Y%m%d'))

            # Append other parameters if there are
            request = self.set_other_param(other_param, request)

            # Add overrides if there are
            request = self.set_overrides(overrides, request)

            self.session.sendRequest(request) # Send the request
            self.eventLoop(self.session)      # Wait for events from session.

        finally:
            # Stop the session
            self.session.stop()

        # Returns a pandas dataframe with a Multi-index (date/ticker)
        df_buffer = pd.concat(self.list_df_buffer).reset_index(level=0).rename(columns={'index': 'date'}).set_index(['date', 'ticker'])

        return df_buffer.fillna(value=np.nan)


    def process_msg_histodata(self, msg):
        dictData = {}  # Used for structuring the data received from bloomberg
        security_data = msg.getElement(self.SECURITY_DATA)
        data = security_data.getElement(self.FIELD_DATA)  # Iterable object that contains all the fields
        security_ticker = security_data.getElementAsString(self.SECURITY)  # Get Ticker (there is only one ticker by message)

        for field_data in data.values():  # Iterate through each date
            date = field_data.getElement('date').getValue()
            dictData[date] = []
            dictData[date].append(security_ticker)

            for my_field in self.fields:
                if field_data.hasElement(my_field):  # Check if the field exists for this particular ticker
                    dictData[date].append(field_data.getElement(my_field).getValue())  # Increment dictionary
                else:
                    dictData[date].append(None)

        # Append data to the list of dataframe (concatenated in the end)
        self.list_df_buffer.append(pd.DataFrame.from_dict(dictData, orient='index', columns=['ticker'] + self.fields))

        return None


    def get_intradaytick(self, ticker, list_events, start_date, end_date, condition_codes, other_param):
        self.boo_getIntradayTick = True

        try:
            self.check_service("//blp/refdata")

            refDataService = self.session.getService("//blp/refdata")
            request = refDataService.createRequest("IntradayTickRequest")

            # only one security/eventType per request
            request.set("security", ticker)

            # Add fields to request
            for event in list_events:
                request.getElement("eventTypes").appendValue(event)

            # All times are in GMT
            request.set("startDateTime", start_date)
            request.set("endDateTime", end_date)

            # Add condition codes
            request.set("includeConditionCodes", condition_codes)

            # Append other parameters if there are
            request = self.set_other_param(other_param, request)

            # Create set of column names if extra columns added to other_param
            self.extra_columns = ['conditionCodes'] if condition_codes else []

            if other_param != None:
                for k,v in other_param.items():
                    if ('include' in k):
                        if v:
                            col_name = k.replace('include', '')
                            col_name = col_name[:1].lower() + col_name[1:]
                            self.extra_columns.append(col_name)

            self.session.sendRequest(request)
            self.eventLoop(self.session)

        finally:
            # Stop the session
            self.session.stop()

        df_buffer = pd.DataFrame.from_dict(self.dictData, orient='index', columns=['type', 'value', 'size'] + self.extra_columns)
        df_buffer['ticker'] = ticker
        df_buffer = df_buffer.reset_index(level=0).rename(columns={'index': 'time'}).set_index(['time', 'ticker'])

        return df_buffer.fillna(value=np.nan)

    def process_msg_intradaytick(self, msg):
        data = msg.getElement(self.TICK_DATA).getElement(self.TICK_DATA)

        for item in data.values():
            time = item.getElementAsDatetime(self.TIME)
            str_type = item.getElementAsString(self.TYPE)
            value = item.getElementAsFloat(self.VALUE)
            size = item.getElementAsInteger(self.TICK_SIZE)

            self.dictData[time] = [str_type, value, size]  # Increment rows in a dictionary

            extra_data = []
            for extra_col in self.extra_columns:
                if item.hasElement(extra_col):
                    extra_data.append(item.getElement(extra_col).getValue())
                else:
                    extra_data.append(None)

            self.dictData[time] += extra_data

        return None

def IntradayBar(security, event, start_date, end_date, barInterval, other_param=None):

    '''
    ────────────────────────────────────────────────────────────────────────────────────────────────
    ┌─────────┐     from blp_pandas import blp_pandas as bbg
    │ Example │
    └─────────┘     df = bbg.IntradayBar(['CAC FP Equity', 'CACX LN Equity'],
                                          ['BID', 'ASK'],
                                          datetime(2018,11,22,9,0),
                                          datetime(2018,11,22,17,30),
                                          1)

    :return: pandas dataframe
    '''

    def get_tickerbar(ticker, event, start_date, end_date, barInterval, other_param):

        '''
        This nested function is called for each ticker so that the Bloomberg object is destroyed after each request
        This is a thread-safe method
        '''

        objBBG = BLP()  # Instantiate object with session
        df_ticker = objBBG.get_intradaybar(ticker, event, start_date, end_date, barInterval, other_param)  # Get data in dataframe

        return df_ticker

    #***************************
    # Check the input variables
    #***************************

    check_date_time(start_date)
    check_date_time(end_date)

    if (type(barInterval) != int):
        raise ValueError('The bar interval has to be an integer greater than 1')
    elif barInterval < 1:
        raise ValueError('The bar interval has to be an integer greater than 1')

    if (type(security) != str) and (type(security) != list):
        raise ValueError('The security parameter has to be a string or a list')

    if (type(event) != str):
        raise ValueError('The event has to be a string')

    # ***************************
    # Get data
    # ***************************

    if type(security) == str:

        return get_tickerbar(security,event, start_date, end_date, barInterval, other_param)

    elif type(security) == list:

        listOfDataframes = []
        for ticker in security:
            listOfDataframes.append(get_tickerbar(ticker, event, start_date, end_date, barInterval, other_param))

        return pd.concat(listOfDataframes)


def RefData(security, fields, overrides=None, other_param=None):

    '''
    ────────────────────────────────────────────────────────────────────────────────────────────────
    ┌─────────┐     from blp_pandas import blp_pandas as bbg
    │ Example │
    └─────────┘     df = bbg.RefData(['CAC FP Equity', 'CACX LN Equity'],
                                     'EQY_WEIGHTED_AVG_PX',
                                     {'VWAP_START_TIME': '9:30','VWAP_END_TIME': '11:30'})


    :return: pandas dataframe
    '''


    #***************************
    # Check the input variables
    #***************************

    check_overrides(overrides)
    check_other_param(other_param)

    if (type(security) != str) and (type(security) != list):
        raise ValueError('The security parameter has to be a string or a list')

    if type(security) == str:
        security = [security]

    if (type(fields) != str) and (type(fields) != list):
        raise ValueError('The fields parameter has to be a string or a list')

    if type(fields) == str:
        fields = [fields]


    # ***************************
    # Get data
    # ***************************

    bloomberg = BLP()

    return bloomberg.get_refdata(security, fields, overrides, other_param)


def IntradayTick(security, list_events, start_date, end_date, condition_codes=True,  other_param=None):

    '''
    ────────────────────────────────────────────────────────────────────────────────────────────────

    ┌─────────┐     from blp_pandas import blp_pandas as bbg
    │ Example │
    └─────────┘     df = bbg.IntradayTick(['CAC FP Equity', 'CACX LN Equity'],
                                          ['BID', 'ASK'],
                                          datetime(2018,11,22,9,0),
                                          datetime(2018,11,22,17,30),
                                          other_param = {'includeNativeTradeId': True})

    :return: dataframe
    '''

    def get_tickertick(ticker, list_events, start_date, end_date, condition_codes, other_param):
        '''
        This nested function is called for each ticker so that the Bloomberg object is destroyed after each request
        This is a thread-safe method
        Returns a pandas dataframe
        '''

        objBBG = BLP()  # Instantiate object with session

        return objBBG.get_intradaytick(ticker, list_events, start_date, end_date, condition_codes, other_param)


    #***************************
    # Check the input variables
    #***************************

    check_date_time(start_date)
    check_date_time(end_date)
    check_other_param(other_param)

    if (type(security) != str) and (type(security) != list):
        raise ValueError('The security parameter has to be a string or a list')

    if (type(list_events) == str):
        list_events = [list_events]

    # ***************************
    # Get data
    # ***************************

    if type(security) == str:
        return getTickerTick(security, list_events, start_date, end_date, condition_codes, other_param)

    elif type(security) == list:
        listOfDataframes = []

        for ticker in security:
            listOfDataframes.append(get_tickertick(ticker, list_events, start_date, end_date, condition_codes, other_param))

        return pd.concat(listOfDataframes)


def HistoData(security, fields, start_date, end_date, overrides=None, other_param=None):

    '''
    ────────────────────────────────────────────────────────────────────────────────────────────────
    ┌─────────┐     from blpPandas import blpPandas as bbg
    │ Example │     df = bbg.HistoData(['CAC FP Equity', 'CACX LN Equity'],
    └─────────┘                        ['PX_LAST', 'VOLUME'],
                                       datetime(2018,1,1),
                                       datetime(2018,12,31))

    :return: pandas dataframe

    '''


    #***************************
    # Check the input variables
    #***************************

    check_date_time(start_date)
    check_date_time(end_date)
    check_overrides(overrides)
    check_other_param(other_param)

    if (type(security) != str) and (type(security) != list):
        raise ValueError('The security parameter has to be a string or a list')

    if type(security) == str:
        security = [security]

    if (type(fields) != str) and (type(fields) != list):
        raise ValueError('The fields parameter has to be a string or a list')

    if type(fields) == str:
        fields = [fields]

    # ***************************
    # Get data
    # ***************************

    bloomberg = BLP()

    return bloomberg.get_histodata(security, fields, start_date, end_date, overrides, other_param)


__copyright__ = """
Copyright 2012. Bloomberg Finance L.P.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:  The above
copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
"""