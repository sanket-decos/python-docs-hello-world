from flask import Flask
from flask_restful import Resource, Api, reqparse
from flask_cors import CORS
import pandas as pd
import ast
import datetime

app = Flask(__name__)
CORS(app)
api = Api(app)

                   
class CustomerSatisfactionTeams(Resource):
    def get(self):
        #parser = reqparse.RequestParser()  # initialize
        #parser.add_argument('userId', required=True)  # add args
        #args = parser.parse_args()  # parse arguments to dictionary

        data = pd.read_csv('customerSatisfactionTeams.csv')  # read local CSV
        #df = pd.read_csv('customerSatisfactionTeams.csv')  # read local CSV
        #data = df.loc[df['userId'] == args['userId']]
        data = data[['Team', 'Quarter', 'Rating']]
        data = data.to_dict()  # convert dataframe to dict
        return {'data': data}, 200  # return data and 200 OK

                    
class CustomerSatisfactionEmployees(Resource):
    def get(self):
        #parser = reqparse.RequestParser()  # initialize
        #parser.add_argument('TeamName', required=True)  # add args
        #args = parser.parse_args()  # parse arguments to dictionary

        #data = pd.read_csv('customerSatisfactionEmployees.csv')  # read local CSV
        df = pd.read_csv('customerSatisfactionEmployees.csv')  # read local CSV
        #data = df.loc[df['TeamName'] == args['TeamName']][['Employee', 'Communication', 'Knowledge', 'TotalImpression']]
        data = df[['Employee', 'Communication', 'Knowledge', 'TotalImpression']]
        data = data.to_dict()  # convert dataframe to dict
        return {'data': data}, 200  # return data and 200 OK

                    
class CSOpenIncidents(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Bij Development', 'In afwachting van externe partij', 'Intern informatie opgevraagd', 'Open', 'Pending', 'Wachten op klantreactie', 'Werkzaamheden ingepland'])]
        df = df.loc[(df['message'] == 'Incident / storing')]

        data = {}
        
        #TicketsByType
        dataTicketsByType = df.groupby(['type']).size().reset_index(name='count')
        dataTicketsByType = dataTicketsByType.to_dict()  # convert dataframe to dict
        data['TicketsByType'] = dataTicketsByType
        
        #TicketsByAgent 
        dataTicketsByAgent = df.groupby(['agent']).size().reset_index(name='count')
        dataTicketsByAgent = dataTicketsByAgent.to_dict()  # convert dataframe to dict
        data['TicketsByAgent'] = dataTicketsByAgent
        
        #TicketsByTypePriority
        datagroup = df.groupby(['type', 'priority']).size().reset_index(name='countt')
        pivoted = datagroup.pivot(index='type', columns= 'priority', values='countt')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)
        dataTicketsByTypePriority = {}
        series = []
        for col in flattened.columns:
            if col == 'type':
                dataTicketsByTypePriority['categories'] = flattened[col].values.tolist()
            else:
                temp = {}
                temp['name'] = str(col)
                temp['data'] = flattened[col].values.tolist()
                series.append(temp)
        dataTicketsByTypePriority['series'] = series
        data['TicketsByTypePriority'] = dataTicketsByTypePriority
        
        #TicketsByStatus
        dataTicketsByStatus = df.groupby(['status']).size().reset_index(name='count')
        json_valuesTicketsByStatus = dataTicketsByStatus.to_json(orient ='values')
        data['TicketsByStatus'] = json_valuesTicketsByStatus

        #TicketsByGroup
        def ClubFunction(row):
            if row['groupName'] == 'Development':
                groupNameClubbed = 'Development'
            elif row['groupName'] == 'Technisch consultants':
                groupNameClubbed = 'Technisch consultants and Functioneel consultants'
            elif row['groupName'] == 'Functioneel consultants':
                groupNameClubbed = 'Technisch consultants and Functioneel consultants'
            elif row['groupName'] == 'Intake':
                groupNameClubbed = 'Intake'
            elif row['groupName'] == 'India Intake':
                groupNameClubbed = 'Intake'
            elif row['groupName'] == 'Support 1e lijn':
                groupNameClubbed = 'Support 1e lijn'
            elif row['groupName'] == 'Support 2e lijn':
                groupNameClubbed = 'Support 2e lijn'
            else:
                groupNameClubbed = 'Others'
            return groupNameClubbed
        df['groupNameClubbed'] = df.apply(ClubFunction, axis=1)
        #Group by
        dfTicketsByGroup = df.groupby(['groupNameClubbed']).size().reset_index(name='count')
        #Sort
        dfTicketsByGroup['groupNameClubbed_cat'] = pd.Categorical(
            dfTicketsByGroup['groupNameClubbed'], 
            categories=['Intake','Support 1e lijn','Support 2e lijn','Technisch consultants and Functioneel consultants','Development','Others'], 
            ordered=True
        )
        dataTicketsByGroup = dfTicketsByGroup[['groupNameClubbed_cat', 'count']].sort_values('groupNameClubbed_cat')
        json_valuesTicketsByGroup = dataTicketsByGroup.to_json(orient ='values')
        data['TicketsByGroup'] = json_valuesTicketsByGroup

        
        return {'data': data}, 200  # return data and 200 OK


class CSAllOpenTickets(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Bij Development', 'In afwachting van externe partij', 'Intern informatie opgevraagd', 'Open', 'Pending', 'Wachten op klantreactie', 'Werkzaamheden ingepland'])]
        df = df[~df.message.isin(['Project', ''])]

        data = {}

        #TicketsByAgentMessage
        datagroup = df.groupby(['agent', 'message']).size().reset_index(name='countt')
        pivoted = datagroup.pivot(index='agent', columns= 'message', values='countt')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)
        dataTicketsByAgentMessage = {}
        series = []
        for col in flattened.columns:
            if col == 'agent':
                dataTicketsByAgentMessage['categories'] = flattened[col].values.tolist()
            else:
                temp = {}
                temp['name'] = str(col)
                temp['data'] = flattened[col].values.tolist()
                series.append(temp)
        dataTicketsByAgentMessage['series'] = series       
        data['TicketsByAgentMessage'] = dataTicketsByAgentMessage  # convert dataframe to dict
        
        #TicketsByTypeMessage
        datagroup = df.groupby(['type', 'message']).size().reset_index(name='countt')
        pivoted = datagroup.pivot(index='type', columns= 'message', values='countt')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)
        dataTicketsByTypeMessage = {}
        series = []
        for col in flattened.columns:
            if col == 'type':
                dataTicketsByTypeMessage['categories'] = flattened[col].values.tolist()
            else:
                temp = {}
                temp['name'] = str(col)
                temp['data'] = flattened[col].values.tolist()
                series.append(temp)
        dataTicketsByTypeMessage['series'] = series  
        data['TicketsByTypeMessage'] = dataTicketsByTypeMessage  # convert dataframe to dict
        
        #TicketsByTypePriority
        datagroup = df.groupby(['type', 'priority']).size().reset_index(name='countt')
        pivoted = datagroup.pivot(index='type', columns= 'priority', values='countt')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)
        dataTicketsByTypePriority = {}
        series = []
        for col in flattened.columns:
            if col == 'type':
                dataTicketsByTypePriority['categories'] = flattened[col].values.tolist()
            else:
                temp = {}
                temp['name'] = str(col)
                temp['data'] = flattened[col].values.tolist()
                series.append(temp)
        dataTicketsByTypePriority['series'] = series  
        data['TicketsByTypePriority'] = dataTicketsByTypePriority  # convert dataframe to dict
        
        #TicketsByCompanyname
        dataTicketsByCompanyname = df.groupby(['companyName']).size().reset_index(name='count')
        data['TicketsByCompanyname'] = dataTicketsByCompanyname.to_dict()  # convert dataframe to dict

        return {'data': data}, 200  # return data and 200 OK


class CSClosedTickets(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Closed', 'In ontwikkeling', 'Resolved'])]
        df = df[df.message.isin(['Feedback', 'Incident / storing', 'Project', 'Vraag', ])]

        data = {}

        #TicketsByAgentMessage
        datagroup = df.groupby(['agent', 'message']).size().reset_index(name='countt')
        pivoted = datagroup.pivot(index='agent', columns= 'message', values='countt')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)
        dataTicketsByAgentMessage = {}
        series = []
        for col in flattened.columns:
            if col == 'agent':
                dataTicketsByAgentMessage['categories'] = flattened[col].values.tolist()
            else:
                temp = {}
                temp['name'] = str(col)
                temp['data'] = flattened[col].values.tolist()
                series.append(temp)
        dataTicketsByAgentMessage['series'] = series  
        data['TicketsByAgentMessage'] = dataTicketsByAgentMessage  # convert dataframe to dict
        

        #TicketsByGroup
        def ClubFunction(row):
            if row['groupName'] == 'Development':
                groupNameClubbed = 'Development'
            elif row['groupName'] == 'Technisch consultants':
                groupNameClubbed = 'Technisch consultants and Functioneel consultants'
            elif row['groupName'] == 'Functioneel consultants':
                groupNameClubbed = 'Technisch consultants and Functioneel consultants'
            elif row['groupName'] == 'Intake':
                groupNameClubbed = 'Intake'
            elif row['groupName'] == 'India Intake':
                groupNameClubbed = 'Intake'
            elif row['groupName'] == 'Support 1e lijn':
                groupNameClubbed = 'Support 1e lijn'
            elif row['groupName'] == 'Support 2e lijn':
                groupNameClubbed = 'Support 2e lijn'
            else:
                groupNameClubbed = 'Others'
            return groupNameClubbed
        df['groupNameClubbed'] = df.apply(ClubFunction, axis=1)
        
        dfTicketsByGroup = df.groupby(['groupNameClubbed']).size().reset_index(name='count')
        #Sort
        dfTicketsByGroup['groupNameClubbed_cat'] = pd.Categorical(
            dfTicketsByGroup['groupNameClubbed'], 
            categories=['Intake','Support 1e lijn','Support 2e lijn','Technisch consultants and Functioneel consultants','Development','Others'], 
            ordered=True
        )
        dataTicketsByGroup = dfTicketsByGroup[['groupNameClubbed_cat', 'count']].sort_values('groupNameClubbed_cat')
        data['TicketsByGroup'] = dataTicketsByGroup.to_json(orient ='values')

        #TicketsByPriority
        dataTicketsByPriority = df.groupby(['priority']).size().reset_index(name='count')
        data['TicketsByPriority'] = dataTicketsByPriority.to_json(orient ='values')

        #TicketsBySolutionStatus
        dataTicketsBySolutionStatus = df.groupby(['solutionStatus']).size().reset_index(name='count')
        data['TicketsBySolutionStatus'] = dataTicketsBySolutionStatus.to_json(orient ='values')
        
        #TicketsByCompanyname
        dataTicketsByCompanyname = df.groupby(['companyName']).size().reset_index(name='count')
        data['TicketsByCompanyname'] = dataTicketsByCompanyname.to_dict()  # convert dataframe to dict
        
        return {'data': data}, 200  # return data and 200 OK


class CSTimeAnalysis(Resource):
    def get(self):

        df = pd.read_csv('customerSupportSummary.csv')  # read local CSV
        
        datamain = df.groupby(['Quarter', 'Month'])["New", "Closed", "OpenTicketstodate"].sum().reset_index()

        data = {}

        datanew = df.groupby(['Quarter'])["New"].sum().reset_index()
        datanew['drilldown'] = datanew['Quarter'] + '- New'
        datanew.columns = ['name', 'y', 'drilldown']
        data['seriesNew'] = datanew.to_json(orient ='records')

        dataclosed = df.groupby(['Quarter'])["Closed"].sum().reset_index()
        dataclosed['drilldown'] = dataclosed['Quarter'] + '- Closed'
        dataclosed.columns = ['name', 'y', 'drilldown']
        data['seriesClosed'] = dataclosed.to_json(orient ='records')

        series = []

        for quarter in datamain['Quarter']:
            dftemp = datamain[datamain.Quarter.isin([quarter])][['Month', 'New']]
            temp = {}
            temp['id'] = quarter + '- New'
            temp['data'] = dftemp.to_json(orient ='values')
            temp['data'] = ast.literal_eval(temp['data'])
            series.append(temp)
            
        for quarter in datamain['Quarter']:
            dftemp = datamain[datamain.Quarter.isin([quarter])][['Month', 'Closed']]
            temp = {}
            temp['id'] = quarter + '- Closed'
            temp['data'] = dftemp.to_json(orient ='values')
            temp['data'] = ast.literal_eval(temp['data'])
            series.append(temp)

        data['seriesDrill'] = series
        
        #Group by for TimeAnalysisTicketsByWeek
        dataTicketsByWeek = df.groupby(['Year', 'WeekNumber'])["New", "Closed", "OpenTicketstodate"].sum().reset_index()
        dataTicketsByWeek = dataTicketsByWeek.to_dict()  # convert dataframe to dict
        data['TicketsByWeek'] = dataTicketsByWeek

        return {'data': data}, 200  # return data and 200 OK
        
                    
class CSSLAOpenTickets(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Bij Development', 'In afwachting van externe partij', 'Intern informatie opgevraagd', 'Open', 'Pending', 'Wachten op klantreactie', 'Werkzaamheden ingepland'])]
        df = df[~df.message.isin(['Project', ''])]
        
        data = {}

        #Bar graph: Group by
        datagroup1 = df.groupby(['status', 'priority']).size().reset_index(name='countt')
        pivoted = datagroup1.pivot(index='status', columns= 'priority', values='countt')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)
        dataOpenTickets = {}
        series = []
        for col in flattened.columns:
            if col == 'status':
                dataOpenTickets['categories'] = flattened[col].values.tolist()
            else:
                temp = {}
                temp['name'] = str(col)
                temp['data'] = flattened[col].values.tolist()
                series.append(temp)
        dataOpenTickets['series'] = series  
        data['BarGraph'] = dataOpenTickets  # convert dataframe to dict

        #Group by
        datagroup2 = df.groupby(['currentStatus']).size().reset_index(name='count')
        data['PieSLA'] = datagroup2.to_json(orient ='values')

        #Group by
        datagroup3 = df.groupby(['status']).size().reset_index(name='count')
        data['PieStatus'] = datagroup3.to_json(orient ='values')
        
        return {'data': data}, 200  # return data and 200 OK
        
                    
class CSSLAClosedTickets(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        df = df[~df.closedTime.isin([''])]

        df['closedTime'] = pd.to_datetime(df['closedTime'].str.slice(0,10), format='%Y-%m-%d', errors='ignore')
        filterdate = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        df = df.loc[(df['closedTime'] >= filterdate)]
        
        #Group by
        datagroup1 = df.groupby(['agent', 'solutionStatus']).size().reset_index(name='countt')
        pivoted = datagroup1.pivot(index='agent', columns= 'solutionStatus', values='countt')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)
        dataClosedTickets = {}
        series = []
        for col in flattened.columns:
            if col == 'agent':
                dataClosedTickets['categories'] = flattened[col].values.tolist()
            else:
                temp = {}
                temp['name'] = str(col)
                temp['data'] = flattened[col].values.tolist()
                series.append(temp)
        dataClosedTickets['series'] = series  
        data = dataClosedTickets  # convert dataframe to dict
        
        return {'data': data}, 200  # return data and 200 OK


api.add_resource(CustomerSatisfactionTeams, '/customerSatisfactionTeams')  # add endpoints
api.add_resource(CustomerSatisfactionEmployees, '/customerSatisfactionEmployees')  # add endpoints

api.add_resource(CSOpenIncidents, '/CSOpenIncidents')  # add endpoints

api.add_resource(CSAllOpenTickets, '/CSAllOpenTickets')  # add endpoints

api.add_resource(CSClosedTickets, '/CSClosedTickets')  # add endpoints

api.add_resource(CSTimeAnalysis, '/CSTimeAnalysis')  # add endpoints

api.add_resource(CSSLAOpenTickets, '/CSSLAOpenTickets')  # add endpoints
api.add_resource(CSSLAClosedTickets, '/CSSLAClosedTickets')  # add endpoints

if __name__ == '__main__':
    app.run()  # run our Flask app
