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
        parser = reqparse.RequestParser()  # initialize
        parser.add_argument('TeamName', required=True)  # add args
        args = parser.parse_args()  # parse arguments to dictionary

        #data = pd.read_csv('customerSatisfactionEmployees.csv')  # read local CSV
        df = pd.read_csv('customerSatisfactionEmployees.csv')  # read local CSV
        data = df.loc[df['TeamName'] == args['TeamName']][['Employee', 'Communication', 'Knowledge', 'TotalImpression']]
        data = data.to_dict()  # convert dataframe to dict
        return {'data': data}, 200  # return data and 200 OK

                    
class CSOpenIncidentsTicketsByType(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Bij Development', 'In afwachting van externe partij', 'Intern informatie opgevraagd', 'Open', 'Pending', 'Wachten op klantreactie', 'Werkzaamheden ingepland'])]
        df = df.loc[(df['message'] == 'Incident / storing')]
        #Group by
        data = df.groupby(['type']).size().reset_index(name='count')
        data = data.to_dict()  # convert dataframe to dict
        return {'data': data}, 200  # return data and 200 OK

                    
class CSOpenIncidentsTicketsByAgent(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Bij Development', 'In afwachting van externe partij', 'Intern informatie opgevraagd', 'Open', 'Pending', 'Wachten op klantreactie', 'Werkzaamheden ingepland'])]
        df = df.loc[(df['message'] == 'Incident / storing')]
        #Group by
        data = df.groupby(['agent']).size().reset_index(name='count')
        data = data.to_dict()  # convert dataframe to dict
        return {'data': data}, 200  # return data and 200 OK

                    
class CSOpenIncidentsTicketsByTypePriority(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Bij Development', 'In afwachting van externe partij', 'Intern informatie opgevraagd', 'Open', 'Pending', 'Wachten op klantreactie', 'Werkzaamheden ingepland'])]
        df = df.loc[(df['message'] == 'Incident / storing')]
        #Group by
        datagroup = df.groupby(['type', 'priority']).size().reset_index(name='countt')
        pivoted = datagroup.pivot(index='type', columns= 'priority', values='countt')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)

        data = flattened.to_dict()  # convert dataframe to dict
        return {'data': data}, 200  # return data and 200 OK

                    
class CSOpenIncidentsTicketsByStatus(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Bij Development', 'In afwachting van externe partij', 'Intern informatie opgevraagd', 'Open', 'Pending', 'Wachten op klantreactie', 'Werkzaamheden ingepland'])]
        df = df.loc[(df['message'] == 'Incident / storing')]
        #Group by
        data = df.groupby(['status']).size().reset_index(name='count')
        #data = data.to_dict()  # convert dataframe to dict
        json_values = data.to_json(orient ='values')
        return {'data': json_values}, 200  # return data and 200 OK

                    
class CSOpenIncidentsTicketsByGroup(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Bij Development', 'In afwachting van externe partij', 'Intern informatie opgevraagd', 'Open', 'Pending', 'Wachten op klantreactie', 'Werkzaamheden ingepland'])]
        df = df.loc[(df['message'] == 'Incident / storing')]

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
        df = df.groupby(['groupNameClubbed']).size().reset_index(name='count')
        #Sort
        df['groupNameClubbed_cat'] = pd.Categorical(
            df['groupNameClubbed'], 
            categories=['Intake','Support 1e lijn','Support 2e lijn','Technisch consultants and Functioneel consultants','Development','Others'], 
            ordered=True
        )
        data = df[['groupNameClubbed_cat', 'count']].sort_values('groupNameClubbed_cat')

        #data = data.to_dict()  # convert dataframe to dict
        json_values = data.to_json(orient ='values')
        return {'data': json_values}, 200  # return data and 200 OK




                    
class CSAllOpenTicketsTicketsByAgentMessage(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Bij Development', 'In afwachting van externe partij', 'Intern informatie opgevraagd', 'Open', 'Pending', 'Wachten op klantreactie', 'Werkzaamheden ingepland'])]
        df = df[~df.message.isin(['Project', ''])]
        #Group by
        datagroup = df.groupby(['agent', 'message']).size().reset_index(name='countt')
        pivoted = datagroup.pivot(index='agent', columns= 'message', values='countt')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)
        data = flattened.to_dict()  # convert dataframe to dict

        return {'data': data}, 200  # return data and 200 OK

                    
class CSAllOpenTicketsTicketsByTypeMessage(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Bij Development', 'In afwachting van externe partij', 'Intern informatie opgevraagd', 'Open', 'Pending', 'Wachten op klantreactie', 'Werkzaamheden ingepland'])]
        df = df[~df.message.isin(['Project', ''])]
        #Group by
        datagroup = df.groupby(['type', 'message']).size().reset_index(name='countt')
        pivoted = datagroup.pivot(index='type', columns= 'message', values='countt')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)
        data = flattened.to_dict()  # convert dataframe to dict
        
        return {'data': data}, 200  # return data and 200 OK

                    
class CSAllOpenTicketsTicketsByTypePriority(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Bij Development', 'In afwachting van externe partij', 'Intern informatie opgevraagd', 'Open', 'Pending', 'Wachten op klantreactie', 'Werkzaamheden ingepland'])]
        df = df[~df.message.isin(['Project', ''])]
        #Group by
        datagroup = df.groupby(['type', 'priority']).size().reset_index(name='countt')
        pivoted = datagroup.pivot(index='type', columns= 'priority', values='countt')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)
        data = flattened.to_dict()  # convert dataframe to dict
        
        return {'data': data}, 200  # return data and 200 OK

                    
class CSAllOpenTicketsTicketsByCompanyname(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Bij Development', 'In afwachting van externe partij', 'Intern informatie opgevraagd', 'Open', 'Pending', 'Wachten op klantreactie', 'Werkzaamheden ingepland'])]
        df = df[~df.message.isin(['Project', ''])]
        #Group by
        data = df.groupby(['companyName']).size().reset_index(name='count')
        data = data.to_dict()  # convert dataframe to dict
        
        return {'data': data}, 200  # return data and 200 OK




class CSClosedTicketsTicketsByAgentMessage(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Closed', 'In ontwikkeling', 'Resolved'])]
        df = df[df.message.isin(['Feedback', 'Incident / storing', 'Project', 'Vraag', ])]
        #Group by
        datagroup = df.groupby(['agent', 'message']).size().reset_index(name='countt')
        pivoted = datagroup.pivot(index='agent', columns= 'message', values='countt')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)
        data = flattened.to_dict()  # convert dataframe to dict
        
        return {'data': data}, 200  # return data and 200 OK

                    
class CSClosedTicketsTicketsByGroup(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Closed', 'In ontwikkeling', 'Resolved'])]
        df = df[df.message.isin(['Feedback', 'Incident / storing', 'Project', 'Vraag', ])]

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
        df = df.groupby(['groupNameClubbed']).size().reset_index(name='count')
        #Sort
        df['groupNameClubbed_cat'] = pd.Categorical(
            df['groupNameClubbed'], 
            categories=['Intake','Support 1e lijn','Support 2e lijn','Technisch consultants and Functioneel consultants','Development','Others'], 
            ordered=True
        )
        data = df[['groupNameClubbed_cat', 'count']].sort_values('groupNameClubbed_cat')

        #data = data.to_dict()  # convert dataframe to dict
        json_values = data.to_json(orient ='values')
        
        return {'data': json_values}, 200  # return data and 200 OK

                    
class CSClosedTicketsTicketsByPriority(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Closed', 'In ontwikkeling', 'Resolved'])]
        df = df[df.message.isin(['Feedback', 'Incident / storing', 'Project', 'Vraag', ])]
        #Group by
        data = df.groupby(['priority']).size().reset_index(name='count')
        #data = data.to_dict()  # convert dataframe to dict
        json_values = data.to_json(orient ='values')
        return {'data': json_values}, 200  # return data and 200 OK

                    
class CSClosedTicketsTicketsBySolutionStatus(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Closed', 'In ontwikkeling', 'Resolved'])]
        df = df[df.message.isin(['Feedback', 'Incident / storing', 'Project', 'Vraag', ])]
        #Group by
        data = df.groupby(['solutionStatus']).size().reset_index(name='count')
        #data = data.to_dict()  # convert dataframe to dict
        json_values = data.to_json(orient ='values')
        return {'data': json_values}, 200  # return data and 200 OK

                    
class CSClosedTicketsTicketsByCompanyname(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Closed', 'In ontwikkeling', 'Resolved'])]
        df = df[df.message.isin(['Feedback', 'Incident / storing', 'Project', 'Vraag', ])]
        #Group by
        data = df.groupby(['companyName']).size().reset_index(name='count')
        data = data.to_dict()  # convert dataframe to dict
        
        return {'data': data}, 200  # return data and 200 OK

                    
class CSTimeAnalysisTicketsByMonthQuarter(Resource):
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

        #data = data.replace("'", "")        
        return {'data': data}, 200  # return data and 200 OK

                    
class CSTimeAnalysisTicketsByWeek(Resource):
    def get(self):

        df = pd.read_csv('customerSupportSummary.csv')  # read local CSV
        #Group by
        data = df.groupby(['Year', 'WeekNumber'])["New", "Closed", "OpenTicketstodate"].sum().reset_index()
        
        data = data.to_dict()  # convert dataframe to dict
        
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
        data['BarGraph'] = flattened.to_dict()  # convert dataframe to dict

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
        data = flattened.to_dict()  # convert dataframe to dict
        
        return {'data': data}, 200  # return data and 200 OK


api.add_resource(CustomerSatisfactionTeams, '/customerSatisfactionTeams')  # add endpoints
api.add_resource(CustomerSatisfactionEmployees, '/customerSatisfactionEmployees')  # add endpoints

api.add_resource(CSOpenIncidentsTicketsByType, '/CSOpenIncidentsTicketsByType')  # add endpoints
api.add_resource(CSOpenIncidentsTicketsByAgent, '/CSOpenIncidentsTicketsByAgent')  # add endpoints
api.add_resource(CSOpenIncidentsTicketsByTypePriority, '/CSOpenIncidentsTicketsByTypePriority')  # add endpoints
api.add_resource(CSOpenIncidentsTicketsByStatus, '/CSOpenIncidentsTicketsByStatus')  # add endpoints
api.add_resource(CSOpenIncidentsTicketsByGroup, '/CSOpenIncidentsTicketsByGroup')  # add endpoints

api.add_resource(CSAllOpenTicketsTicketsByAgentMessage, '/CSAllOpenTicketsTicketsByAgentMessage')  # add endpoints
api.add_resource(CSAllOpenTicketsTicketsByTypeMessage, '/CSAllOpenTicketsTicketsByTypeMessage')  # add endpoints
api.add_resource(CSAllOpenTicketsTicketsByTypePriority, '/CSAllOpenTicketsTicketsByTypePriority')  # add endpoints
api.add_resource(CSAllOpenTicketsTicketsByCompanyname, '/CSAllOpenTicketsTicketsByCompanyname')  # add endpoints

api.add_resource(CSClosedTicketsTicketsByAgentMessage, '/CSClosedTicketsTicketsByAgentMessage')  # add endpoints
api.add_resource(CSClosedTicketsTicketsByGroup, '/CSClosedTicketsTicketsByGroup')  # add endpoints
api.add_resource(CSClosedTicketsTicketsByPriority, '/CSClosedTicketsTicketsByPriority')  # add endpoints
api.add_resource(CSClosedTicketsTicketsBySolutionStatus, '/CSClosedTicketsTicketsBySolutionStatus')  # add endpoints
api.add_resource(CSClosedTicketsTicketsByCompanyname, '/CSClosedTicketsTicketsByCompanyname')  # add endpoints

api.add_resource(CSTimeAnalysisTicketsByMonthQuarter, '/CSTimeAnalysisTicketsByMonthQuarter')  # add endpoints
api.add_resource(CSTimeAnalysisTicketsByWeek, '/CSTimeAnalysisTicketsByWeek')  # add endpoints

api.add_resource(CSSLAOpenTickets, '/CSSLAOpenTickets')  # add endpoints
api.add_resource(CSSLAClosedTickets, '/CSSLAClosedTickets')  # add endpoints

if __name__ == '__main__':
    app.run()  # run our Flask app
