from flask import Flask
from flask_restful import Resource, Api, reqparse
from flask_cors import CORS
import pandas as pd

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
        data = df.groupby(['type', 'priority']).size().reset_index(name='count')
        data = data.to_dict()  # convert dataframe to dict
        return {'data': data}, 200  # return data and 200 OK

                    
class CSOpenIncidentsTicketsByStatus(Resource):
    def get(self):

        df = pd.read_csv('customerSupport.csv')  # read local CSV
        #Apply filters
        df = df[df.status.isin(['Bij Development', 'In afwachting van externe partij', 'Intern informatie opgevraagd', 'Open', 'Pending', 'Wachten op klantreactie', 'Werkzaamheden ingepland'])]
        df = df.loc[(df['message'] == 'Incident / storing')]
        #Group by
        data = df.groupby(['status']).size().reset_index(name='count')
        data = data.to_dict()  # convert dataframe to dict
        return {'data': data}, 200  # return data and 200 OK

                    
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
        data = df.sort_values('groupNameClubbed_cat')

        data = data.to_dict()  # convert dataframe to dict
        return {'data': data}, 200  # return data and 200 OK


api.add_resource(CustomerSatisfactionTeams, '/customerSatisfactionTeams')  # add endpoints
api.add_resource(CustomerSatisfactionEmployees, '/customerSatisfactionEmployees')  # add endpoints
api.add_resource(CSOpenIncidentsTicketsByType, '/CSOpenIncidentsTicketsByType')  # add endpoints
api.add_resource(CSOpenIncidentsTicketsByAgent, '/CSOpenIncidentsTicketsByAgent')  # add endpoints
api.add_resource(CSOpenIncidentsTicketsByTypePriority, '/CSOpenIncidentsTicketsByTypePriority')  # add endpoints
api.add_resource(CSOpenIncidentsTicketsByStatus, '/CSOpenIncidentsTicketsByStatus')  # add endpoints
api.add_resource(CSOpenIncidentsTicketsByGroup, '/CSOpenIncidentsTicketsByGroup')  # add endpoints

if __name__ == '__main__':
    app.run()  # run our Flask app
