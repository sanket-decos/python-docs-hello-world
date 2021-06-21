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


api.add_resource(Users, '/users')  # add endpoints
api.add_resource(CustomerSatisfactionTeams, '/customerSatisfactionTeams')  # add endpoints
api.add_resource(CustomerSatisfactionEmployees, '/customerSatisfactionEmployees')  # add endpoints

if __name__ == '__main__':
    app.run()  # run our Flask app
