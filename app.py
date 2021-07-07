from flask import Flask
from flask_restful import Resource, Api, reqparse
from flask_cors import CORS
import pandas as pd
import ast
import datetime
from calendar import month_name

app = Flask(__name__)
CORS(app)
api = Api(app)

                   
class CustomerSatisfactionTeams(Resource):
    def get(self):

        data = pd.read_csv('customerSatisfactionTeams.csv')  # read local CSV
        data = data[['Team', 'Quarter', 'Rating']]
        pivoted = data.pivot(index='Quarter', columns= 'Team', values='Rating')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)
        data = flattened.to_dict()  # convert dataframe to dict
        return {'data': data}, 200  # return data and 200 OK

                    
class CustomerSatisfactionEmployees(Resource):
    def get(self):

        df = pd.read_csv('customerSatisfactionEmployees.csv')  # read local CSV
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
        
        #TicketsAll
        df= df.fillna('blank')
        data['TicketsAll'] = df[['agent', 'companyName', 'message', 'type', 'priority', 'version', 'environment']].to_dict(orient= 'records')  # convert dataframe to dict
        
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


class CSTimeAnalysisTemp(Resource):
    def get(self):

        df = pd.read_csv('customerSupportSummaryTemp.csv')  # read local CSV
        data = {}

        df['recordDate'] = pd.to_datetime(df['recordDate'].str.slice(0,10), format='%Y-%m-%d', errors='ignore') 
        df = df.sort_values(['recordDate'], ascending=[True])

        df['UnixDate'] = (pd.DatetimeIndex(df['recordDate']).astype(int) // 10**9) * 1000
        
        data['TimeNew'] = df[['UnixDate', 'New']].to_json(orient ='values')
        
        data['TimeClosed'] = df[['UnixDate', 'Closed']].to_json(orient ='values')

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
        
                    
class ServicesTeams(Resource):
    def get(self):

        df = pd.read_csv('servicesTeams.csv')  # read local CSV
        data = {}

        for team in ['Front Office Integration', 'JZD']:
            temp = {}
            df_filtered_team = df[df.team.isin([team])]
            for category in df_filtered_team['category']:
                df_filtered_team_cat = df_filtered_team[df_filtered_team.category.isin([category])]
                df_filtered_team_cat = df_filtered_team_cat.sort_values(['Month number'], ascending=[True])
                temp[category] = df_filtered_team_cat[['Month', 'Target', 'Result']].to_dict()
            data[team] = temp

        for team in ['Overall', 'Customer Support']:
            df_filtered_team = df[df.team.isin([team])]
            df_filtered_team = df_filtered_team.sort_values(['Month number'], ascending=[True])
            data[team] = df_filtered_team[['Month', 'Target', 'Result']].to_dict()     
        
        return {'data': data}, 200  # return data and 200 OK
        
                    
class ServicesEmployees(Resource):
    def get(self):

        df = pd.read_csv('servicesEmployees.csv')  # read local CSV
        df.sort_values(['Employee', 'Month number'], ascending = [True, True], inplace = True)        
        data = {}

        dfTarget = df[df['Current Month'].isin([1])][['Employee', 'Target']]
        dfTarget['drilldown'] = dfTarget['Employee'] + '- Target'
        data['seriesTarget'] = dfTarget.rename(columns={"Employee": "name", "Target": "y"}).to_dict(orient= 'records')

        dfResult = df[df['Current Month'].isin([1])][['Employee', 'Result']]
        dfResult['drilldown'] = dfResult['Employee'] + '- Result'
        data['seriesResult'] = dfResult.rename(columns={"Employee": "name", "Result": "y"}).to_dict(orient= 'records')

        dfDifferenceYTD = df[['Employee', 'DifferenceYTD']].groupby(['Employee'])['DifferenceYTD'].sum().reset_index().round({"DifferenceYTD":0})
        dfDifferenceYTD['drilldown'] = dfDifferenceYTD['Employee'] + '- DifferenceYTD'
        data['seriesDifferenceYTD'] = dfDifferenceYTD.rename(columns={"Employee": "name", "DifferenceYTD": "y"}).to_dict(orient= 'records')

        df['drillTarget'] = df['Employee'] + '- Target'
        dfDrillTarget = df[['drillTarget', 'Month', 'Target']]
        dicDrillTarget = dict(dfDrillTarget.set_index('drillTarget').groupby(level = 0).apply(lambda x : x.to_dict(orient= 'split')))
        drillTargetlist = list(map(lambda a: {'id':a['index'][0], 'name': 'Target', 'data': a['data']}, list(dicDrillTarget.values())))

        df['drillResult'] = df['Employee'] + '- Result'
        dfDrillResult = df[['drillResult', 'Month', 'Result']]
        dicDrillResult = dict(dfDrillResult.set_index('drillResult').groupby(level = 0).apply(lambda x : x.to_dict(orient= 'split')))
        drillResultlist = list(map(lambda a: {'id':a['index'][0], 'name': 'Result', 'data': a['data']}, list(dicDrillResult.values())))

        df['drillDifferenceYTD'] = df['Employee'] + '- DifferenceYTD'
        dfDrillDifferenceYTD = df[['drillDifferenceYTD', 'Month', 'DifferenceYTD']]
        dicDrillDifferenceYTD = dict(dfDrillDifferenceYTD.set_index('drillDifferenceYTD').groupby(level = 0).apply(lambda x : x.to_dict(orient= 'split')))
        drillDifferenceYTDlist = list(map(lambda a: {'id':a['index'][0], 'name': 'Monthly Difference', 'data': a['data']}, list(dicDrillDifferenceYTD.values())))

        data['seriesDrill'] = drillTargetlist + drillResultlist + drillDifferenceYTDlist

        return {'data': data}, 200  # return data and 200 OK
        
                    
class DevelopmentProcessAdherenceScore(Resource):
    def get(self):

        df = pd.read_csv('development.csv')  # read local CSV

        dftemp = df[['Employee', 'Team', 'Month', 'MonthNumber', 'Process Adherence Score']].sort_values(['MonthNumber'], ascending=[True])
        pivoted = dftemp.pivot(index=['Employee', 'Team'], columns= 'Month', values='Process Adherence Score')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)

        def Diff(li1, li2):
            return list(set(li1) - set(li2)) + list(set(li2) - set(li1))
        month_lookup = list(month_name)
        col = flattened.columns.to_list()
        coldiff = Diff(col, ['Employee', 'Team'])
        colsort = sorted(coldiff, key=month_lookup.index)
        colsort = ['Employee', 'Team'] + colsort
        flattened = flattened[colsort]

        #data = dict(flattened.set_index('Team').groupby(level = 0).\
        #    apply(lambda x : x.to_dict(orient= 'records')))
        data = flattened.to_dict(orient= 'records')

        return {'data': data}, 200  # return data and 200 OK
        
                    
class DevelopmentTechnicalScore(Resource):
    def get(self):

        df = pd.read_csv('development.csv')  # read local CSV

        dftemp = df[['Employee', 'Team', 'Month', 'MonthNumber', 'Technical Score']].sort_values(['MonthNumber'], ascending=[True])
        pivoted = dftemp.pivot(index=['Employee', 'Team'], columns= 'Month', values='Technical Score')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)

        def Diff(li1, li2):
            return list(set(li1) - set(li2)) + list(set(li2) - set(li1))
        month_lookup = list(month_name)
        col = flattened.columns.to_list()
        coldiff = Diff(col, ['Employee', 'Team'])
        colsort = sorted(coldiff, key=month_lookup.index)
        colsort = ['Employee', 'Team'] + colsort
        flattened = flattened[colsort]

        #data = dict(flattened.set_index('Team').groupby(level = 0).\
        #    apply(lambda x : x.to_dict(orient= 'records')))
        data = flattened.to_dict(orient= 'records')

        return {'data': data}, 200  # return data and 200 OK
        
                    
class DevelopmentTotalCommits(Resource):
    def get(self):

        df = pd.read_csv('development.csv')  # read local CSV

        dftemp = df[['Employee', 'Team', 'Month', 'MonthNumber', 'Total Commits']].sort_values(['MonthNumber'], ascending=[True])
        pivoted = dftemp.pivot(index=['Employee', 'Team'], columns= 'Month', values='Total Commits')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)

        def Diff(li1, li2):
            return list(set(li1) - set(li2)) + list(set(li2) - set(li1))
        month_lookup = list(month_name)
        col = flattened.columns.to_list()
        coldiff = Diff(col, ['Employee', 'Team'])
        colsort = sorted(coldiff, key=month_lookup.index)
        colsort = ['Employee', 'Team'] + colsort
        flattened = flattened[colsort]

        #data = dict(flattened.set_index('Team').groupby(level = 0).\
        #    apply(lambda x : x.to_dict(orient= 'records')))
        data = flattened.to_dict(orient= 'records')

        return {'data': data}, 200  # return data and 200 OK
        
                    
class DevelopmentProductivityScore(Resource):
    def get(self):

        df = pd.read_csv('development.csv')  # read local CSV

        dftemp = df[['Employee', 'Team', 'Month', 'MonthNumber', 'Productivity Score']].sort_values(['MonthNumber'], ascending=[True])
        pivoted = dftemp.pivot(index=['Employee', 'Team'], columns= 'Month', values='Productivity Score')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)

        def Diff(li1, li2):
            return list(set(li1) - set(li2)) + list(set(li2) - set(li1))
        month_lookup = list(month_name)
        col = flattened.columns.to_list()
        coldiff = Diff(col, ['Employee', 'Team'])
        colsort = sorted(coldiff, key=month_lookup.index)
        colsort = ['Employee', 'Team'] + colsort
        flattened = flattened[colsort]

        #data = dict(flattened.set_index('Team').groupby(level = 0).\
        #    apply(lambda x : x.to_dict(orient= 'records')))
        data = flattened.to_dict(orient= 'records')

        return {'data': data}, 200  # return data and 200 OK
       
                    
class DevelopmentAverageVelocity(Resource):
    def get(self):

        df = pd.read_csv('development.csv')  # read local CSV

        df = df[['Team', 'Month', 'Average Velocity']].groupby(['Team', 'Month'])['Average Velocity'].mean().reset_index(name='Average Velocity').round({"Average Velocity":0})
        pivoted = df.pivot(index=['Team'], columns= 'Month', values='Average Velocity')
        flattened = pd.DataFrame(pivoted.to_records()).fillna(0)

        def Diff(li1, li2):
            return list(set(li1) - set(li2)) + list(set(li2) - set(li1))
        month_lookup = list(month_name)
        col = flattened.columns.to_list()
        coldiff = Diff(col, ['Team'])
        colsort = sorted(coldiff, key=month_lookup.index)
        colsort = ['Team'] + colsort
        flattened = flattened[colsort]

        data = flattened.to_dict(orient= 'records')

        return {'data': data}, 200  # return data and 200 OK
       
                    
class DevelopmentCodeReviews(Resource):
    def get(self):

        df = pd.read_csv('development.csv')  # read local CSV
        month_lookup = list(month_name)
        df['Month_Rank'] = df['Month'].map(month_lookup.index)
        df.sort_values(['Team', 'Employee', 'Month_Rank'], ascending = [True, True, True], inplace = True)        
        data = {}

        dfCR = df[df.recentMonth.isin([1])][['Team', 'Employee', 'Code Review Score']]
        dfCR['drilldown'] = dfCR['Employee'] + '- CR'
        data['seriesCR'] = dfCR.rename(columns={"Employee": "name", "Code Review Score": "y"}).to_dict(orient= 'records')

        dfCRR = df[df.recentMonth.isin([1])][['Team', 'Employee', 'Code Review Received Score']]
        dfCRR['drilldown'] = dfCRR['Employee'] + '- CRR'
        data['seriesCRR'] = dfCRR.rename(columns={"Employee": "name", "Code Review Received Score": "y"}).to_dict(orient= 'records')

        df['drillCR'] = df['Employee'] + '- CR'
        dfdrillCR = df[['drillCR', 'Month', 'Code Review Score']]
        dicDrillCR = dict(dfdrillCR.set_index('drillCR').groupby(level = 0).apply(lambda x : x.to_dict(orient= 'split')))
        drillCRlist = list(map(lambda a: {'id':a['index'][0], 'name': 'Code Review Score', 'data': a['data']}, list(dicDrillCR.values())))

        df['drillCRR'] = df['Employee'] + '- CRR'
        dfdrillCRR = df[['drillCRR', 'Month', 'Code Review Received Score']]
        dicDrillCRR = dict(dfdrillCRR.set_index('drillCRR').groupby(level = 0).apply(lambda x : x.to_dict(orient= 'split')))
        drillCRRlist = list(map(lambda a: {'id':a['index'][0], 'name': 'Code Review Received Score', 'data': a['data']}, list(dicDrillCRR.values())))

        data['seriesDrill'] = drillCRlist + drillCRRlist

        return {'data': data}, 200  # return data and 200 OK


api.add_resource(CustomerSatisfactionTeams, '/CustomerSatisfactionTeams')  # add endpoints
api.add_resource(CustomerSatisfactionEmployees, '/CustomerSatisfactionEmployees')  # add endpoints

api.add_resource(CSOpenIncidents, '/CSOpenIncidents')  # add endpoints
api.add_resource(CSAllOpenTickets, '/CSAllOpenTickets')  # add endpoints
api.add_resource(CSClosedTickets, '/CSClosedTickets')  # add endpoints
api.add_resource(CSTimeAnalysis, '/CSTimeAnalysis')  # add endpoints
api.add_resource(CSTimeAnalysisTemp, '/CSTimeAnalysisTemp')  # add endpoints

api.add_resource(CSSLAOpenTickets, '/CSSLAOpenTickets')  # add endpoints
api.add_resource(CSSLAClosedTickets, '/CSSLAClosedTickets')  # add endpoints

api.add_resource(ServicesTeams, '/ServicesTeams')  # add endpoints
api.add_resource(ServicesEmployees, '/ServicesEmployees')  # add endpoints

api.add_resource(DevelopmentProcessAdherenceScore, '/DevelopmentProcessAdherenceScore')  # add endpoints
api.add_resource(DevelopmentTechnicalScore, '/DevelopmentTechnicalScore')  # add endpoints
api.add_resource(DevelopmentTotalCommits, '/DevelopmentTotalCommits')  # add endpoints
api.add_resource(DevelopmentProductivityScore, '/DevelopmentProductivityScore')  # add endpoints
api.add_resource(DevelopmentAverageVelocity, '/DevelopmentAverageVelocity')  # add endpoints
api.add_resource(DevelopmentCodeReviews, '/DevelopmentCodeReviews')  # add endpoints

if __name__ == '__main__':
    app.run()  # run our Flask app
