from nba_api.stats.endpoints import boxscoretraditionalv2
from nba_api.stats.endpoints import leaguegamefinder
import requests
import pandas
import pyodbc
import sqlalchemy
import datetime
import schedule,time
import gspread_pandas
import re
import xml.etree.ElementTree as et
import requests
import numpy as np
from scipy import stats
import tabula
#from Rz_Send_Email import *
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from lxml import etree
import AFL_analysis


#test row 
headers = {
    'Host': 'stats.nba.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://stats.nba.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true'
}

class Rz_NBA():

    headers = {
    'Host': 'stats.nba.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://stats.nba.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true'
    
    }

    def write_pandas_into_sql_table(self,data_frame,database,table_name,primary_key_header_dict={}):
        #primary_key_header_dict={data_frame_position_id:'Primary_Key_Column_Header'}
        sql_clause=''
        if primary_key_header_dict:
            # if there is primary key, insert if primary key doesn't exist in table, do nothing if primary key exists in table

            for index,row in data_frame.iterrows():
                where_clause=''
                data_list=[]
                for data in row:
                    if data:
                        data_list.append(data)
                    else:
                        data_list.append('')
                    data=str(data_list)[1:-1]

                where_clause=' Where '
                for col_no in primary_key_header_dict:
                    key=row[col_no]
                    header=primary_key_header_dict[col_no]
                    where_clause=where_clause+header+'='+str(key)+' and '
                where_clause=where_clause[:-5]
                sql_clause=r"IF NOT EXISTS (SELECT * FROM {a}{b}) INSERT INTO {a} values({c})".format(a=table_name,b=where_clause,c=data)
                print(sql_clause)
                database.cursor().execute(sql_clause)
                database.commit()

        else:
            # if there is no primary key, insert into will do.
            for index,row in data_frame.iterrows():
                where_clause=''
                data_list=[]
                for data in row:
                    data_list.append(data)
                    data=str(data_list)[1:-1]
                sql_clause="INSERT INTO {} values({})".format(table_name,data)
                database.cursor().execute(sql_clause)
                database.commit()

    def write_player_box_score_into_database(self,game_id,database,table_name):
        bst=boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        x=bst.get_data_frames()
        bs_df=x[0]
        #bs_df.to_sql('player_boxscore',dw,if_exists='append')
        self.write_pandas_into_sql_table(bs_df,database=database,table_name=table_name,primary_key_header_dict={0:'game_id',4:'player_id'})
        
    def create_empty_dataframe_for_player_boxscore(self):
        columns=['GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_CITY', 'PLAYER_ID',
       'PLAYER_NAME', 'START_POSITION', 'COMMENT', 'MIN', 'FGM', 'FGA',
       'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB',
       'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'PLUS_MINUS','GAME_DATE']
        x=pandas.DataFrame(columns=columns,dtype=float)
        #print(x)
        return x
    
    def get_player_boxscore_df_base_on_date(self,date_from,date_to):
        
        player_box_score_df=self.create_empty_dataframe_for_player_boxscore()

        try:
            game=leaguegamefinder.LeagueGameFinder(date_from_nullable=date_from,date_to_nullable=date_to,headers=self.headers)
            x=game.get_data_frames()
            game_df=x[0]
            game_df['GAME_DATE']=pandas.to_datetime(game_df['GAME_DATE'])
            game_df=game_df.loc[:,'GAME_ID':'GAME_DATE'].drop_duplicates(subset=['GAME_ID'],keep='last').reset_index(drop=True)
            print(game_df)
            #print(game_id_series)
            

            #loop through all id
            for index,row in game_df.iterrows():
                id=row[0]
                game_date=row[1]
                bs_df=boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=id,headers=self.headers).get_data_frames()[0]
                bs_df['GAME_DATE']=game_date
                player_box_score_df=player_box_score_df.append(bs_df,sort=False)
                player_box_score_df['MIN']=player_box_score_df['MIN'].apply(get_mins)
                #print(player_box_score_df)
        except Exception:
            print('no new boxscore generated')

        return player_box_score_df

    def read_sql_into_df(self,database,table):
        df=pandas.read_sql("select * from "+table,database)
        return df
    
    def get_sportsbet_odds_df(self):

        s=requests.get('http://affiliate.sportsbet.com.au/xmlfeeds/Basketball.xml')
        xroot=et.fromstring(s.text)
        xml_list=[]

        for child in xroot[1][0]:
            for grandchild in child:
                for grandgrandchild in grandchild:
                    for gggc in grandgrandchild:
                        #print(grandgrandchild.attrib)
                        type=grandchild.attrib.get('Type')
                        Selc_name=grandgrandchild.attrib.get('EventSelectionName')
                        player_name=''
                        player_market=''
                        over_odd=None
                        under_odd=None
                        try:
                            Odds=float(gggc.attrib.get('Odds'))
                        except Exception:
                            Odds=None

                        if ' - ' in type and type:
                            player_name=type[:type.find(' - ')]
                            player_market=type[type.find(' - ')+3:]
                        if re.search('(Over|Under)$',Selc_name):
                            over_under=re.search('Over|Under',Selc_name).group(0)
                        else:
                            over_under=''
                        
                        if over_under=='Over':
                            over_odd=Odds
                            under_odd=None
                        elif over_under=='Under':
                            over_odd=None
                            under_odd=Odds
                        
                        line=None
                        try:
                            line=float(gggc.attrib.get('Line'))
                        except Exception:
                            pass
                        


                        item=[child.attrib.get('EventId'),child.attrib.get('EventName'),child.attrib.get('EventDate')[:10],child.attrib.get('Group'),type,player_name,player_market,grandgrandchild.attrib.get('BetSelectionID'),Selc_name,over_under,Odds,over_odd,under_odd,line]
                        xml_list.append(item)
            
        df=pandas.DataFrame(xml_list,columns=['Event_id','Event_name','Event_Date','Group','Type','Player_Name','Player_Market','Bet_Selection_Id','Event_Selection_Name','Over_Under','Odds','Over_Odds','Under_Odds','Line'])
        return df

    def filter_players_df_only(self,raw_dataframe):
        isplayer=np.where(raw_dataframe['Type'].str.contains('- Pts|- Points|- Assists|- Rebounds|- Made Threes|- Pts + Ast|- Pts + Reb + Ast|- Pts + Reb|- Reb + Ast'),True,False)
        players_df=raw_dataframe[isplayer]
        players_df.groupby(['Event_name','Player_Name','Event_Date','Player_Market']).agg({'Event_name':'first','Player_Name':'first','Player_Market':'first','Event_id':'first','Event_Date':'first','Type':'first','Over_Odds':'sum','Under_Odds':'sum','Line':'mean'}).reset_index(drop=True)
        return players_df
    
    def list_bs_df_kpis_vertically(self,box_score_df):
                #filter=box_score_df.notnull()['MIN']
        filter=np.where(box_score_df['MIN']>0,True,False)
        combined_bs_df=box_score_df[filter]

        columns=['PLAYER_ID','Player_Name','Player_Market','COUNT','MEAN','STDEV']
        nba_analytics_pts=pandas.DataFrame(columns=columns,dtype=float)
        nba_analytics_ast=pandas.DataFrame(columns=columns,dtype=float)
        nba_analytics_reb=pandas.DataFrame(columns=columns,dtype=float)
        nba_analytics_pa=pandas.DataFrame(columns=columns,dtype=float)
        nba_analytics_par=pandas.DataFrame(columns=columns,dtype=float)
        nba_analytics_ar=pandas.DataFrame(columns=columns,dtype=float)
        nba_analytics_m3=pandas.DataFrame(columns=columns,dtype=float)
        nba_analytics_pr=pandas.DataFrame(columns=columns,dtype=float)

        nba_analytics_pts['PLAYER_ID']=combined_bs_df['PLAYER_ID']
        nba_analytics_ast['PLAYER_ID']=combined_bs_df['PLAYER_ID']
        nba_analytics_reb['PLAYER_ID']=combined_bs_df['PLAYER_ID']
        nba_analytics_pa['PLAYER_ID']=combined_bs_df['PLAYER_ID']
        nba_analytics_par['PLAYER_ID']=combined_bs_df['PLAYER_ID']
        nba_analytics_ar['PLAYER_ID']=combined_bs_df['PLAYER_ID']
        nba_analytics_m3['PLAYER_ID']=combined_bs_df['PLAYER_ID']
        nba_analytics_pr['PLAYER_ID']=combined_bs_df['PLAYER_ID']

        nba_analytics_pts['Player_Name']=combined_bs_df['PLAYER_NAME']
        nba_analytics_ast['Player_Name']=combined_bs_df['PLAYER_NAME']
        nba_analytics_reb['Player_Name']=combined_bs_df['PLAYER_NAME']
        nba_analytics_pa['Player_Name']=combined_bs_df['PLAYER_NAME']
        nba_analytics_par['Player_Name']=combined_bs_df['PLAYER_NAME']
        nba_analytics_ar['Player_Name']=combined_bs_df['PLAYER_NAME']
        nba_analytics_m3['Player_Name']=combined_bs_df['PLAYER_NAME']
        nba_analytics_pr['Player_Name']=combined_bs_df['PLAYER_NAME']

        nba_analytics_pts['Player_Market']='Points'
        nba_analytics_ast['Player_Market']='Assists'
        nba_analytics_reb['Player_Market']='Rebounds'
        nba_analytics_pa['Player_Market']='Pts + Ast'
        nba_analytics_par['Player_Market']='Pts + Reb + Ast'
        nba_analytics_ar['Player_Market']='Reb + Ast'
        nba_analytics_m3['Player_Market']='Made Threes'
        nba_analytics_pr['Player_Market']='Pts + Reb'


        nba_analytics_pts['GAME_ID']=combined_bs_df['GAME_ID']
        nba_analytics_ast['GAME_ID']=combined_bs_df['GAME_ID']
        nba_analytics_reb['GAME_ID']=combined_bs_df['GAME_ID']
        nba_analytics_pa['GAME_ID']=combined_bs_df['GAME_ID']
        nba_analytics_par['GAME_ID']=combined_bs_df['GAME_ID']
        nba_analytics_ar['GAME_ID']=combined_bs_df['GAME_ID']
        nba_analytics_m3['GAME_ID']=combined_bs_df['GAME_ID']
        nba_analytics_pr['GAME_ID']=combined_bs_df['GAME_ID']




        nba_analytics_pts['TEAM_ID']=combined_bs_df['TEAM_ID']
        nba_analytics_ast['TEAM_ID']=combined_bs_df['TEAM_ID']
        nba_analytics_reb['TEAM_ID']=combined_bs_df['TEAM_ID']
        nba_analytics_pa['TEAM_ID']=combined_bs_df['TEAM_ID']
        nba_analytics_par['TEAM_ID']=combined_bs_df['TEAM_ID']
        nba_analytics_ar['TEAM_ID']=combined_bs_df['TEAM_ID']
        nba_analytics_m3['TEAM_ID']=combined_bs_df['TEAM_ID']
        nba_analytics_pr['TEAM_ID']=combined_bs_df['TEAM_ID']

        nba_analytics_pts['MIN']=combined_bs_df['MIN']
        nba_analytics_ast['MIN']=combined_bs_df['MIN']
        nba_analytics_reb['MIN']=combined_bs_df['MIN']
        nba_analytics_pa['MIN']=combined_bs_df['MIN']
        nba_analytics_par['MIN']=combined_bs_df['MIN']
        nba_analytics_ar['MIN']=combined_bs_df['MIN']
        nba_analytics_m3['MIN']=combined_bs_df['MIN']
        nba_analytics_pr['MIN']=combined_bs_df['MIN']


        nba_analytics_pts['COUNT']=pandas.to_numeric(combined_bs_df['PTS'])
        nba_analytics_pts['MEAN']=pandas.to_numeric(combined_bs_df['PTS'])
        nba_analytics_pts['STDEV']=pandas.to_numeric(combined_bs_df['PTS'])

        nba_analytics_ast['COUNT']=pandas.to_numeric(combined_bs_df['AST'])
        nba_analytics_ast['MEAN']=pandas.to_numeric(combined_bs_df['AST'])
        nba_analytics_ast['STDEV']=pandas.to_numeric(combined_bs_df['AST'])

        nba_analytics_reb['COUNT']=pandas.to_numeric(combined_bs_df['REB'])
        nba_analytics_reb['MEAN']=pandas.to_numeric(combined_bs_df['REB'])
        nba_analytics_reb['STDEV']=pandas.to_numeric(combined_bs_df['REB'])

        nba_analytics_pa['COUNT']=pandas.to_numeric(combined_bs_df['PTS'])+pandas.to_numeric(combined_bs_df['AST'])
        nba_analytics_pa['MEAN']=pandas.to_numeric(combined_bs_df['PTS'])+pandas.to_numeric(combined_bs_df['AST'])
        nba_analytics_pa['STDEV']=pandas.to_numeric(combined_bs_df['PTS'])+pandas.to_numeric(combined_bs_df['AST'])

        nba_analytics_par['COUNT']=pandas.to_numeric(combined_bs_df['PTS'])+pandas.to_numeric(combined_bs_df['AST'])+pandas.to_numeric(combined_bs_df['REB'])
        nba_analytics_par['MEAN']=pandas.to_numeric(combined_bs_df['PTS'])+pandas.to_numeric(combined_bs_df['AST'])+pandas.to_numeric(combined_bs_df['REB'])
        nba_analytics_par['STDEV']=pandas.to_numeric(combined_bs_df['PTS'])+pandas.to_numeric(combined_bs_df['AST'])+pandas.to_numeric(combined_bs_df['REB'])

        nba_analytics_ar['COUNT']=pandas.to_numeric(combined_bs_df['AST'])+pandas.to_numeric(combined_bs_df['REB'])
        nba_analytics_ar['MEAN']=pandas.to_numeric(combined_bs_df['AST'])+pandas.to_numeric(combined_bs_df['REB'])
        nba_analytics_ar['STDEV']=pandas.to_numeric(combined_bs_df['AST'])+pandas.to_numeric(combined_bs_df['REB'])

        nba_analytics_m3['COUNT']=pandas.to_numeric(combined_bs_df['FG3M'])
        nba_analytics_m3['MEAN']=pandas.to_numeric(combined_bs_df['FG3M'])
        nba_analytics_m3['STDEV']=pandas.to_numeric(combined_bs_df['FG3M'])

        nba_analytics_pr['COUNT']=pandas.to_numeric(combined_bs_df['PTS'])+pandas.to_numeric(combined_bs_df['REB'])
        nba_analytics_pr['MEAN']=pandas.to_numeric(combined_bs_df['PTS'])+pandas.to_numeric(combined_bs_df['REB'])
        nba_analytics_pr['STDEV']=pandas.to_numeric(combined_bs_df['PTS'])+pandas.to_numeric(combined_bs_df['REB'])

        #nba_analytics_pts=nba_analytics_pts.groupby(['PLAYER_ID']).agg({'PLAYER_ID':'first','Player_Name':'first','Player_Market':'first','COUNT':'count','MEAN':'mean','STDEV':'std'}).reset_index(drop=True)
        #nba_analytics_ast=nba_analytics_ast.groupby(['PLAYER_ID']).agg({'PLAYER_ID':'first','Player_Name':'first','Player_Market':'first','COUNT':'count','MEAN':'mean','STDEV':'std'}).reset_index(drop=True)
        #nba_analytics_reb=nba_analytics_reb.groupby(['PLAYER_ID']).agg({'PLAYER_ID':'first','Player_Name':'first','Player_Market':'first','COUNT':'count','MEAN':'mean','STDEV':'std'}).reset_index(drop=True)
        #nba_analytics_pa=nba_analytics_pa.groupby(['PLAYER_ID']).agg({'PLAYER_ID':'first','Player_Name':'first','Player_Market':'first','COUNT':'count','MEAN':'mean','STDEV':'std'}).reset_index(drop=True)
        #nba_analytics_par=nba_analytics_par.groupby(['PLAYER_ID']).agg({'PLAYER_ID':'first','Player_Name':'first','Player_Market':'first','COUNT':'count','MEAN':'mean','STDEV':'std'}).reset_index(drop=True)
        #nba_analytics_ar=nba_analytics_ar.groupby(['PLAYER_ID']).agg({'PLAYER_ID':'first','Player_Name':'first','Player_Market':'first','COUNT':'count','MEAN':'mean','STDEV':'std'}).reset_index(drop=True)
        #nba_analytics_m3=nba_analytics_m3.groupby(['PLAYER_ID']).agg({'PLAYER_ID':'first','Player_Name':'first','Player_Market':'first','COUNT':'count','MEAN':'mean','STDEV':'std'}).reset_index(drop=True)
        #nba_analytics_pr=nba_analytics_pr.groupby(['PLAYER_ID']).agg({'PLAYER_ID':'first','Player_Name':'first','Player_Market':'first','COUNT':'count','MEAN':'mean','STDEV':'std'}).reset_index(drop=True)


        #nba_analytics=pandas.concat([nba_analytics_pts,nba_analytics_ast,nba_analytics_reb,nba_analytics_pa,nba_analytics_par,nba_analytics_ar,nba_analytics_m3,nba_analytics_pr]).reset_index()
        nba_analytics=pandas.concat([nba_analytics_pts,nba_analytics_ast,nba_analytics_reb,nba_analytics_pa,nba_analytics_par,nba_analytics_ar,nba_analytics_m3,nba_analytics_pr])
        return nba_analytics

    def summarise_bs_df(self,box_score_df):
        
        nba_analytics=self.list_bs_df_kpis_vertically(box_score_df=box_score_df)
        nba_analytics=nba_analytics.groupby(['PLAYER_ID','Player_Market']).agg({'PLAYER_ID':'first','Player_Name':'first','Player_Market':'first','COUNT':'count','MEAN':'mean','STDEV':'std'}).reset_index(drop=True)


        
        return nba_analytics

    def consolidate_odds_boxscore(self,boxscore_dataframe,nba_sports_odds_players_df):
        bs_df2=self.summarise_bs_df(boxscore_dataframe)
        result=pandas.merge(nba_sports_odds_players_df,bs_df2,on=['Player_Name','Player_Market'],how='left')
        result['t_value']=(result['MEAN']-result['Line'])/(result['STDEV'])    #result['t_value']=(result['MEAN']-result['Line'])/(result['STDEV']/np.sqrt(result['COUNT']-1))
        result['under_p_value']=(0.5-stats.norm.sf(np.abs(result['t_value'])))*np.sign(result['Line']-result['MEAN'])+0.5  #result['under_p_value']=(0.5-stats.t.sf(np.abs(result['t_value']), result['COUNT']-1))*np.sign(result['Line']-result['MEAN'])+0.5
        result['over_p_value']=1-result['under_p_value']
        result['over_return']=result['over_p_value']*result['Over_Odds']
        result['under_return']=result['under_p_value']*result['Under_Odds']
        
        result=result.groupby(['Player_Name','Player_Market','Event_name']).agg({'Player_Name':'first','PLAYER_ID':'first','Player_Market':'first','Event_id':'first','Event_Date':'first','Event_name':'first','Over_Odds':'sum','Under_Odds':'sum','Line':'mean','COUNT':'mean','MEAN':'mean','STDEV':'mean','t_value':'mean','over_p_value':'mean','under_p_value':'mean','over_return':'sum','under_return':'sum'}).reset_index(drop=True)
        result['max_return']=np.where(result['over_return']>result['under_return'],result['over_return'],result['under_return'])
        result['choose']=np.where(result['over_return']>result['under_return'],'OVER','UNDER')
        result['chosen_odd']=np.where(result['over_return']>result['under_return'],result['Over_Odds'],result['Under_Odds'])
        result['chosen_pos']=np.where(result['over_return']>result['under_return'],result['over_p_value'],result['under_p_value'])

        return result

    def backup_tables(self,database,engine,sql_table_name_list=['nba_recent_player_boxscore','nba_season_all_games','nba_sports_odds_all','nba_odds_analysis','nba_sports_odds_players','nba_betting_recommendations','nba_recommendation_log']):
        for table in sql_table_name_list:
            df=self.read_sql_into_df(database,table)
            df.to_sql('backup_'+table,engine,chunksize=50,method='multi',if_exists='replace',index=False)

    def get_nba_stats(self,database,engine,days_interval=4,bs_table_name="nba_recent_player_boxscore",season_game_table_name="nba_season_all_games",season="2019-20"):
        
        
        today_date=datetime.date.today().strftime("%m/%d/%Y")
        now = datetime.datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print('----  Running nba stats query for '+today_date+'   @ '+current_time)
        from_date=(datetime.date.today()-datetime.timedelta(days=days_interval)).strftime("%m/%d/%Y")
        games_from_date=(datetime.date.today()-datetime.timedelta(days=90)).strftime("%m/%d/%Y")
        
        
        existing_bs_df=self.read_sql_into_df(dw,bs_table_name)
        #write all season games into sql

        

        season_games_df=leaguegamefinder.LeagueGameFinder(date_from_nullable=games_from_date,date_to_nullable=today_date,headers=self.headers).get_data_frames()[0]
        season_games_df['GAME_DATE']=pandas.to_datetime(season_games_df['GAME_DATE'])
        season_games_df.to_sql(season_game_table_name,engine,chunksize=50,method='multi',if_exists='replace',index=False)



        #combine existing and new, remove duplicates, keep last entry
        new_bs_df=self.get_player_boxscore_df_base_on_date(from_date,today_date)
        combined_bs_df=pandas.concat([new_bs_df,existing_bs_df],sort=False).drop_duplicates(subset=['GAME_ID','PLAYER_ID'],keep='first').reset_index(drop=True)
        
        #keep all history of boxscore to enable profitability review
        #cut_off_date=pandas.Timestamp.today()-pandas.Timedelta('45 days')
        #filter=np.where(combined_bs_df['GAME_DATE']>=cut_off_date,True,False)
        #combined_bs_df=combined_bs_df[filter]
        combined_bs_df['MIN']=combined_bs_df['MIN'].apply(get_mins)

        #write data frame into sql  
        combined_bs_df.to_sql(bs_table_name,engine,chunksize=50,method='multi',if_exists='replace',index=False)   

        cut_off_date_45days=pandas.Timestamp.today()-pandas.Timedelta('45 days')
        filter_45=np.where(combined_bs_df['GAME_DATE']>=cut_off_date_45days,True,False)

        combined_bs_df=combined_bs_df[filter_45]

        try:
            spread.df_to_sheet(season_games_df, index=False, sheet=season_game_table_name, start='A1', replace=True)
            spread.df_to_sheet(combined_bs_df, index=False, sheet=bs_table_name, start='A1', replace=True)
        except Exception:
            print('Failed to load into googlesheet')


        print('Stats generated')
        
    def get_nba_sportsbet_odds(self,database,engine,all_odds_tb_name='nba_sports_odds_all',player_odds_tb_name='nba_sports_odds_players'):

        today_date=datetime.date.today().strftime("%m/%d/%Y")
        now = datetime.datetime.now()
        current_time = now.strftime("%H:%M:%S")

        print('----  Running sportsbet odds query for '+today_date+'   @ '+current_time)
        nba_sports_odds_all_df=self.get_sportsbet_odds_df()
        nba_sports_odds_players_df=self.filter_players_df_only(nba_sports_odds_all_df)

        nba_sports_odds_all_df.to_sql(all_odds_tb_name,engine,chunksize=50,method='multi',if_exists='replace',index=False)
        nba_sports_odds_players_df.to_sql(player_odds_tb_name,engine,chunksize=50,method='multi',if_exists='replace',index=False)

        spread=gspread_pandas.Spread('NBA_ANALYSIS')
        spread.df_to_sheet(nba_sports_odds_all_df, index=False, sheet=all_odds_tb_name, start='A1', replace=True)
        spread.df_to_sheet(nba_sports_odds_players_df, index=False, sheet=player_odds_tb_name, start='A1', replace=True)
        print('sportsbet odds generated')

        try:
            temp=self.get_nba_sb_odds_df(database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets')
            spread.df_to_sheet(temp, index=False, sheet='sb_temp', start='A1', replace=True)
        except Exception:
            print('error loading new sb odds')

    def get_nba_sb_odds_df_old(self,database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets'):
        
        margin_line_list=['End of 3rd Quarter Handicap','End of 3rd Quarter Alternate Handicap','Handicap Betting','Pick Your Own Line','1st Quarter Handicap','1st Quarter Alternate Handicap','2nd Quarter Handicap','2nd Quarter Alternate Handicap','3rd Quarter Handicap','3rd Quarter Alternate Handicap','4th Quarter Handicap','4th Quarter Alternate Handicap']
        

        s=requests.get('http://affiliate.sportsbet.com.au/xmlfeeds/Basketball.xml')
        parser = etree.XMLParser(recover=True)
        xroot=etree.fromstring(s.text, parser=parser)
        
        xml_list=[]

        for child in xroot[1][0]:
            for grandchild in child:
                for grandgrandchild in grandchild:
                    for gggc in grandgrandchild:
                        #print(grandgrandchild.attrib)
                        Event_name=child.attrib.get('EventName')
                        str_lst=Event_name.split(' At ')
                        Home_Team=str_lst[1]
                        Away_Team=str_lst[0]
                        type=grandchild.attrib.get('Type')
                        Selc_name=grandgrandchild.attrib.get('EventSelectionName')
                        player_name=''
                        player_market=''
                        over_odd=None
                        under_odd=None
                        selection_code=None
                        try:
                            Odds=float(gggc.attrib.get('Odds'))
                        except Exception:
                            Odds=None
                        
                        line=None
                        numstr=re.compile('3-Pointers|76er')
                        selection_mod=numstr.sub('',Selc_name)
                        try:
                            line=float(gggc.attrib.get('Line'))
                        except Exception:
                            try:
                                line=float(re.search('(\+|\-)*\d+\.*\d*',selection_mod).group())
                            except Exception:
                                line=None

                        #get default over or under attribute
                        if re.search('(Over|Under)$',Selc_name):
                            over_under=re.search('Over|Under',Selc_name).group(0)
                        else:
                            over_under=''


                        #Do calculations for player markets:
                        if ' - ' in type and type:
                            player_name=type[:type.find(' - ')]
                            player_market=type[type.find(' - ')+3:]
                            selection_code=player_name
                            type='Player '+player_market
                        
                        elif type in margin_line_list:
                            selection_code='Home'
                            if Home_Team in Selc_name:
                                line=-line
                                over_under='Over'
                            elif Away_Team in Selc_name:
                                over_under='Under'

                        
                        elif type in ['Match Betting']:
                            selection_code='Home'
                            line=0
                            if Selc_name==Home_Team:
                                over_under='Over'
                            elif Selc_name==Away_Team:
                                over_under='Under'


                        
                        #Do calculation for total line markets                        
                        elif re.search(' Total Pts| Total Points',type):
                            if re.search('Home',type):
                                selection_code='Home'
                                try:
                                    over_under=re.search('Over|Under',Selc_name).group(0)
                                except Exception:
                                    pass
                                
                            elif re.search('Away',type):
                                selection_code='Away'
                                try:
                                    over_under=re.search('Over|Under',Selc_name).group(0)
                                except Exception:
                                    pass
                            else:
                                selection_code=''
                                try:
                                    over_under=re.search('Over|Under',Selc_name).group(0)
                                except Exception:
                                    pass
                            

                        #Calculate over under odds
                        if over_under=='Over':
                            over_odd=Odds
                            under_odd=None
                        elif over_under=='Under':
                            over_odd=None
                            under_odd=Odds
                        
                        link='https://www.sportsbet.com.au/betting/basketball-us/nba-matches'

                        


                        item=['SB',Event_name,child.attrib.get('EventDate')[:10],type,selection_code,Selc_name,over_under,Odds,over_odd,under_odd,line,Home_Team,Away_Team,link]
                        xml_list.append(item)
            
        sb_odds_df=pandas.DataFrame(xml_list,columns=['Bookie','Event_name_raw','Event_Date','Market_raw','Selection_code','Selection_raw','Outcome','Odds','Over_odd','Under_odd','Line','Home_Team','Away_Team','Link'])
        
        #get standard event_name
        
        team_df=self.read_sql_into_df(database,team_mapping_table)[['TEAM_NAME','TEAM_NAME_SB','TEAM_ABBREVIATION']]
        team_df.columns=['Home_Team_std','Home_Team','Home_Abb']
        sb_odds_df=pandas.merge(sb_odds_df, team_df, on=['Home_Team'], how='left',suffixes=('','_h'))
        team_df.columns=['Away_Team_std','Away_Team','Away_Abb']
        sb_odds_df=pandas.merge(sb_odds_df, team_df, on=['Away_Team'], how='left',suffixes=('','_a'))
        
        sb_odds_df['Event_Name_std']=sb_odds_df['Away_Team_std']+' At '+sb_odds_df['Home_Team_std']
        sb_odds_df=sb_odds_df.drop(columns=['Home_Team_std','Away_Team_std'])
        market_mapping_df=self.read_sql_into_df(database,market_mapping_table)
        sb_odds_df=pandas.merge(sb_odds_df, market_mapping_df, on=['Bookie','Market_raw'], how='left')
        
        #map players
        sb_odds_df=self.map_player_name(sb_odds_df,database,engine,player_table='nba_vw_dim_players',player_exception_table='nba_player_name_manual_mapping')

        #rearrange order
        sb_odds_df=sb_odds_df.reindex(columns=['Bookie','Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line','Odds', 'Over_odd', 'Under_odd', 'Event_Name_raw', 'Home_Team', 'Away_Team','Market_raw', 'Selection_raw', 'Selection_code','Home_Abb', 'Away_Abb','Link'])



        return sb_odds_df

    def analyse_odds(self,database,engine,bs_table_name="nba_recent_player_boxscore",odds_table='nba_sports_odds_players',save_to='nba_odds_analysis'):
            # analyse odds and put into sql and googlesheet
        combined_bs_df=self.read_sql_into_df(database,bs_table_name)
        cut_off_date_45days=pandas.Timestamp.today()-pandas.Timedelta('45 days')
        cut_off_date_15days=pandas.Timestamp.today()-pandas.Timedelta('15 days')
        filter_45=np.where(combined_bs_df['GAME_DATE']>=cut_off_date_45days,True,False)
        filter_15=np.where(combined_bs_df['GAME_DATE']>=cut_off_date_15days,True,False)
        combined_bs_df_45d=combined_bs_df[filter_45]
        combined_bs_df_15d=combined_bs_df[filter_15]


        nba_sports_odds_players_df=self.read_sql_into_df(database,odds_table)
        odds_analysis_df_a=self.consolidate_odds_boxscore(combined_bs_df_15d,nba_sports_odds_players_df)
        odds_analysis_df_b=self.consolidate_odds_boxscore(combined_bs_df_45d,nba_sports_odds_players_df)[['PLAYER_ID','Player_Market','MEAN','max_return','choose']]
        odds_analysis_df=pandas.merge(odds_analysis_df_a, odds_analysis_df_b, on=['PLAYER_ID','Player_Market'], how='left',suffixes=('','_15day'))

        #odds_analysis_df=odds_analysis_df.dropna()
        try:
            odds_analysis_df.to_sql(save_to,engine,chunksize=50,method='multi',if_exists='replace',index=False)
        except Exception:
            print('failed to load analysis df into sql')
        
        spread=gspread_pandas.Spread('NBA_ANALYSIS')
        try:
            spread.df_to_sheet(odds_analysis_df, index=False, sheet=save_to, start='A1', replace=True)
        except Exception:
            print('failed to load into googlesheet')

        print('odds analysis results generated')

    def prepare_commentary(self,database,engine,odds_table='nba_odds_analysis'):
            # Prepare COmmentary sheet
        odds_analysis_df=self.read_sql_into_df(database,odds_table)
        spread_com=gspread_pandas.Spread('NBA_Analysis_Commentary')
        today_game_df=odds_analysis_df.groupby('Event_name').agg({'Event_name':'first'})
        spread_com.df_to_sheet(today_game_df, index=False, sheet='odds_analysis_dump', start='A1', replace=True)

    def analyse_analysis_result(self,database,engine,analysis_result='nba_odds_analysis',output='recommended_output',threshold=1.5):
        odds_analysis_df=self.read_sql_into_df(database,analysis_result)
        filter=np.where(((odds_analysis_df['max_return']>=3))|((odds_analysis_df['choose_15day']==odds_analysis_df['choose']) & (odds_analysis_df['max_return']>=threshold) & (odds_analysis_df['max_return_15day']>=(threshold-0.15))),True,False)
        odds_analysis_df=odds_analysis_df[filter]
        return odds_analysis_df
    
    def get_notifying_df(self,database,engine,sql_table_name='nba_recommendation_log',threshold=1.5):
        today_date=datetime.date.today().strftime("%m/%d/%Y")
        now = datetime.datetime.now()
        current_time = now.strftime("%H:%M:%S")

        existing_reco_df_org=self.read_sql_into_df(database,sql_table_name)
        existing_reco_df=existing_reco_df_org[['Event_Date','Event_name','Player_Name','Player_Market','max_return']]
        df_analytic_result=self.analyse_analysis_result(database,engine,threshold=threshold).sort_values(by='max_return', ascending=False)
        df_analytic_result['run_date']=today_date+' '+current_time
        to_notify_df_temp=pandas.merge(df_analytic_result, existing_reco_df, on=['Event_Date','Event_name','Player_Name'], how='left',suffixes=('','_y'))
        filter=to_notify_df_temp['max_return_y'].isna()
        to_notify_df=to_notify_df_temp[filter]
        to_notify_df=to_notify_df.drop(columns=['max_return_y','Player_Market_y'])
        to_notify_df=to_notify_df.drop_duplicates(subset=['PLAYER_ID'],keep='first').reset_index(drop=True)

        #save_to_sql
        to_notify_df.to_sql(sql_table_name,engine,chunksize=50,method='multi',if_exists='append',index=False)
        
        return to_notify_df

    def get_recommendation_text_from_to_notify_df(self,to_notify_df):
        
        to_notify_df['s_return']=(to_notify_df['max_return']-1).map('{:,.1%}'.format)
        to_notify_df['s_line']=(to_notify_df['Line']).map('{:,.1f}'.format)
        to_notify_df['s_posb']=(to_notify_df['chosen_pos']).map('{:,.1%}'.format)
        to_notify_df['s_mean']=(to_notify_df['MEAN_15day']).map('{:,.1f}'.format)
        to_notify_df['s_mean_15day']=(to_notify_df['MEAN']).map('{:,.1f}'.format)
        to_notify_df['s_odd']=(to_notify_df['chosen_odd']).map('{:,.2f}'.format)
        to_notify_df['s_stdev']=(to_notify_df['STDEV']).map('{:,.2f}'.format)
        def star_ratings(df):
            
            if len(df) !=  0:

                if (df['max_return']-1) >= 0.9:
                    return '★★★★★'
                elif (df['max_return']-1) >= 0.8:
                    return '★★★★☆'
                elif (df['max_return']-1) >= 0.7:
                    return '★★★☆☆'
                elif (df['max_return']-1) >= 0.6:
                    return '★★☆☆☆'
                elif (df['max_return']-1) >= 0.5:
                    return '★☆☆☆☆'
                else:
                    return ''
            else:
                return ''

        to_notify_df['rating'] = to_notify_df.apply(star_ratings, axis = 1)


        to_notify_df['recommendation']=to_notify_df['rating']+': Bet '+to_notify_df['Player_Name']+' achieving '+to_notify_df['choose']+' '+to_notify_df['s_line']+' '+to_notify_df['Player_Market']+' in '+to_notify_df['Event_name']+', expected return is '+to_notify_df['s_return']+'(posb='+to_notify_df['s_posb']+'*Odd='+to_notify_df['s_odd']+'. 15days avg='+to_notify_df['s_mean']+'; 45days avg='+to_notify_df['s_mean_15day']+')'
        
        body=to_notify_df['recommendation'].str.cat(sep='\n\n')

        if to_notify_df['max_return'].max()>=10:
            header='!!!!!!!!!SportsBet!!!!!!!!!'
        else:
            header='SportsBet Update (DO NOT REPLY)'

        header_body_list=[header,body]
        
        return header_body_list

    def prepare_email_body_string(self,database,engine,sql_table_name='nba_recommendation_log',threshold=1.5):
        to_notify_df=self.get_notifying_df(database=database,engine=engine,sql_table_name=sql_table_name,threshold=threshold)
        header_body_list=self.get_recommendation_text_from_to_notify_df(to_notify_df)
        return header_body_list
    
    def get_email_distribution_list(self,workbook_name='NBA_email_subscribers',sheet_name='register'):
        spread=gspread_pandas.Spread(workbook_name)
        register_df=spread.sheet_to_df(index=None,sheet=sheet_name)
        contact_list=[]
        for index, data in register_df.iterrows():
            data_list=[]
            for i in data:
                data_list.append(i)
            contact_list.append(data_list)
        return contact_list

    def updates_via_telegram(self,update_text='hello'):
        distribution_list=self.get_email_distribution_list(workbook_name='NBA_email_subscribers',sheet_name='register')
        if update_text!='':
            def telegram_bot_sendtext(chat_id,bot_message='hello',bot_token='1004217287:AAFRu5CD-3s70TgnPLbdKqIkKoqWWywkrco'):     
                params = {"chat_id": str(chat_id),"text": bot_message,"parse_mode": "HTML"}
                requests.get("https://api.telegram.org/bot{}/sendMessage".format(bot_token),params=params)

            for row in distribution_list:
                chat_id=row[2]
                if chat_id!='':
                    telegram_bot_sendtext(chat_id,update_text)

            
            print('telegram update been sent')
        else:
            print('no update required')

    def sync_sql_table_to_gs(self,database,sql_table_name='nba_vw_recommendation_log_results_with_outcome',gspread_name='NBA_ANALYSIS',sheet_name='outcome_log'):
        df=self.read_sql_into_df(database,sql_table_name)
        spread=gspread_pandas.Spread(gspread_name)
        try:
            spread.df_to_sheet(df, index=False, sheet=sheet_name, start='A1', replace=True)
        except Exception:
            print('failed to load into googlesheet')

    def get_latest_injury_report(self):
    
        def get_injury_df(link):
            #print('requesting injury report:')      
            
            f2 = tabula.read_pdf(link,pages='all')
            data_list=[]
            output_list=[]
            status_selection_code=['Out','Questionable','Doubtful',]
            player_name_list=[]
            status_list=[]
            for df in f2:
                for index,row in df.iterrows():
                    for data in row:
                        data_list.append(data)

            no_rec=len(data_list)
            for i in range (no_rec):
                if data_list[i] in status_selection_code:
                    name=data_list[i-1].split(', ')
                    player_name=name[1]+' '+name[0]
                    status=data_list[i]

                    output_list.append([player_name,status])


            df=pandas.DataFrame(output_list,columns=['Player_Name','Status'])

            return df

        for days in range(3):
            date=(datetime.date.today()-datetime.timedelta(days)).strftime("%Y-%m-%d")
            for time in ['08PM','05PM','01PM']:
                link="https://ak-static.cms.nba.com/referee/injury/Injury-Report_"+date+'_'+time+".pdf"
                try:
                    
                    df=get_injury_df(link)
                    
                    return df

                except Exception:
                    next

    def write_injury_report(self,database,engine,sql_table_name='nba_injury_report'):
        df=self.get_latest_injury_report()
        if not df.empty:
            df.to_sql(sql_table_name,engine,chunksize=50,method='multi',if_exists='replace',index=False)   
            spread=gspread_pandas.Spread('NBA_ANALYSIS')
            try:
                spread.df_to_sheet(df, index=False, sheet=sql_table_name, start='A1', replace=True)
            except Exception:
                print('failed to load injury report into googlesheet')

    def map_player_name(self,input_odds_df,database,engine,player_table='nba_vw_dim_players',player_exception_table='nba_player_name_manual_mapping'):


        #slice only selected columns, read player df and exception table
        odds_df=input_odds_df[['Bookie','Home_Abb','Away_Abb','Selection_code','Match_Component']]
        player_df=self.read_sql_into_df(database,player_table)
        player_excp_df=self.read_sql_into_df(database,player_exception_table)
        #get player markets only
        filter=np.where(odds_df['Match_Component']=='Player',True,False)
        odds_df=odds_df[filter]
        odds_df=odds_df.groupby('Selection_code').agg({'Bookie':'first','Home_Abb':'first','Away_Abb':'first','Selection_code':'first'}).reset_index(drop=True)

        # define method of getting a standard naming convention
        def get_ref_name(name):
            name_str_list=name.split(' ')
            if len(name_str_list)>=2:
                given_name=name_str_list[0][:2]
                family_name=name_str_list[1][:6]
                ref_name=given_name+' '+family_name
            else:
                ref_name=name

            return ref_name
        odds_df['ref_name']=odds_df['Selection_code'].apply(get_ref_name)
        player_df['ref_name']=player_df['PLAYER_NAME'].apply(get_ref_name)

        #player name will look up three points: 1. home+name ref, 2. away+name ref, 3 exception
        player_mapping_df=odds_df.merge(player_df, left_on=['Home_Abb','ref_name'], right_on=['TEAM_ABBREVIATION','ref_name'],how='left')
        player_mapping_df=player_mapping_df.merge(player_df, left_on=['Away_Abb','ref_name'], right_on=['TEAM_ABBREVIATION','ref_name'],how='left')
        player_mapping_df=player_mapping_df.merge(player_excp_df,left_on=['Bookie','Selection_code'],right_on=['Bookie','Player_name_raw'],how='left')

        #if listed in exception, then use exception, otherwise use whatever can be mapped
        player_mapping_df['Selection_code_std']=np.where(player_mapping_df['Player_name_std'].isna(),np.where(player_mapping_df['PLAYER_NAME_x'].isna(),np.where(player_mapping_df['PLAYER_NAME_y'].isna(),'',player_mapping_df['PLAYER_NAME_y']),player_mapping_df['PLAYER_NAME_x']),player_mapping_df['Player_name_std'])

        #remove unnecessary columns, generate output
        player_mapping_df=player_mapping_df[['Bookie','Home_Abb','Away_Abb','Selection_code','Selection_code_std']]
        output_odds_df=input_odds_df.merge(player_mapping_df, left_on=['Bookie','Home_Abb','Away_Abb','Selection_code'], right_on=['Bookie','Home_Abb','Away_Abb','Selection_code'],how='left')
        output_odds_df['Selection_code_std']=np.where(output_odds_df['Match_Component']=='Player',output_odds_df['Selection_code_std'],output_odds_df['Selection_code'])
        filter=np.where((output_odds_df['Match_Component']=='Player')&((output_odds_df['Selection_code_std'].isna())|(output_odds_df['Selection_code_std']=="")),False,True)
        output_odds_df=output_odds_df[filter]


        #get a list of unmapped players, append to existing df, and load into database
        filter=np.where(player_mapping_df['Selection_code_std']=='',True,False)
        unmapped_player_df=player_mapping_df[filter]
        unmapped_player_df.loc[:,'Player_name_raw']=unmapped_player_df['Selection_code']
        unmapped_player_df=unmapped_player_df[['Bookie','Home_Abb','Away_Abb','Player_name_raw']]
        player_excp_df=player_excp_df.append(unmapped_player_df,sort=False)
        player_excp_df=player_excp_df.groupby(['Bookie','Player_name_raw']).agg({'Bookie':'first','Player_name_raw':'first','Player_name_std':'first'}).reset_index(drop=True)
        player_excp_df.to_sql(player_exception_table,engine,chunksize=50,method='multi',if_exists='replace',index=False)

        #return output
        return output_odds_df

    def get_nba_tab_odds_df(self,database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets'):
        
        res=requests.get('https://api.beta.tab.com.au/v1/tab-info-service/sports/Basketball/competitions/NBA?jurisdiction=VIC').json()
        
        player_mkt_mapping={'Player Points':'Points','Alternate Player Points':'Points','Player Rebounds':'Rebounds','Alternate Player Rebounds':'Rebounds','Player Assists':'Assists','Alternate Player Assists':'Assists','Player PRA Over/Under':'Pts + Reb + Ast','Player Points + Rebounds + Assists':'Pts + Reb + Ast','Player Threes':'Made Threes','Alternate Player Threes':'Made Threes','NBA 3 Point Shootout 1st Round Score':'Made Threes'}
            
        margin_line_market=['Pick Your Own Line','Line','Extra Line','1st Half Line','1st Half Pick Your Own Line','1st Quarter Line','1st Quarter Pick Your Own Line']
        
        total_line_market=['1st Half Pick Your Own Total','Team Points Over/Under','Total Points Over/Under','Pick Your Own Total','1st Quarter Pick Your Own Total','2nd Quarter Pick Your Own Total','3rd Quarter Pick Your Own Total','4th Quarter Pick Your Own Total','Extra Total Line']
        
        
        output=[]
        bookie='Tab'
        
        
        matches=res['matches']
        for match in matches:
            Event_Name=match['name']
            Home_Team=Event_Name.split(' v ')[0]
            Away_Team=Event_Name.split(' v ')[1]
            Event_Time=match['startTime'][:10]
            for market in match['markets']:
                bet_option=market['betOption']
                for side in market['propositions']:
                    selection_name=side['name']
                    
                    if re.search('Over|Under',selection_name):
                        outcome=re.search('Over|Under',selection_name).group()
                    else:
                        outcome=''
                    odd=side['returnWin']
                    
                    try:
                        temp_name=selection_name
                        numstr=re.compile('3-Pointers|76ers')
                        temp_name=numstr.sub('',temp_name) 
                        
                        line=float(re.search('(\+|\-)*\d+\.*\d*',temp_name).group())
                        if line==int(line):
                            line=line-0.5
                    except Exception:
                        line=None

                    ####specifications for player Market
                    if bet_option in player_mkt_mapping:
                        Player_Name=re.search('\D+',selection_name).group()[:-1]
                        ou=re.compile('( Over| Under)')
                        Player_Name=ou.sub('',Player_Name)
                        selection_code=Player_Name

                        try:
                            if re.search('Over|\+',selection_name):
                                outcome='Over'
                            elif re.search('Under',selection_name):
                                outcome='Under'
                        except Exception:
                            outcome=''
                    
                
                    ###specifications for margin line market:
                    elif bet_option in margin_line_market:




                        selection_code='Home'
                        
                        if Home_Team in selection_name:
                            outcome='Over'
                            line=-line
                        elif Away_Team in selection_name:
                            outcome='Under'
                            
                    #head to head is an exception of margin line
                    elif bet_option in ['Head To Head']:
                        selection_code='Home'
                        if re.search('\D+',selection_name).group()==Home_Team:
                            outcome='Over'
                            line=0
                        elif re.search('\D+',selection_name).group()==Away_Team:
                            outcome='Under'
                            line=0
                    
                    ###specifications for TOTAL LINE market:
                    elif bet_option in total_line_market:
                        #there is a typo in selection name, charlotte was typed as chrlotte
                        selection_name=re.sub('Chrlotte','Charlotte',selection_name)
                        selection_name=re.sub('Philly','Philadelphia',selection_name)
                        selection_name=re.sub('Milwauke','Milwaukee',selection_name)
                        
                        if Home_Team in selection_name:
                            selection_code='Home'
                            try:
                                if re.search('Over|\+',selection_name):
                                    outcome='Over'
                                elif re.search('Under',selection_name):
                                    outcome='Under'
                            except Exception:
                                outcome=''
                        elif Away_Team in selection_name:
                            selection_code='Away'
                            try:
                                if re.search('Over|\+',selection_name):
                                    outcome='Over'
                                elif re.search('Under',selection_name):
                                    outcome='Under'
                            except Exception:
                                outcome=''
                        else:
                            selection_code=''
                            try:
                                if re.search('Over|\+',selection_name):
                                    outcome='Over'
                                elif re.search('Under',selection_name):
                                    outcome='Under'
                            except Exception:
                                outcome=''
                    #calc over/under odds columns
                    Over_odd=None
                    Under_odd=None
                    if outcome=='Over':
                        Over_odd=odd
                    elif outcome=='Under':
                        Under_odd=odd
                    
                    link='https://www.tab.com.au/sports/betting/Basketball/competitions/NBA/matches/'+Home_Team+' v '+Away_Team
                    link=link.replace(' ','%20')



                    row=[bookie,Event_Name,Event_Time,Home_Team,Away_Team,bet_option,selection_name,selection_code,outcome,line,odd,Over_odd,Under_odd,link]
                    output.append(row)

        #df=pandas.DataFrame.from_dict(data)      
        df = pandas.DataFrame(output,columns =['Bookie','Event_Name_raw','Event_Date','Home_Team','Away_Team','Market_raw','Selection_raw','Selection_code','Outcome','Line','Odds','Over_odd','Under_odd','Link'])

        #get standard event_name
        tab_odds_df=df
        team_df=self.read_sql_into_df(database,team_mapping_table)[['TEAM_NAME','TEAM_NAME_TAB','TEAM_ABBREVIATION']]
        team_df.columns=['Home_Team_std','Home_Team','Home_Abb']
        tab_odds_df=pandas.merge(tab_odds_df, team_df, on=['Home_Team'], how='left',suffixes=('','_h'))
        team_df.columns=['Away_Team_std','Away_Team','Away_Abb']
        tab_odds_df=pandas.merge(tab_odds_df, team_df, on=['Away_Team'], how='left',suffixes=('','_a'))
        #tab_odds_df=tab_odds_df.drop(columns=['Home_Team_std_h','Away_Team_std_a'])
        tab_odds_df['Event_Name_std']=tab_odds_df['Away_Team_std']+' At '+tab_odds_df['Home_Team_std']
        tab_odds_df=tab_odds_df.drop(columns=['Home_Team_std','Away_Team_std'])
        market_mapping_df=self.read_sql_into_df(database,market_mapping_table)
        tab_odds_df=pandas.merge(tab_odds_df, market_mapping_df, on=['Bookie','Market_raw'], how='left')
        
        #map players
        tab_odds_df=self.map_player_name(tab_odds_df,database,engine,player_table='nba_vw_dim_players',player_exception_table='nba_player_name_manual_mapping')

        #rearrange order
        tab_odds_df=tab_odds_df.reindex(columns=['Bookie','Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line','Odds', 'Over_odd', 'Under_odd', 'Event_Name_raw', 'Home_Team', 'Away_Team','Market_raw', 'Selection_raw', 'Selection_code','Home_Abb', 'Away_Abb','Link'])

        return tab_odds_df

    def get_nba_tabs_odds(self,database,engine,all_odds_tb_name='nba_tab_odds_all'):
        
        today_date=datetime.date.today().strftime("%m/%d/%Y")
        now = datetime.datetime.now()
        current_time = now.strftime("%H:%M:%S")

        print('----  Running tab odds query for '+today_date+'   @ '+current_time)
        try:
            nba_sports_odds_all_df=self.get_nba_tab_odds_df(database,engine)
            
            nba_sports_odds_all_df.to_sql(all_odds_tb_name,engine,chunksize=50,method='multi',if_exists='replace',index=False)
            
            #spread=gspread_pandas.Spread('NBA_ANALYSIS')
            #spread.df_to_sheet(nba_sports_odds_all_df, index=False, sheet=all_odds_tb_name, start='A1', replace=True)
            
            
            print('tab odds generated')
        except Exception:
            print('can not refresh tab odds')

    def get_nba_sb_odds_df(self,database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets'):

        matches_data=requests.get('https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Competitions/6927?displayType=default&eventFilter=matches').json()

        events=matches_data['events']
        margin_line_list=['Head to Head','End of 3rd Quarter Handicap','End of 3rd Quarter Alternate Handicap','Handicap Betting','Pick Your Own Line','1st Quarter Handicap','1st Quarter Alternate Handicap','2nd Quarter Handicap','2nd Quarter Alternate Handicap','3rd Quarter Handicap','3rd Quarter Alternate Handicap','4th Quarter Handicap','4th Quarter Alternate Handicap']
        xml_list=[]


        for event in events:
            event_id=event['id']
            Event_name=event['name']
            Event_date=int(event['startTime'])
            Event_date=datetime.datetime.fromtimestamp(Event_date).isoformat()[:10]
            str_lst=Event_name.split(' At ')
            Home_Team=str_lst[1]
            Away_Team=str_lst[0]
            market_groups=[]
            market_sgmg_groups=[]
            event_data=requests.get('https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Events/{}/SportCard?displayWinnersPriceMkt=true&includeLiveMarketGroupings=true&includeCollection=true'.format(event_id)).json()
            market_groupings=event_data['marketGrouping']
            for group in market_groupings:
                market_group_id=group['id']
                market_group_name=group['name']
                markets_data=requests.get('https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Events/{}/MarketGroupings/{}/Markets'.format(event_id,market_group_id)).json()
                for market in markets_data:
                    market_name=market['name']
                    selections=market['selections']
                    for selection in selections:
                        Selc_name=selection['name']
                        player_name=''
                        player_market=''
                        over_odd=None
                        under_odd=None
                        selection_code=None

                        #get default over or under attribute
                        if re.search('(Over|Under)$',Selc_name):
                            over_under=re.search('Over|Under',Selc_name).group(0)
                        else:
                            over_under=''

                        try:
                            Odds=float(selection['price']['winPrice'])
                        except Exception:
                            Odds=0
                        try:
                            line=float(selection['displayHandicap'])
                        except Exception:
                            try:
                                temp_name=Selc_name
                                numstr=re.compile('3-Pointers|76ers')
                                temp_name=numstr.sub('',temp_name)                                

                                line=float(re.search('(\+|\-)*\d+\.*\d*',temp_name).group())
                            except Exception:
                                line=0
                        


                        #Do calculations for player markets:
                        if ' - ' in market_name and market_name:
                            player_name=market_name[:market_name.find(' - ')]
                            player_market=market_name[market_name.find(' - ')+3:]
                            selection_code=player_name
                            rev_market_name='Player '+player_market
                        else:
                            rev_market_name=market_name
                        
                        #Do calculation for Margin Mrkets
                        #rev_market_name=market_name

                        if market_name in margin_line_list:
                            selection_code='Home'
                            if Home_Team in Selc_name:
                                over_under='Over'
                                line=-line
                            elif Away_Team in Selc_name:
                                over_under='Under'
                        



                        #Calculate over under odds
                        if over_under=='Over':
                            over_odd=Odds
                            under_odd=None
                        elif over_under=='Under':
                            over_odd=None
                            under_odd=Odds
                        
                        link='https://www.sportsbet.com.au/betting/basketball-us/nba-matches/'+Event_name+'-'+str(event_id)
                        link=link.replace(' ','-')

                        


                        item=['SB',Event_name,Event_date,rev_market_name,selection_code,Selc_name,over_under,Odds,over_odd,under_odd,line,Home_Team,Away_Team,link]
                        xml_list.append(item)
        
        row=['SB',Event_name,Event_date,'Sacramento Kings','Portland Trail Blazers','Player Points Markets','dummy','dummy','Under',0,0,0,0,link]
        xml_list.append(row)
        row=['SB',Event_name,Event_date,'Sacramento Kings','Portland Trail Blazers','Player Points Markets','dummy','dummy','Over',0,0,0,0,link]
        xml_list.append(row)  

        sb_odds_df=pandas.DataFrame(xml_list,columns=['Bookie','Event_name_raw','Event_Date','Market_raw','Selection_code','Selection_raw','Outcome','Odds','Over_odd','Under_odd','Line','Home_Team','Away_Team','Link'])

        #get standard event_name

        team_df=self.read_sql_into_df(database,team_mapping_table)[['TEAM_NAME','TEAM_NAME_SB','TEAM_ABBREVIATION']]
        team_df.columns=['Home_Team_std','Home_Team','Home_Abb']
        sb_odds_df=pandas.merge(sb_odds_df, team_df, on=['Home_Team'], how='left',suffixes=('','_h'))
        team_df.columns=['Away_Team_std','Away_Team','Away_Abb']
        sb_odds_df=pandas.merge(sb_odds_df, team_df, on=['Away_Team'], how='left',suffixes=('','_a'))

        sb_odds_df['Event_Name_std']=sb_odds_df['Away_Team_std']+' At '+sb_odds_df['Home_Team_std']
        sb_odds_df=sb_odds_df.drop(columns=['Home_Team_std','Away_Team_std'])
        market_mapping_df=self.read_sql_into_df(database,market_mapping_table)
        sb_odds_df=pandas.merge(sb_odds_df, market_mapping_df, on=['Bookie','Market_raw'], how='left')

        #map players
        sb_odds_df=self.map_player_name(sb_odds_df,database,engine,player_table='nba_vw_dim_players',player_exception_table='nba_player_name_manual_mapping')

        #rearrange order
        sb_odds_df=sb_odds_df.reindex(columns=['Bookie','Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line','Odds', 'Over_odd', 'Under_odd', 'Event_Name_raw', 'Home_Team', 'Away_Team','Market_raw', 'Selection_raw', 'Selection_code','Home_Abb', 'Away_Abb','Link'])

        return sb_odds_df

    def get_nba_be_odds_df(self,database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets'):

        eventlist=requests.get("https://beteasy.com.au/api/sports/navigation/basketball/nba/nba-matches").json()['result']['events']
        player_mkt_mapping={'Player Points':'Points','Alternate Player Points':'Points','Player Rebounds':'Rebounds','Alternate Player Rebounds':'Rebounds','Player Assists':'Assists','Alternate Player Assists':'Assists','Player PRA Over/Under':'Pts + Reb + Ast','Player Points + Rebounds + Assists':'Pts + Reb + Ast','Player Threes':'Made Threes','Alternate Player Threes':'Made Threes','NBA 3 Point Shootout 1st Round Score':'Made Threes'}
        margin_line_list=['Line','Pick Your Own Line','1st Quarter Line']

        output=[]
        for event in eventlist:
            event_id=event['masterEventId']
            res=requests.get('https://beteasy.com.au/api/sports/event',params={'id':event_id})
            if res.status_code!=200:
                continue
            markets=res.json()['result']['EventGroups']
            for market in markets:
                try:
                    market_id=market['GroupOrderByID']
                    data=requests.get('https://beteasy.com.au/api/sports/event-group',params={'id':event_id,'ecGroupOrderByIds[]':market_id}).json()
                    market_id=str(market_id)
                    market_raw=data['result'][market_id]['ECGroupName']
                    for betting_type in data['result'][market_id]['BettingType']:
                        event_name=betting_type['MasterEventName']
                        
                        event_name_str_list=event_name.split(' @ ')
                        home_team=event_name_str_list[1]
                        away_team=event_name_str_list[0]
                        sub_mkt_name=betting_type['EventName']
                        
                        url_id=str(betting_type['EventID'])
                        slug=str(betting_type['Slug'])
                        date_stamp=str(betting_type['DateSlug'])
                        
                        event_date=betting_type['AdvertisedStartTime'][:10]
                        outcomes=betting_type['Outcomes']
                        for outcome in outcomes:
                            selection=outcome['OutcomeName']
                            
                            market_raw_output=''
                            for tp in outcome['BetTypes']:
                                selection_code=''
                                over_under=None
                                odd=tp['Price']
                                try:
                                    odd=float(odd)
                                except Exception:
                                    odd=None

                                category=tp['MarketTypeCode']
                                
                                numstr=re.compile('3-Pointers|76ers')
                                selection_mod=numstr.sub('',selection)
                                try:
                                    line=float(re.search('(\+|\-)*\d+\.*\d*',selection_mod).group())
                                except Exception:
                                    try:
                                        line=tp['Points']
                                        line=float(line)
                                    except Exception:
                                        line=None
                                if odd==None or odd==0:
                                    continue

                                #get default over under attribute
                                try:
                                    if re.search('Over|\+',selection):
                                        over_under='Over'
                                    elif re.search('Under',selection):
                                        over_under='Under'
                                except Exception:
                                    over_under=None
                                    
                                #do calc for players market
                                player_name=None
                                market_raw_output=sub_mkt_name
                                
                                if 'Player' in market_raw and 'Quarter' not in sub_mkt_name and 'Half' not in sub_mkt_name:
                                    
                                    player_mkts=re.compile('( Rebounds \+ Assists Over/Under| Points \+ Assists Over/Under| Points \+ Rebounds Over/Under| Points \+ Rebounds \+ Assists Over/Under| Assists Over/Under| Points Over/Under| Rebounds Over/Under| 3-Pointers Over/Under| Alternate Points| Alternate Rebounds| Alternate Assists| Alternate Steals| Alternate Blocks| Alternate 3-Pointers)')
                                    player_name=player_mkts.sub('',sub_mkt_name)
                                    selection_code=player_name
                                    
                                    numstr=re.compile('3-Pointers|76ers')
                                    selection_mod=numstr.sub('',selection)
                                    
                                    try:
                                        line=float(re.search('(\+|\-)*\d+\.*\d*',selection_mod).group())
                                        if line==int(line):
                                            line=line-0.5
                                    except Exception:
                                        try:
                                            line=tp['Points']
                                            line=float(line)
                                            if line==int(line):
                                                line=line-0.5
                                        except Exception:
                                            line=None

                                    
                                    #market2=re.search('(Points + Rebounds + Assists|Points + Rebounds|Points + Assists)',sub_mkt_name)
                                    market2=player_mkts.search(sub_mkt_name)
                                    if market2:
                                        #market2=market2.group()
                                        market_raw_output='Player'+market2.group()
                                        #print(sub_mkt_name,market2,selection)
                                    #else:
                                        #market2=re.search('(Points|Assists|Rebounds|3-Pointers|Steals|Blocks)',sub_mkt_name)
                                        #if market2:
                                            #market2=market2.group()
                                            #market_raw_output='Player '+market2
                                            #print(sub_mkt_name,market2,selection)
                                        

                                
                                #do calc for margin market
                                elif sub_mkt_name in margin_line_list:
                                    selection_code='Home'
                                    if home_team in selection:
                                        line=-line
                                        over_under='Over'
                                    elif away_team in selection:
                                        over_under='Under'
                            
                                elif sub_mkt_name in ['Head to Head','1st Half Winner']:
                                    selection_code='Home'
                                    line=0
                                    if home_team in selection:
                                        over_under='Over'
                                    elif away_team in selection:
                                        over_under='Under'

                                #do calc for total market
                                elif re.search('Alternate Total Points Over/Under|Total Points Over/Under|1st Half Over/Under',sub_mkt_name):
                                    if home_team in selection:
                                        selection_code='Home'
                                        try:
                                            over_under=re.search('Over|Under',selection).group(0)
                                        except Exception:
                                            pass
                                        
                                    elif away_team in selection:
                                        selection_code='Away'
                                        try:
                                            over_under=re.search('Over|Under',selection).group(0)
                                        except Exception:
                                            pass
                                    else:
                                        selection_code=''
                                        try:
                                            over_under=re.search('Over|Under',selection).group(0)
                                        except Exception:
                                            pass                                                 
                                #Calculate over under odds
                                if over_under=='Over':
                                    over_odd=odd
                                    under_odd=None
                                elif over_under=='Under':
                                    over_odd=None
                                    under_odd=odd
                                link='https://beteasy.com.au/sports-betting/basketball/nba/nba-matches/'+slug+'-'+date_stamp+'-'+str(event_id)+'-'+url_id

                                output.append(['BE',event_name,event_date,home_team,away_team,market_raw_output,selection,selection_code,over_under,line,odd,over_odd,under_odd,link])
                except Exception:
                    next

        #print(output)
        sb_odds_df = pandas.DataFrame(output,columns =['Bookie','Event_Name_raw','Event_Date','Home_Team','Away_Team','Market_raw','Selection_raw','Selection_code','Outcome','Line','Odds','Over_odd','Under_odd','Link'])

        #get standard event_name
        
        team_df=self.read_sql_into_df(database,team_mapping_table)[['TEAM_NAME','TEAM_NAME_BE','TEAM_ABBREVIATION']]
        team_df.columns=['Home_Team_std','Home_Team','Home_Abb']
        sb_odds_df=pandas.merge(sb_odds_df, team_df, on=['Home_Team'], how='left',suffixes=('','_h'))
        team_df.columns=['Away_Team_std','Away_Team','Away_Abb']
        sb_odds_df=pandas.merge(sb_odds_df, team_df, on=['Away_Team'], how='left',suffixes=('','_a'))
        
        sb_odds_df['Event_Name_std']=sb_odds_df['Away_Team_std']+' At '+sb_odds_df['Home_Team_std']
        sb_odds_df=sb_odds_df.drop(columns=['Home_Team_std','Away_Team_std'])
        market_mapping_df=self.read_sql_into_df(database,market_mapping_table)
        sb_odds_df=pandas.merge(sb_odds_df, market_mapping_df, on=['Bookie','Market_raw'], how='left')
        
        #map players
        sb_odds_df=self.map_player_name(sb_odds_df,database,engine,player_table='nba_vw_dim_players',player_exception_table='nba_player_name_manual_mapping')

        #rearrange order
        sb_odds_df=sb_odds_df.reindex(columns=['Bookie','Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line','Odds', 'Over_odd', 'Under_odd', 'Event_Name_raw', 'Home_Team', 'Away_Team','Market_raw', 'Selection_raw', 'Selection_code','Home_Abb', 'Away_Abb','Link'])



        return sb_odds_df

    def get_nba_neds_odds_df(self,database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets'):
    
        headers={'authority':'api.ladbrokes.com.au',
        'method':'GET',
        'path':'/v2/sport/event-request?category_ids=%5B%223c34d075-dc14-436d-bfc4-9272a49c2b39%22%5D&competition_id=2d20a25b-6b96-4651-a523-442834136e2d',
        'scheme': 'https',
        'accept':'*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://www.ladbrokes.com.au',
        'referer': 'https://www.ladbrokes.com.au/sports',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36'}

        bookie='NEDS'
        ladbrok_unknown_var='940b8704-e497-4a76-b390-00918ff7d282'
        output=[]

        player_mkt_mapping={'Player Points Markets':'Points','Player Rebounds Markets':'Rebounds','Player Assists Markets':'Assists',"Player 3's Markets":'Made Threes'}
        responds=requests.get(r'https://api.ladbrokes.com.au/v2/sport/event-request?category_ids=%5B%223c34d075-dc14-436d-bfc4-9272a49c2b39%22%5D&competition_id=2d20a25b-6b96-4651-a523-442834136e2d',headers=headers).json()
        events=responds['next_events']
        for event_id in events:
            game_str=responds['events'][event_id]['name'].replace(' ','-')
            link='https://www.ladbrokes.com.au/sports/australian-rules/afl/'+game_str+'/'+event_id
            headers={'accept-encoding': 'gzip, deflate, br','origin':'https://www.ladbrokes.com.au','referer':link,'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'}
            data=requests.get('https://api.ladbrokes.com.au/v2/sport/event-card',params={'id':event_id},headers=headers).json()
            #if entrants['events'][event_id]['event_type']['name']!='Match':
                #next
            entrants=data['entrants']
            markets=data['markets']
            market_groups=data['market_type_groups']
            prices=data['prices']
            event_detail=data['events'][event_id]
            event_slug=event_detail['slug']
            event_name=event_detail['name']
            Home_Team=event_name.split(' V ')[0]
            Away_Team=event_name.split(' V ')[-1]
            event_date=event_detail['actual_start'][:10]

            market_group_list=event_detail['market_type_group_markets']
            for market_group in market_group_list:
                
                market_type_group_id=market_group['market_type_group_id']
                market_type_group_name=market_groups[market_type_group_id]['name']
                market_list=market_group['market_ids']
                for market_id in market_list:
                    market_name=markets[market_id]['name']
                    entrant_ids=markets[market_id]['entrant_ids']
                    for entrant_id in entrant_ids:
                        selection_name=entrants[entrant_id]['name']
                        selection_code=''
                        odd=float(prices[entrant_id+':'+ladbrok_unknown_var+':']['odds']['numerator'])/prices[entrant_id+':'+ladbrok_unknown_var+':']['odds']['denominator']+1
                        #print([event_name,event_date,market_type_group_name,market_name,selection_name,odd])


                        
                        if re.search('Over|Under',selection_name):
                            outcome=re.search('Over|Under',selection_name).group()
                        else:
                            outcome=''
        
                        
                        try:
                            temp_name=market_name
                            numstr=re.compile('3-Pointers|76ers')
                            temp_name=numstr.sub('',temp_name)
                            line=float(re.search('(\+|\-)*\d+\.*\d*',temp_name).group())
                            if line==int(line):
                                line=line-0.5
                            
                        except Exception:
                            line=None

                        ####specifications for player Market
                        if market_type_group_name in player_mkt_mapping:
                            Player_Name=selection_name.replace(' ('+Home_Team+')','')
                            Player_Name=Player_Name.replace(' ('+Away_Team+')','')

                            selection_code=Player_Name

                            try:
                                if re.search('Over|\+',market_name):
                                    outcome='Over'
                                elif re.search('Under',market_name):
                                    outcome='Under'
                            except Exception:
                                outcome=''

                            try:
                                line=float(re.search('(\+|\-)*\d+\.*\d*',market_name).group())
                                if line==int(line):
                                    line=line-0.5
                                
                            except Exception:
                                line=None

                        ####specifications for margin Market
                        if market_name2 in margin_line_list:
                            selection_code='Home'
                            if Home_Team in selection_name:
                                #line=-line
                                outcome='Over'
                            elif Away_Team in selection_name:
                                outcome='Under'
                    
                        elif market_name2 in ['Match Betting','1st Half H2H','2nd Half H2H','1st Quarter H2H','2nd Quarter H2H','3rd Quarter H2H','4th Quarter H2H']:
                            selection_code='Home'
                            line=0
                            if Home_Team in selection_name:
                                outcome='Over'
                            elif Away_Team in selection_name:
                                outcome='Under'




                        #calc over/under odds columns
                        Over_odd=None
                        Under_odd=None
                        if outcome=='Over':
                            Over_odd=odd
                        elif outcome=='Under':
                            Under_odd=odd
                        
                        link='https://www.neds.com.au/sports/basketball/usa/nba/'+event_slug+'/'+event_id
                        link=link.replace(' ','%20')


                        #print([event_name,event_date,market_type_group_name,market_name,selection_name,odd])
                        row=[bookie,event_name,event_date,Home_Team,Away_Team,market_type_group_name,selection_name,selection_code,outcome,line,odd,Over_odd,Under_odd,link]
                        output.append(row)
        
        row=[bookie,event_name,event_date,'Sacramento Kings','Portland Trail Blazers','Player Points Markets','dummy','dummy','Under',0,0,0,0,link]
        output.append(row)
        row=[bookie,event_name,event_date,'Sacramento Kings','Portland Trail Blazers','Player Points Markets','dummy','dummy','Over',0,0,0,0,link]
        output.append(row)


        df = pandas.DataFrame(output,columns =['Bookie','Event_Name_raw','Event_Date','Home_Team','Away_Team','Market_raw','Selection_raw','Selection_code','Outcome','Line','Odds','Over_odd','Under_odd','Link'])


                #print(selection_code)

        #get standard event_name
        odds_df=df
        team_df=self.read_sql_into_df(database,team_mapping_table)[['TEAM_NAME','TEAM_NAME_NEDS','TEAM_ABBREVIATION']]
        team_df.columns=['Home_Team_std','Home_Team','Home_Abb']
        odds_df=pandas.merge(odds_df, team_df, on=['Home_Team'], how='left',suffixes=('','_h'))
        team_df.columns=['Away_Team_std','Away_Team','Away_Abb']
        odds_df=pandas.merge(odds_df, team_df, on=['Away_Team'], how='left',suffixes=('','_a'))
        #odds_df=odds_df.drop(columns=['Home_Team_std_h','Away_Team_std_a'])
        odds_df['Event_Name_std']=odds_df['Away_Team_std']+' At '+odds_df['Home_Team_std']
        odds_df=odds_df.drop(columns=['Home_Team_std','Away_Team_std'])
        market_mapping_df=self.read_sql_into_df(database,market_mapping_table)
        odds_df=pandas.merge(odds_df, market_mapping_df, on=['Bookie','Market_raw'], how='left')

        #map players
        odds_df=self.map_player_name(odds_df,database,engine,player_table='nba_vw_dim_players',player_exception_table='nba_player_name_manual_mapping')

        #rearrange order
        odds_df=odds_df.reindex(columns=['Bookie','Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line','Odds', 'Over_odd', 'Under_odd', 'Event_Name_raw', 'Home_Team', 'Away_Team','Market_raw', 'Selection_raw', 'Selection_code','Home_Abb', 'Away_Abb','Link'])

        return odds_df

    def get_nba_pb_odds_df(self,database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets'):
        output=[]
        bookie='PB'

        games=requests.get('https://api.pointsbet.com/api/v2/competitions/7176/events/featured?includeLive=true').json()['events']


        for game in games:
            gameid=game['key']
            event_name=game['name']
            Home_Team=game['homeTeam']
            Away_Team=game['awayTeam']
            event_date=game['startsAt'][:10]
            link='https://pointsbet.com.au/basketball/NBA/'+gameid

            res=requests.get('https://api.pointsbet.com/api/v2/events/'+gameid)
            
            if res.status_code==200:
                allmarkets=res.json()
                
            else:
                next

            if 'fixedOddsMarkets' in allmarkets:
                allmarkets=allmarkets['fixedOddsMarkets']
            else:
                next
                
            for market in allmarkets:
                market_group=market['groupName']
                market_name=market['name']
                market_name=market_name.replace(' ('+event_name+')','')
                outcomes=market['outcomes']
                
                for outcome in outcomes:
                    selection_code=outcome['name']
                    odds=float(outcome['price'])
                    #get default over or under attribute
                    if re.search('(Over|Under)',selection_code):
                        over_under=re.search('Over|Under',selection_code).group(0)
                    elif re.search('\+',selection_code):
                        over_under='Over'
                    else:
                        over_under=''
                    
                    numstr=re.compile('3-Pointers|76er')
                    selection_mod=numstr.sub('',selection_code)

                    try:
                        temp_name=selection_mod
                        numstr=re.compile('3-Pointers|76ers')
                        temp_name=numstr.sub('',temp_name)
                        line=float(re.search('(\+|\-)*\d+\.*\d*',selection_mod).group())
                        if line==int(line):
                            line=line-0.5
                    except Exception:
                        line=None
                    
                    #player_name
                    selection_code2=re.split(' Over | Under | To Get ',selection_code)[0]

                    ####specifications for head to head market
                    if market_name in ['Match Handicap','Match Result']:
                        if line==None:
                            line=0
                        selection_code='Home'
                        if Home_Team in selection_code3:
                            line=-line
                            over_under='Over'
                        elif Away_Team in selection_code3:
                            over_under='Under'


                    ####specifications for margin Market
                    if market_name in margin_line_list:
                        selection_code='Home'
                        if Home_Team in selection_code3:
                            line=-line
                            over_under='Over'
                        elif Away_Team in selection_code3:
                            over_under='Under'


                
                    elif market_name in ['Match Winner','1st Half Result','2nd Half Result','1st Quarter Result','2nd Quarter Result','3rd Quarter Result','4th Quarter Result']:
                        selection_code='Home'
                        line=0
                        if Home_Team in selection_code3:
                            over_under='Over'
                        elif Away_Team in selection_code3:
                            over_under='Under'

                    #calc over/under odds columns
                    Over_odd=None
                    Under_odd=None
                    if over_under=='Over':
                        Over_odd=odds
                    elif over_under=='Under':
                        Under_odd=odds

                    #print(event_name,Home_Team,Away_Team,event_date,market_group,market_name,selection_code,over_under,odds)

                    row=[bookie,event_name,event_date,Home_Team,Away_Team,market_name,selection_code,selection_code2,over_under,line,odds,0,0,link]
                    output.append(row)

        row=[bookie,event_name,event_date,'Sacramento Kings','Portland Trail Blazers','Player Points Over/Under','dummy','dummy','Under',0,0,0,0,link]
        output.append(row)
        row=[bookie,event_name,event_date,'Sacramento Kings','Portland Trail Blazers','Player Points Over/Under','dummy','dummy','Over',0,0,0,0,link]
        output.append(row)

        df = pandas.DataFrame(output,columns =['Bookie','Event_Name_raw','Event_Date','Home_Team','Away_Team','Market_raw','Selection_raw','Selection_code','Outcome','Line','Odds','Over_odd','Under_odd','Link'])

                

        #get standard event_name
        odds_df=df
        team_df=self.read_sql_into_df(database,team_mapping_table)[['TEAM_NAME','TEAM_NAME_PB','TEAM_ABBREVIATION']]
        team_df.columns=['Home_Team_std','Home_Team','Home_Abb']
        odds_df=pandas.merge(odds_df, team_df, on=['Home_Team'], how='left',suffixes=('','_h'))
        team_df.columns=['Away_Team_std','Away_Team','Away_Abb']
        odds_df=pandas.merge(odds_df, team_df, on=['Away_Team'], how='left',suffixes=('','_a'))
        #odds_df=odds_df.drop(columns=['Home_Team_std_h','Away_Team_std_a'])
        odds_df['Event_Name_std']=odds_df['Away_Team_std']+' At '+odds_df['Home_Team_std']
        odds_df=odds_df.drop(columns=['Home_Team_std','Away_Team_std'])
        market_mapping_df=self.read_sql_into_df(database,market_mapping_table)
        odds_df=pandas.merge(odds_df, market_mapping_df, on=['Bookie','Market_raw'], how='left')

        #map players
        odds_df=self.map_player_name(odds_df,database,engine,player_table='nba_vw_dim_players',player_exception_table='nba_player_name_manual_mapping')

        #rearrange order
        odds_df=odds_df.reindex(columns=['Bookie','Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line','Odds', 'Over_odd', 'Under_odd', 'Event_Name_raw', 'Home_Team', 'Away_Team','Market_raw', 'Selection_raw', 'Selection_code','Home_Abb', 'Away_Abb','Link'])

        return odds_df

    def get_nba_uni_odds_df(self,database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets'):
        output=[]
        bookie='UNI'

        games=requests.get('https://o1-api.aws.kambicdn.com/offering/v2018/ubau/listView/basketball/nba.json?lang=en_AU&market=AU&client_id=2&channel_id=1&useCombined=true').json()['events']


        for game in games:
            gameid=game['event']['id']
            event_name=game['event']['englishName']
            Home_Team=game['event']['homeName']
            Away_Team=game['event']['awayName']
            event_date=game['event']['start'][:10]
            link='https://www.unibet.com.au/betting/sports/event/'+str(gameid)

            res=requests.get('https://o1-api.aws.kambicdn.com/offering/v2018/ubau/betoffer/event/{}.json?lang=en_AU&market=AU'.format(gameid))
            #print(res)
            
            #if res.status_code==200:
            #    allmarkets=res.json()['betOffers']
                
            #else:
            #    next
            allmarkets=res.json()['betOffers'] 
            for market in allmarkets:
                market_group=market['criterion']['label']
                market_name=market['criterion']['label']
                
                outcomes=market['outcomes']
                selection_code2=''
                for outcome in outcomes:
                    selection_code=outcome['englishLabel']
                    try:
                        selection_code2=outcome['participant']
                    except Exception:
                        pass
                    if len(selection_code2.split(', '))>1:
                        selection_code2=selection_code2.split(', ')[1]+' '+selection_code2.split(', ')[0]


                    try:
                        odds=float(outcome['odds'])/1000
                    except Exception:
                        odds=0
                    #get default over or under attribute
                    if re.search('(Over|Under)',selection_code):
                        over_under=re.search('Over|Under',selection_code).group(0)
                    elif re.search('\+',selection_code):
                        over_under='Over'
                    else:
                        over_under=''
                    
                    numstr=re.compile('3-Pointers|76er')
                    selection_mod=numstr.sub('',selection_code)

                    try:
                        line=float(re.search('(\+|\-)*\d+\.*\d*',selection_mod).group())
                        if line==int(line):
                            line=line-0.5
                    except Exception:
                        line=None
                    



                    #calc over/under odds columns
                    Over_odd=None
                    Under_odd=None
                    if over_under=='Over':
                        Over_odd=odds
                    elif over_under=='Under':
                        Under_odd=odds

                    #print(event_name,Home_Team,Away_Team,event_date,market_group,market_name,selection_code,over_under,odds)

                    row=[bookie,event_name,event_date,Home_Team,Away_Team,market_name,selection_code,selection_code2,over_under,line,odds,0,0,link]
                    output.append(row)

        row=[bookie,event_name,event_date,'Sacramento Kings','Portland Trail Blazers','Player Points Over/Under','dummy','dummy','Under',0,0,0,0,link]
        output.append(row)
        row=[bookie,event_name,event_date,'Sacramento Kings','Portland Trail Blazers','Player Points Over/Under','dummy','dummy','Over',0,0,0,0,link]
        output.append(row)

        df = pandas.DataFrame(output,columns =['Bookie','Event_Name_raw','Event_Date','Home_Team','Away_Team','Market_raw','Selection_raw','Selection_code','Outcome','Line','Odds','Over_odd','Under_odd','Link'])

        #get standard event_name
        odds_df=df
        team_df=self.read_sql_into_df(database,team_mapping_table)[['TEAM_NAME','TEAM_NAME_UNI','TEAM_ABBREVIATION']]
        team_df.columns=['Home_Team_std','Home_Team','Home_Abb']
        odds_df=pandas.merge(odds_df, team_df, on=['Home_Team'], how='left',suffixes=('','_h'))
        team_df.columns=['Away_Team_std','Away_Team','Away_Abb']
        odds_df=pandas.merge(odds_df, team_df, on=['Away_Team'], how='left',suffixes=('','_a'))
        #odds_df=odds_df.drop(columns=['Home_Team_std_h','Away_Team_std_a'])
        odds_df['Event_Name_std']=odds_df['Away_Team_std']+' At '+odds_df['Home_Team_std']
        odds_df=odds_df.drop(columns=['Home_Team_std','Away_Team_std'])
        market_mapping_df=self.read_sql_into_df(database,market_mapping_table)
        odds_df=pandas.merge(odds_df, market_mapping_df, on=['Bookie','Market_raw'], how='left')

        #map players
        odds_df=self.map_player_name(odds_df,database,engine,player_table='nba_vw_dim_players',player_exception_table='nba_player_name_manual_mapping')

        #rearrange order
        odds_df=odds_df.reindex(columns=['Bookie','Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line','Odds', 'Over_odd', 'Under_odd', 'Event_Name_raw', 'Home_Team', 'Away_Team','Market_raw', 'Selection_raw', 'Selection_code','Home_Abb', 'Away_Abb','Link'])


        return odds_df

    def get_compare_odds_df(self,database,engine,consolidated_odds_all_bookies_df,arber_threshold=0.05):

        
        filter=np.where(consolidated_odds_all_bookies_df['KPI_Name'].isna(),False,True)
        consolidated_odds_all_bookies_df=consolidated_odds_all_bookies_df[filter]
        url_lookup=consolidated_odds_all_bookies_df.groupby(['Bookie','Event_Name_std', 'Match_Component', 'Selection_code_std', 'KPI_Name']).agg({'Bookie':'first','Event_Name_std':'first','Match_Component':'first','Selection_code_std':'first','KPI_Name':'first','Link':'first'}).reset_index(drop=True)
        event_lookup=consolidated_odds_all_bookies_df.groupby(['Event_Name_std']).agg({'Event_Name_std':'first','Event_Date':'first'}).reset_index(drop=True)


        simple_odds_df=consolidated_odds_all_bookies_df[['Bookie','Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line','Odds']]
        simple_odds_df=simple_odds_df.groupby(['Bookie','Event_Name_std', 'Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line']).agg({'Bookie':'first','Event_Name_std':'first', 'Match_Component':'first', 'Selection_code_std':'first', 'KPI_Name':'first','Outcome':'first', 'Line':'first','Odds':'max'}).reset_index(drop=True)
        simple_odds_df=pandas.merge(simple_odds_df,event_lookup,on=['Event_Name_std'],how='left')


        filter_over=np.where(simple_odds_df['Outcome']=='Over',True,False)
        filter_under=np.where(simple_odds_df['Outcome']=='Under',True,False)
        over_df=simple_odds_df[filter_over]
        under_df=simple_odds_df[filter_under]

        def pivot_odds_df(odds_df):
            
            pivoted=pandas.pivot_table(odds_df, values='Odds', index=['Event_Name_std', 'Event_Date','Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line'], columns=['Bookie'],aggfunc=np.max).reset_index()
            best=pandas.pivot_table(odds_df, values='Odds', index=['Event_Name_std', 'Event_Date','Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line'], columns=[],aggfunc=np.max).reset_index()
            conso_df=pandas.merge(pivoted,best,on=['Event_Name_std','Event_Date','Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line'],how='left')
            if 'SB' not in conso_df:
                conso_df.loc[:,'SB']=0
            if 'Tab' not in conso_df:
                conso_df.loc[:,'Tab']=0
            if 'BE' not in conso_df:
                conso_df.loc[:,'BE']=0
            if 'NEDS' not in conso_df:
                conso_df.loc[:,'NEDS']=0
            if 'PB' not in conso_df:
                conso_df.loc[:,'PB']=0
            if 'UNI' not in conso_df:
                conso_df.loc[:,'UNI']=0
            

            return conso_df

        over_df=pivot_odds_df(over_df)
        under_df=pivot_odds_df(under_df)

        analysis_df=pandas.merge(over_df,under_df,on=['Event_Name_std','Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name'],how='left',suffixes=('_o','_u'))
        #remove empty odds
        filter=np.where(analysis_df['Odds_o'].isna()|analysis_df['Odds_u'].isna(),False,True)
        analysis_df=analysis_df[filter]
        #remove lines that are not matching
        filter=np.where(analysis_df['Line_o']<=analysis_df['Line_u'],True,False)
        analysis_df=analysis_df[filter]
        #filter=np.where(analysis_df['Match_Component']=='Player',True,False)
        #analysis_df=analysis_df[filter]
        filter=np.where((analysis_df['Odds_o']>=1.5)&(analysis_df['Odds_u']>=1.5),True,False)
        analysis_df=analysis_df[filter]
        filter=np.where((analysis_df['Line_o']>=-0.5)&(analysis_df['Line_u']<=0.5),False,True)
        analysis_df=analysis_df[filter]
        
        
        analysis_df['line_gap']=analysis_df['Line_u']-analysis_df['Line_o']
        analysis_df['line_gap_pctg']=np.where((analysis_df['KPI_Name']=='Margin_Line'),analysis_df['line_gap']/30,2*analysis_df['line_gap']/(analysis_df['Line_u']+analysis_df['Line_o']))
        analysis_df['cost_index']=(1/analysis_df['Odds_o']+1/analysis_df['Odds_u'])-1
        #analysis_df['middle_return_kpi']=analysis_df['line_gap_pctg']/analysis_df['cost_index']
        arber_filter=np.where((analysis_df['cost_index']<0)|((analysis_df['cost_index']<=arber_threshold)&(analysis_df['line_gap']>0)),True,False)
        analysis_df=analysis_df[arber_filter]

        


        
        analysis_df['Over_Bookie']=np.where(analysis_df['Odds_o']==analysis_df['BE_o'],'BE',np.where(analysis_df['Odds_o']==analysis_df['SB_o'],'SB',np.where(analysis_df['Odds_o']==analysis_df['Tab_o'],'Tab',np.where(analysis_df['Odds_o']==analysis_df['NEDS_o'],'NEDS',np.where(analysis_df['Odds_o']==analysis_df['PB_o'],'PB',np.where(analysis_df['Odds_o']==analysis_df['UNI_o'],'UNI',''))))))
        analysis_df['Over_stake']=100*analysis_df['Odds_u']/(analysis_df['Odds_u']+analysis_df['Odds_o'])
        analysis_df['Under_Bookie']=np.where(analysis_df['Odds_u']==analysis_df['BE_u'],'BE',np.where(analysis_df['Odds_u']==analysis_df['SB_u'],'SB',np.where(analysis_df['Odds_u']==analysis_df['Tab_u'],'Tab',np.where(analysis_df['Odds_u']==analysis_df['NEDS_u'],'NEDS',np.where(analysis_df['Odds_u']==analysis_df['PB_u'],'PB',np.where(analysis_df['Odds_u']==analysis_df['UNI_u'],'UNI',''))))))
        analysis_df['Under_stake']=100*analysis_df['Odds_o']/(analysis_df['Odds_u']+analysis_df['Odds_o'])
        analysis_df['Margin']=analysis_df['Over_stake']*analysis_df['Odds_o']-100


        #1.1 plus calculated middle kpi
        analysis_df['middle_return_kpi']=0.5+analysis_df['line_gap_pctg']*(analysis_df['Over_stake']*analysis_df['Odds_o']+analysis_df['Under_stake']*analysis_df['Odds_u']-(analysis_df['Over_stake']+analysis_df['Under_stake']))/(-analysis_df['Margin'])

        
        analysis_df=analysis_df.merge(url_lookup, left_on=['Over_Bookie','Event_Name_std', 'Match_Component', 'Selection_code_std', 'KPI_Name'], right_on=['Bookie','Event_Name_std', 'Match_Component', 'Selection_code_std', 'KPI_Name'], suffixes=('', '_o'))
        analysis_df=analysis_df.merge(url_lookup, left_on=['Under_Bookie','Event_Name_std', 'Match_Component', 'Selection_code_std', 'KPI_Name'], right_on=['Bookie','Event_Name_std', 'Match_Component', 'Selection_code_std', 'KPI_Name'], suffixes=('', '_u'))

        analysis_df=analysis_df.sort_values(by='cost_index', ascending=True)

        
        return analysis_df

    def get_consolidated_odds(self,database,engine,arber_threshold=0,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets',output_name='nba_all_bookies_cons'):
        
        df_list=[]
        try:
            uni=self.get_nba_uni_odds_df(database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets')
            df_list.append(uni)
        except Exception:
            pass

        try:
            be=self.get_nba_be_odds_df(database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets')
            df_list.append(be)
        except Exception:
            pass

        try:
            pb=self.get_nba_pb_odds_df(database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets')
            df_list.append(pb)
        except Exception:
            pass

        try:
            neds=self.get_nba_neds_odds_df(database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets')
            df_list.append(neds)
        except Exception:
            pass

        try:
            sb=self.get_nba_sb_odds_df(database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets')
            df_list.append(sb)
        except Exception:
            pass

        try:
            tab=self.get_nba_tab_odds_df(database,engine,team_mapping_table='nba_dim_teams',market_mapping_table='nba_dim_markets')
            df_list.append(tab)
        except Exception:
            pass

        consolidated_odds_all_bookies_df = pandas.concat(df_list, axis=0)

        #consolidated_odds_all_bookies_df.to_sql(output_name,engine,chunksize=50,method='multi',if_exists='replace',index=False)


        return consolidated_odds_all_bookies_df
    
    def get_arber_notification_header_body(self,arber_notifying_df):
        
        to_notify_df=arber_notifying_df

        def star_ratings(df):
            
            if len(df) !=  0:

                if (df['cost_index']) <= -0.09:
                    return '★★★★★'
                elif (df['cost_index']) <= -0.08:
                    return '★★★★★'
                elif (df['cost_index']) <= -0.07:
                    return '★★★★★'
                elif (df['cost_index']) <= -0.06:
                    return '★★★★★'
                elif (df['cost_index']) <= -0.05:
                    return '★★★★☆'
                elif (df['cost_index']) <= 0:
                    return '★★★☆☆'
                else:
                    return ''
            else:
                return ''

        def get_recom_string(to_notify_df):

            home=to_notify_df['home']
            away=to_notify_df['away']
            if to_notify_df['KPI_Name']=='Margin_Line':
                recom=to_notify_df['rating']+': '+to_notify_df['Event_Name_std']+' - '+to_notify_df['Match_Component']+' - '+to_notify_df['KPI_Name']+':\n Bet $'+to_notify_df['Over_stake_str']+' on '+home+' '+to_notify_df['Line_o_str2']+' @ odd: '+to_notify_df['Odds_o_str']+' in '+to_notify_df['Over_Bookie']+';\n'+' Bet $'+to_notify_df['Under_stake_str']+' on '+away+' '+to_notify_df['Line_u_str']+' @ odd: '+to_notify_df['Odds_u_str']+' in '+to_notify_df['Under_Bookie']+'\nGuaranteed profit is $'+to_notify_df['Margin_str']+'\n\n'+to_notify_df['odds_list']+'\n\nBet Over Here: '+to_notify_df['Link']+'\nBet Under Here: '+to_notify_df['Link_u']+'\n\n'
            elif to_notify_df['Match_Component']=='Player':
                recom=to_notify_df['rating']+': '+to_notify_df['Event_Name_std']+' - '+to_notify_df['Match_Component']+' - '+to_notify_df['KPI_Name']+' - '+to_notify_df['Selection_code_std']+':\n Bet $'+to_notify_df['Over_stake_str']+' on OVER '+to_notify_df['Line_o_str']+' @ odd: '+to_notify_df['Odds_o_str']+' in '+to_notify_df['Over_Bookie']+';\n'+' Bet $'+to_notify_df['Under_stake_str']+' on UNDER '+to_notify_df['Line_u_str']+' @ odd: '+to_notify_df['Odds_u_str']+' in '+to_notify_df['Under_Bookie']+'\nGuaranteed profit is $'+to_notify_df['Margin_str']+'\n\n'+to_notify_df['odds_list']+'\n\nBet Over Here: '+to_notify_df['Link']+'\nBet Under Here: '+to_notify_df['Link_u']+'\n\n'           
            else:
                recom=to_notify_df['rating']+': '+to_notify_df['Event_Name_std']+' - '+to_notify_df['Match_Component']+' - '+to_notify_df['KPI_Name']+':\n Bet $'+to_notify_df['Over_stake_str']+' on OVER '+to_notify_df['Line_o_str']+' @ odd: '+to_notify_df['Odds_o_str']+' in '+to_notify_df['Over_Bookie']+';\n'+' Bet $'+to_notify_df['Under_stake_str']+' on UNDER '+to_notify_df['Line_u_str']+' @ odd: '+to_notify_df['Odds_u_str']+' in '+to_notify_df['Under_Bookie']+'\nGuaranteed profit is $'+to_notify_df['Margin_str']+'\n\n'+to_notify_df['odds_list']+'\n\nBet Over Here: '+to_notify_df['Link']+'\nBet Under Here: '+to_notify_df['Link_u']+'\n\n'
            return recom
        
        
        if to_notify_df.shape[0]==0:
            header_body_list=["",""]
        else:
            to_notify_df['Over_stake_str']=(to_notify_df['Over_stake']).map('{:,.2f}'.format)
            to_notify_df['Under_stake_str']=(to_notify_df['Under_stake']).map('{:,.2f}'.format)
            to_notify_df['Line_o_str']=(to_notify_df['Line_o']).map('{:,.1f}'.format)
            to_notify_df['Line_u_str']=(to_notify_df['Line_u']).map('{:,.1f}'.format)
            to_notify_df['Line_o_str2']=(-to_notify_df['Line_o']).map('{:,.1f}'.format)
            to_notify_df['Odds_o_str']=(to_notify_df['Odds_o']).map('{:,.2f}'.format)
            to_notify_df['Odds_u_str']=(to_notify_df['Odds_u']).map('{:,.2f}'.format)
            to_notify_df['Margin_str']=(to_notify_df['Margin']).map('{:,.2f}'.format)
            to_notify_df['Over_Bookie']=(to_notify_df['Over_Bookie']).map({'SB': 'SportsBet', 'BE': 'BetEasy','Tab':'Tab','NEDS':'NEDS','PB':'PointsBet','UNI':'UniBet'})
            to_notify_df['Under_Bookie']=(to_notify_df['Under_Bookie']).map({'SB': 'SportsBet', 'BE': 'BetEasy','Tab':'Tab','NEDS':'NEDS','PB':'PointsBet','UNI':'UniBet'})


            to_notify_df['SB_o_str']=(to_notify_df['SB_o']).map('{:,.2f}'.format)
            to_notify_df['Tab_o_str']=(to_notify_df['Tab_o']).map('{:,.2f}'.format)
            to_notify_df['NEDS_o_str']=(to_notify_df['NEDS_o']).map('{:,.2f}'.format)
            to_notify_df['PB_o_str']=(to_notify_df['PB_o']).map('{:,.2f}'.format)
            to_notify_df['BE_o_str']=(to_notify_df['BE_o']).map('{:,.2f}'.format)
            to_notify_df['UNI_o_str']=(to_notify_df['UNI_o']).map('{:,.2f}'.format)

            to_notify_df['SB_u_str']=(to_notify_df['SB_u']).map('{:,.2f}'.format)
            to_notify_df['Tab_u_str']=(to_notify_df['Tab_u']).map('{:,.2f}'.format)
            to_notify_df['NEDS_u_str']=(to_notify_df['NEDS_u']).map('{:,.2f}'.format)
            to_notify_df['PB_u_str']=(to_notify_df['PB_u']).map('{:,.2f}'.format)
            to_notify_df['BE_u_str']=(to_notify_df['BE_u']).map('{:,.2f}'.format)
            to_notify_df['UNI_u_str']=(to_notify_df['UNI_u']).map('{:,.2f}'.format)

            to_notify_df['home']=np.where(to_notify_df['Event_Name_std'].str.contains(" At "),to_notify_df['Event_Name_std'].str.split(" At ", n = 1, expand = True)[1],"")
            to_notify_df['away']=np.where(to_notify_df['Event_Name_std'].str.contains(" At "),to_notify_df['Event_Name_std'].str.split(" At ", n = 1, expand = True)[0],"")

            to_notify_df['odds_list']='Bookies  Over/Under'+'\nSB:       '+to_notify_df['SB_o_str']+'/'+to_notify_df['SB_u_str']+'\nTab:      '+to_notify_df['Tab_o_str']+'/'+to_notify_df['Tab_u_str']+'\nBE:       '+to_notify_df['BE_o_str']+'/'+to_notify_df['BE_u_str']+'\nPB:       '+to_notify_df['PB_o_str']+'/'+to_notify_df['PB_u_str']+'\nNEDS:     '+to_notify_df['NEDS_o_str']+'/'+to_notify_df['NEDS_u_str']+'\nUNI:     '+to_notify_df['UNI_o_str']+'/'+to_notify_df['UNI_u_str']
            


            to_notify_df['rating'] = to_notify_df.apply(star_ratings, axis = 1)
            to_notify_df['recommendation']=to_notify_df.apply(get_recom_string, axis = 1)


            body=to_notify_df['recommendation'].str.cat(sep='\n\n')

            header='Risk Free Betting!!! (DO NOT REPLY)'

            header_body_list=[header,body]
        
        return header_body_list

    def get_middle_notification_header_body(self,middle_notifying_df):
            
        to_notify_df=middle_notifying_df

        def star_ratings(df):
            
            if len(df) !=  0:

                if df['middle_return_kpi']<0:
                    return '★★★★★★★★★★★★'
                elif (df['middle_return_kpi'])>= 6:
                    return '★★★★★'
                elif (df['middle_return_kpi'])>= 5:
                    return '★★★★☆'
                elif (df['middle_return_kpi']) >= 4:
                    return '★★★☆☆'
                elif (df['middle_return_kpi']) >= 3:
                    return '★★☆☆☆'
                elif (df['middle_return_kpi']) >=2:
                    return '★☆☆☆☆'
                else:
                    return ''
            else:
                return ''

        def get_recom_string(to_notify_df):
            home=to_notify_df['home']
            away=to_notify_df['away']
            if to_notify_df['middle_return_kpi']<0:
                rsk_or_guart='Guaranteed Winning'
            else:
                rsk_or_guart='Outlay'

            if to_notify_df['KPI_Name']=='Margin_Line':
                recom=to_notify_df['rating']+': '+to_notify_df['Event_Name_std']+' - '+to_notify_df['Match_Component']+' - '+to_notify_df['KPI_Name']+':\n Bet $'+to_notify_df['Over_stake_str']+' on '+home+' '+to_notify_df['Line_o_str2']+' @ odd: '+to_notify_df['Odds_o_str']+' in '+to_notify_df['Over_Bookie']+';\n'+' Bet $'+to_notify_df['Under_stake_str']+' on '+away+' '+to_notify_df['Line_u_str']+' @ odd: '+to_notify_df['Odds_u_str']+' in '+to_notify_df['Under_Bookie']+';\n'+rsk_or_guart+' is $'+to_notify_df['Margin_str']+'\nPossible Winning is :$'+to_notify_df['middle_return_str']+'\n\n'+to_notify_df['odds_list']+'\n\nBet Over Here: '+to_notify_df['Link']+'\nBet Under Here: '+to_notify_df['Link_u']+'\n\n'
            elif to_notify_df['Match_Component']=='Player':
                recom=to_notify_df['rating']+': '+to_notify_df['Event_Name_std']+' - '+to_notify_df['Match_Component']+' - '+to_notify_df['KPI_Name']+' - '+to_notify_df['Selection_code_std']+':\n Bet $'+to_notify_df['Over_stake_str']+' on OVER '+to_notify_df['Line_o_str']+' @ odd: '+to_notify_df['Odds_o_str']+' in '+to_notify_df['Over_Bookie']+';\n'+' Bet $'+to_notify_df['Under_stake_str']+' on UNDER '+to_notify_df['Line_u_str']+' @ odd: '+to_notify_df['Odds_u_str']+' in '+to_notify_df['Under_Bookie']+';\n'+rsk_or_guart+' is $'+to_notify_df['Margin_str']+'\nPossible Winning is :$'+to_notify_df['middle_return_str']+'\n\n'+to_notify_df['odds_list']+'\n\nBet Over Here: '+to_notify_df['Link']+'\nBet Under Here: '+to_notify_df['Link_u']+'\n\n'
            else:
                recom=to_notify_df['rating']+': '+to_notify_df['Event_Name_std']+' - '+to_notify_df['Match_Component']+' - '+to_notify_df['KPI_Name']+':\n Bet $'+to_notify_df['Over_stake_str']+' on OVER '+to_notify_df['Line_o_str']+' @ odd: '+to_notify_df['Odds_o_str']+' in '+to_notify_df['Over_Bookie']+';\n'+' Bet $'+to_notify_df['Under_stake_str']+' on UNDER '+to_notify_df['Line_u_str']+' @ odd: '+to_notify_df['Odds_u_str']+' in '+to_notify_df['Under_Bookie']+';\n'+rsk_or_guart+' is $'+to_notify_df['Margin_str']+'\nPossible Winning is :$'+to_notify_df['middle_return_str']+'\n\n'+to_notify_df['odds_list']+'\n\nBet Over Here: '+to_notify_df['Link']+'\nBet Under Here: '+to_notify_df['Link_u']+'\n\n'
            return recom

        if to_notify_df.shape[0]==0:
            header_body_list=["",""]
        else:
            to_notify_df['middle_return']=to_notify_df['Over_stake']*to_notify_df['Odds_o']+to_notify_df['Under_stake']*to_notify_df['Odds_u']-(to_notify_df['Over_stake']+to_notify_df['Under_stake'])


            to_notify_df['Over_stake_str']=(to_notify_df['Over_stake']).map('{:,.2f}'.format)
            to_notify_df['Under_stake_str']=(to_notify_df['Under_stake']).map('{:,.2f}'.format)
            to_notify_df['Line_o_str']=(to_notify_df['Line_o']).map('{:,.1f}'.format)
            to_notify_df['Line_u_str']=(to_notify_df['Line_u']).map('{:,.1f}'.format)
            to_notify_df['Line_o_str2']=(-to_notify_df['Line_o']).map('{:,.1f}'.format)
            to_notify_df['Odds_o_str']=(to_notify_df['Odds_o']).map('{:,.2f}'.format)
            to_notify_df['Odds_u_str']=(to_notify_df['Odds_u']).map('{:,.2f}'.format)
            to_notify_df['Margin_str']=(to_notify_df['Margin']).map('{:,.2f}'.format)
            to_notify_df['middle_return_str']=(to_notify_df['middle_return']).map('{:,.2f}'.format)
            to_notify_df['Over_Bookie']=(to_notify_df['Over_Bookie']).map({'SB': 'SportsBet', 'BE': 'BetEasy','Tab':'Tab','NEDS':'NEDS','PB':'PointsBet','UNI':'UniBet'})
            to_notify_df['Under_Bookie']=(to_notify_df['Under_Bookie']).map({'SB': 'SportsBet', 'BE': 'BetEasy','Tab':'Tab','NEDS':'NEDS','PB':'PointsBet','UNI':'UniBet'})
            
            to_notify_df['SB_o_str']=(to_notify_df['SB_o']).map('{:,.2f}'.format)
            to_notify_df['Tab_o_str']=(to_notify_df['Tab_o']).map('{:,.2f}'.format)
            to_notify_df['NEDS_o_str']=(to_notify_df['NEDS_o']).map('{:,.2f}'.format)
            to_notify_df['PB_o_str']=(to_notify_df['PB_o']).map('{:,.2f}'.format)
            to_notify_df['BE_o_str']=(to_notify_df['BE_o']).map('{:,.2f}'.format)
            to_notify_df['UNI_o_str']=(to_notify_df['UNI_o']).map('{:,.2f}'.format)

            to_notify_df['SB_u_str']=(to_notify_df['SB_u']).map('{:,.2f}'.format)
            to_notify_df['Tab_u_str']=(to_notify_df['Tab_u']).map('{:,.2f}'.format)
            to_notify_df['NEDS_u_str']=(to_notify_df['NEDS_u']).map('{:,.2f}'.format)
            to_notify_df['PB_u_str']=(to_notify_df['PB_u']).map('{:,.2f}'.format)
            to_notify_df['BE_u_str']=(to_notify_df['BE_u']).map('{:,.2f}'.format)
            to_notify_df['UNI_u_str']=(to_notify_df['UNI_u']).map('{:,.2f}'.format)

            to_notify_df['home']=np.where(to_notify_df['Event_Name_std'].str.contains(" At "),to_notify_df['Event_Name_std'].str.split(" At ", n = 1, expand = True)[1],"")
            to_notify_df['away']=np.where(to_notify_df['Event_Name_std'].str.contains(" At "),to_notify_df['Event_Name_std'].str.split(" At ", n = 1, expand = True)[0],"")

            to_notify_df['odds_list']='Bookies  Over/Under'+'\nSB:       '+to_notify_df['SB_o_str']+'/'+to_notify_df['SB_u_str']+'\nTab:      '+to_notify_df['Tab_o_str']+'/'+to_notify_df['Tab_u_str']+'\nBE:       '+to_notify_df['BE_o_str']+'/'+to_notify_df['BE_u_str']+'\nPB:       '+to_notify_df['PB_o_str']+'/'+to_notify_df['PB_u_str']+'\nNEDS:     '+to_notify_df['NEDS_o_str']+'/'+to_notify_df['NEDS_u_str']+'\nUNI:     '+to_notify_df['UNI_o_str']+'/'+to_notify_df['UNI_u_str']
    


            to_notify_df['rating'] = to_notify_df.apply(star_ratings, axis = 1)


            to_notify_df['recommendation']=to_notify_df.apply(get_recom_string, axis = 1)    

            body=to_notify_df['recommendation'].str.cat(sep='\n\n')


            header='Betting on middle! (DO NOT REPLY)'

            header_body_list=[header,body]
        
        return header_body_list
     
    def get_notifying_dfv2(self,database,engine,compare_odds_df,sql_table_name='nba_arber_recommendation_log',arber_threshold=-0.05,middle_threshold=4):
        filter=np.where((compare_odds_df['middle_return_kpi']>=0)&(compare_odds_df['middle_return_kpi']<1) & (compare_odds_df['cost_index']<=arber_threshold),True,False)
        arber_compare_df=compare_odds_df[filter]
        
        filter=np.where((compare_odds_df['middle_return_kpi']>=middle_threshold)|(compare_odds_df['middle_return_kpi']<0),True,False)
        middle_df=compare_odds_df[filter]
        
        today_date=datetime.date.today().strftime("%m/%d/%Y")
        now = datetime.datetime.now()
        current_time = now.strftime("%H:%M:%S")

        existing_arber_reco_df_org=self.read_sql_into_df(database,sql_table_name)
        existing_arber_reco_df_org=existing_arber_reco_df_org[['Event_Name_std','Event_Date','Match_Component','Selection_code_std','KPI_Name','cost_index']]

        arber_compare_df['run_date']=today_date+' '+current_time
        middle_df['run_date']=today_date+' '+current_time
        
        to_notify_df_arber=pandas.merge(arber_compare_df, existing_arber_reco_df_org, on=['Event_Name_std','Event_Date','Match_Component','Selection_code_std','KPI_Name'], how='left',suffixes=('','_y'))
        to_notify_df_middle=pandas.merge(middle_df, existing_arber_reco_df_org, on=['Event_Name_std','Event_Date','Match_Component','Selection_code_std','KPI_Name'], how='left',suffixes=('','_y'))
       
        filter=to_notify_df_arber['cost_index_y'].isna()
        to_notify_df_arber=to_notify_df_arber[filter]
        arber_log_df=to_notify_df_arber[['Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','cost_index','Over_Bookie','Over_stake','Line_o','Odds_o','Under_Bookie','Under_stake','Line_u','Odds_u']]

        filter=to_notify_df_middle['cost_index_y'].isna()
        to_notify_df_middle=to_notify_df_middle[filter]
        middle_log_df=to_notify_df_middle[['Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','cost_index','Over_Bookie','Over_stake','Line_o','Odds_o','Under_Bookie','Under_stake','Line_u','Odds_u']]
        

        #save_to_sql
        

        
        ###################################################################
        arber_log_df.to_sql(sql_table_name,engine,chunksize=50,method='multi',if_exists='append',index=False)

        ###################################################################      
        middle_log_df.to_sql(sql_table_name,engine,chunksize=50,method='multi',if_exists='append',index=False)


        return [to_notify_df_arber,to_notify_df_middle]
        
    def telegram_bot_sendtext(self,chat_id='-1001362763351',bot_message='hello',bot_token='1004217287:AAFRu5CD-3s70TgnPLbdKqIkKoqWWywkrco'):
        msgs=bot_message.split('\n\n\n\n')
        for msg in msgs:
            params = {"chat_id": str(chat_id),"text": msg,"parse_mode": "HTML"}
            requests.get("https://api.telegram.org/bot{}/sendMessage".format(bot_token),params=params)

    def wxbot_send_update(self,wxbot,group='球球球', bot_msg='hello'):
        msgs=bot_msg.split('\n\n\n\n')
        for msg in msgs:
            wxbot.groups().search(group)[0].send(bot_msg)

    def bookies_wrapper(self,database,engine,spread):   #wxbot=wxbot):
        try:

            consolidated_odds_all_bookies_df = self.get_consolidated_odds(database,engine)
            arber_analysis_df=self.get_compare_odds_df(database,engine,consolidated_odds_all_bookies_df,arber_threshold=0.1)
            #try:
                #consolidated_odds_all_bookies_df.to_sql('nba_odds_all',engine,chunksize=50,method='multi',if_exists='replace',index=False)
                #save all odds
            #except Exception:
                #print('cannot save odds')
            try:
                spread.df_to_sheet(arber_analysis_df, index=False, sheet='arber analysis', start='A1', replace=True)
                print('load into googlesheet')
            except Exception:
                print('arber analysis into googlesheet failed')
            
            notify_list=self.get_notifying_dfv2(database,engine,arber_analysis_df,sql_table_name='nba_arber_recommendation_log',arber_threshold=-0.05,middle_threshold=2.5)##-0.05, 2
            
            get_arber_notifying_df=notify_list[0]
            get_middle_notifying_df=notify_list[1]
            
            email_body_arb=self.get_arber_notification_header_body(get_arber_notifying_df)
            email_body_mid=self.get_middle_notification_header_body(get_middle_notifying_df)
            #print('arber: '+email_body_arb[1])
            try:                    
                self.telegram_bot_sendtext(chat_id='-1001362763351',bot_message=email_body_arb[1],bot_token='1004217287:AAFRu5CD-3s70TgnPLbdKqIkKoqWWywkrco')
                self.telegram_bot_sendtext(chat_id='-1001362763351',bot_message=email_body_mid[1],bot_token='1004217287:AAFRu5CD-3s70TgnPLbdKqIkKoqWWywkrco')
                #self.telegram_bot_sendtext(chat_id='1029752482',bot_message=email_body_arb[1],bot_token='1004217287:AAFRu5CD-3s70TgnPLbdKqIkKoqWWywkrco')
                #self.telegram_bot_sendtext(chat_id='1029752482',bot_message=email_body_mid[1],bot_token='1004217287:AAFRu5CD-3s70TgnPLbdKqIkKoqWWywkrco')
                pass

            except Exception:
                print('telegram update failed')

            #try:                    
            #    nba_obj.wxbot_send_update(wxbot,group='球球球',bot_msg=email_body_arb[1])
            #    nba_obj.wxbot_send_update(wxbot,group='球球球',bot_msg=email_body_mid[1])

            #except Exception:
            #    print('wechat update failed')

            #nba_obj.send_email_v2(email_body_arb)
            #nba_obj.send_email_v2(email_body_mid)



            now = datetime.datetime.now()
            current_time = now.strftime("%H:%M:%S")
            
            print('NBA refreshed refreshed @'+str(datetime.date.today())+' '+current_time)

        except Exception:
            print('bookies analysis failed')


def get_mins(MIN_string):
    MIN_string=str(MIN_string)
    if MIN_string:
        y=re.search('^\d+',MIN_string)
        if y:
            y=int(y.group())
        else:
            pass
    else:
        y=None
    return y

def refresh_tableau():
    driver = webdriver.Firefox(executable_path='C:\Program Files\geckodriver\geckodriver.exe')
    driver.get('https://public.tableau.com/profile/rzanalytics#!/vizhome/nba_analysis/OddsAnalysis')
    try:
        driver.find_elements_by_class_name("login-link")[0].click()
        inputElement = driver.find_element_by_id("login-email")
        inputElement.send_keys('zitian1211@gmail.com')
        inputElement = driver.find_element_by_id("login-password")
        inputElement.send_keys('ilovethisgame1!')
        inputElement.submit()
    except Exception:
        pass

    time.sleep(40)
    driver.find_elements_by_class_name("viz-refresh-extract")[0].click()
    driver.close()

def main():
    #daily_bs_into_sql()
    #schedule.every().day.at("20:00").do(daily_bs_into_sql)
    #schedule.every().day.at("05:00").do(daily_bs_into_sql)
    #schedule.every().day.at("13:30").do(daily_bs_into_sql)
    #while True:
    #    schedule.run_pending()
    #    time.sleep(1)

    rz_nba=Rz_NBA()


    dw=pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-UJKV8TQ\DEV1;DATABASE=datawarehouse;Trusted_Connection=yes;')
    engine = sqlalchemy.create_engine("mssql+pyodbc://sa:Tianxiaomiao6!@DESKTOP-UJKV8TQ\DEV1/datawarehouse?driver=SQL+Server")
    
    engine.connect()
    spread=gspread_pandas.Spread('NBA_ANALYSIS')
    #wxbot=Bot()#initiate wechat bot
    #distribution_list=[['Ronny ', 'zitian1211@gmail.com'],['Jason','wance605@gmail.com'],['Lee','dogginoz@hotmail.com'],['老婆','tianwenqinggd@gmail.com'],['赵星宇','zhaoxingyu331@163.com']]

    #rz_nba.prepare_commentary(dw,engine)
    def stats_wrapper(nba_obj=rz_nba,database=dw,engine=engine):
        try:
            nba_obj.backup_tables(database,engine,sql_table_name_list=['nba_recent_player_boxscore','nba_season_all_games'])
            nba_obj.get_nba_stats(database,engine)
            nba_obj.sync_sql_table_to_gs(database,sql_table_name='nba_vw_recommendation_log_results_with_outcome',gspread_name='NBA_ANALYSIS',sheet_name='outcome_log')
            nba_obj.sync_sql_table_to_gs(database,sql_table_name='nba_vw_arber_recommendation_log_results_with_outcome',gspread_name='NBA_ANALYSIS',sheet_name='arber_outcome_log')
            #nba_obj.backup_tables(database,engine)
            print('stats refreshed')
        except Exception:
            print('stats cannot be refreshed')
    
    def odds_wrapper(nba_obj=rz_nba,database=dw,engine=engine):
        try:
            #s=requests.get('http://affiliate.sportsbet.com.au/xmlfeeds/Basketball.xml')
            
            nba_obj.backup_tables(database,engine,sql_table_name_list=['nba_sports_odds_all','nba_odds_analysis','nba_sports_odds_players','nba_betting_recommendations','nba_recommendation_log'])
            nba_obj.get_nba_sportsbet_odds(database,engine)
            nba_obj.get_nba_tabs_odds(database,engine)



            try:
                nba_obj.write_injury_report(database,engine)
            except Exception:
                print('injury report cannot be generated')
            nba_obj.analyse_odds(database,engine)
            #nba_obj.prepare_commentary(database,engine)

            #nba_obj.send_email(database,engine,sql_table_name='nba_recommendation_log',threshold=1.5)

            #nba_obj.backup_tables(database,engine)
            now = datetime.datetime.now()
            current_time = now.strftime("%H:%M:%S")
            
            print('odds refreshed @'+current_time)

            
            
            
        except Exception:
            print('odds cannot be refreshed')



        try:
            refresh_tableau()
            print('Tableau refreshed')
        except Exception:
            print('Tableau refresh failed')
    #rz_nba.get_nba_stats(dw,engine,days_interval=60)
    #nba_refresh_all(rz_nba,dw,engine)
    #stats_wrapper()
    #odds_wrapper()

    def nba_scheduler():
        rz_nba.bookies_wrapper(database=dw,engine=engine,spread=spread)

    def afl_scheduler():
        afl=AFL_analysis.AFL()
        afl.bookies_wrapper(database=dw,engine=engine,spread=spread)
        #refresh AFL

    nba_scheduler()
    afl_scheduler()

    schedule.every(1).minutes.do(nba_scheduler)
    schedule.every(1).minutes.do(afl_scheduler)
    #schedule.every(120).minutes.do(odds_wrapper)

    #schedule.every().day.at("17:30").do(stats_wrapper)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()

