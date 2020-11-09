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
from Rz_Send_Email import *
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from lxml import etree

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

class AFL():

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

    def create_empty_dataframe_for_player_boxscore(self):
        columns=['GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_CITY', 'PLAYER_ID',
       'PLAYER_NAME', 'START_POSITION', 'COMMENT', 'MIN', 'FGM', 'FGA',
       'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB',
       'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'PLUS_MINUS','GAME_DATE']
        x=pandas.DataFrame(columns=columns,dtype=float)
        #print(x)
        return x
    
    def read_sql_into_df(self,database,table):
        df=pandas.read_sql("select * from "+table,database)
        return df
    
    def analyse_analysis_result(self,database,engine,analysis_result='nba_odds_analysis',output='recommended_output',threshold=1.5):
        odds_analysis_df=self.read_sql_into_df(database,analysis_result)
        filter=np.where(((odds_analysis_df['max_return']>=3))|((odds_analysis_df['choose_15day']==odds_analysis_df['choose']) & (odds_analysis_df['max_return']>=threshold) & (odds_analysis_df['max_return_15day']>=(threshold-0.15))),True,False)
        odds_analysis_df=odds_analysis_df[filter]
        return odds_analysis_df

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

    def send_email(self,database,engine,sql_table_name='nba_recommendation_log',threshold=1.5):
        #this function will send email on any additional item, and then update the sql table at the back
        distribution_list=self.get_email_distribution_list(workbook_name='NBA_email_subscribers',sheet_name='register')
        header_body_list=self.prepare_email_body_string(database,engine,sql_table_name,threshold)
        text=header_body_list[1]
        header=header_body_list[0]
        if text=='':
            pass
            print('no_new_info_to be sent via email')
        else:
            rz_send_email(user_name='nba.odds.analysis@gmail.com',password='Tianxiaomiao6',smtp_server='smtp.gmail.com',port='465',email_text=text,subject=header,distribution_list=distribution_list)
            print('update been sent')

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

    def get_afl_tab_odds_df(self,database,engine,team_mapping_table='afl_dim_teams',market_mapping_table='afl_dim_markets'):
        
        res=requests.get('https://api.beta.tab.com.au/v1/tab-info-service/sports/AFL%20Football/competitions/AFL?jurisdiction=VIC').json()
        
        player_mkt_mapping={'Player Points':'Points','Alternate Player Points':'Points','Player Rebounds':'Rebounds','Alternate Player Rebounds':'Rebounds','Player Assists':'Assists','Alternate Player Assists':'Assists','Player PRA Over/Under':'Pts + Reb + Ast','Player Points + Rebounds + Assists':'Pts + Reb + Ast','Player Threes':'Made Threes','Alternate Player Threes':'Made Threes','NBA 3 Point Shootout 1st Round Score':'Made Threes'}
            
        margin_line_market=['Pick Your Own Line','Line','Extra Line','1st Half Line','1st Half Pick Your Own Line','1st Quarter Line','1st Quarter Pick Your Own Line']
        
        total_line_market=['Total Points Over/Under','1st Half Pick Your Own Total','Team Points Over/Under','Total Points Over/Under','Pick Your Own Total','1st Quarter Pick Your Own Total','2nd Quarter Pick Your Own Total','3rd Quarter Pick Your Own Total','4th Quarter Pick Your Own Total','Extra Total Line']
        
        
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
                bet_option2=bet_option
                for side in market['propositions']:
                    selection_name=side['name']
                    
                    if re.search('Over|Under',selection_name):
                        outcome=re.search('Over|Under',selection_name).group()
                    else:
                        outcome=''
                    odd=side['returnWin']
                    
                    try:
                        line=float(re.search('(\+|\-)*\d+\.*\d*',selection_name).group())
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
                    elif re.search('\d+\+ Disposals$',bet_option):
                        player_name=selection_name
                        player_name=player_name.split(' ')[1].title()+' '+player_name.split(' ')[0].title()
                        player_name=re.sub('\(\w+\)/','',player_name)
                        selection_code=player_name
                        line=float(re.search('\d+',bet_option).group())-0.5
                        outcome='Over'
                        bet_option2='Player Disposals'                       
                
                    ###specifications for margin line market:
                    elif bet_option in margin_line_market:
                        selection_code='Home'
                        
                        if Home_Team in selection_name:
                            outcome='Over'
                            line=-line
                        elif Away_Team in selection_name:
                            outcome='Under'
                            
                    #head to head is an exception of margin line
                    elif 'Head To Head' in bet_option:
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
                    
                    link='https://www.tab.com.au/sports/betting/AFL%20Football/competitions/AFL/matches/'+Home_Team+' v '+Away_Team
                    link=link.replace(' ','%20')



                    row=[bookie,Event_Name,Event_Time,Home_Team,Away_Team,bet_option2,selection_name,selection_code,outcome,line,odd,Over_odd,Under_odd,link]
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
        tab_odds_df['Event_Name_std']=tab_odds_df['Home_Team_std']+' v '+tab_odds_df['Away_Team_std']
        tab_odds_df=tab_odds_df.drop(columns=['Home_Team_std','Away_Team_std'])
        market_mapping_df=self.read_sql_into_df(database,market_mapping_table)
        tab_odds_df=pandas.merge(tab_odds_df, market_mapping_df, on=['Bookie','Market_raw'], how='left')
        
        #map players
        tab_odds_df=self.map_player_name(tab_odds_df,database,engine,player_table='afl_dim_players',player_exception_table='afl_player_name_manual_mapping')

        #rearrange order
        tab_odds_df=tab_odds_df.reindex(columns=['Bookie','Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line','Odds', 'Over_odd', 'Under_odd', 'Event_Name_raw', 'Home_Team', 'Away_Team','Market_raw', 'Selection_raw', 'Selection_code','Home_Abb', 'Away_Abb','Link'])

        return tab_odds_df

    def get_afl_sb_odds_df(self,database,engine,team_mapping_table='afl_dim_teams',market_mapping_table='afl_dim_markets'):
        
        matches_data=requests.get('https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Competitions/4165?displayType=default&eventFilter=matches').json()

        events=matches_data['events']
        margin_line_list=['Head to Head','Line','Pick Your Line','Line - 2nd Quarter Only','Line - 3rd Quarter Only','Line - Last Quarter Only','Quarter Time Line','Half Time Line']
        xml_list=[]


        for event in events:
            event_id=event['id']
            Event_name=event['name']
            Event_date=int(event['startTime'])
            Event_date=datetime.datetime.fromtimestamp(Event_date).isoformat()[:10]
            str_lst=Event_name.split(' v ')
            Home_Team=str_lst[0]
            Away_Team=str_lst[1]
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
                                line=float(re.search('(\+|\-)*\d+\.*\d*',Selc_name).group())
                            except Exception:
                                line=0

                        #Do calculations for player markets:
                        #Do calculations for player markets:
                        if ' - ' in market_name and market_name:
                            player_name=market_name[:market_name.find(' - ')]
                            player_market=market_name[market_name.find(' - ')+3:]
                            selection_code=player_name
                            rev_market_name='Player '+player_market
                        elif re.search('To Get \d+ or More Disposals',market_name):
                            player_name=Selc_name
                            player_market='Disposals'
                            selection_code=player_name
                            line=float(re.search('\d+',market_name).group())-0.5
                            over_under='Over'
                            rev_market_name='Player Disposals'                       
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
                        
                        link='https://www.sportsbet.com.au/betting/australian-rules/afl/'+Event_name+'-'+str(event_id)
                        link=link.replace(' ','-')

                        


                        item=['SB',Event_name,Event_date,rev_market_name,selection_code,Selc_name,over_under,Odds,over_odd,under_odd,line,Home_Team,Away_Team,link]
                        xml_list.append(item)
        

        sb_odds_df=pandas.DataFrame(xml_list,columns=['Bookie','Event_name_raw','Event_Date','Market_raw','Selection_code','Selection_raw','Outcome','Odds','Over_odd','Under_odd','Line','Home_Team','Away_Team','Link'])

        #get standard event_name

        team_df=self.read_sql_into_df(database,team_mapping_table)[['TEAM_NAME','TEAM_NAME_SB','TEAM_ABBREVIATION']]
        team_df.columns=['Home_Team_std','Home_Team','Home_Abb']
        sb_odds_df=pandas.merge(sb_odds_df, team_df, on=['Home_Team'], how='left',suffixes=('','_h'))
        team_df.columns=['Away_Team_std','Away_Team','Away_Abb']
        sb_odds_df=pandas.merge(sb_odds_df, team_df, on=['Away_Team'], how='left',suffixes=('','_a'))

        sb_odds_df['Event_Name_std']=sb_odds_df['Home_Team_std']+' v '+sb_odds_df['Away_Team_std']
        sb_odds_df=sb_odds_df.drop(columns=['Home_Team_std','Away_Team_std'])
        market_mapping_df=self.read_sql_into_df(database,market_mapping_table)
        sb_odds_df=pandas.merge(sb_odds_df, market_mapping_df, on=['Bookie','Market_raw'], how='left')

        #map players
        sb_odds_df=self.map_player_name(sb_odds_df,database,engine,player_table='afl_dim_players',player_exception_table='afl_player_name_manual_mapping')

        #rearrange order
        sb_odds_df=sb_odds_df.reindex(columns=['Bookie','Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line','Odds', 'Over_odd', 'Under_odd', 'Event_Name_raw', 'Home_Team', 'Away_Team','Market_raw', 'Selection_raw', 'Selection_code','Home_Abb', 'Away_Abb','Link'])

        return sb_odds_df

    def get_afl_be_odds_df(self,database,engine,team_mapping_table='afl_dim_teams',market_mapping_table='afl_dim_markets'):
        
        eventlist=requests.get("https://beteasy.com.au/api/sports/navigation/australian-rules/afl/afl-matches").json()['result']['events']
        player_mkt_mapping={'Player Points':'Points','Alternate Player Points':'Points','Player Rebounds':'Rebounds','Alternate Player Rebounds':'Rebounds','Player Assists':'Assists','Alternate Player Assists':'Assists','Player PRA Over/Under':'Pts + Reb + Ast','Player Points + Rebounds + Assists':'Pts + Reb + Ast','Player Threes':'Made Threes','Alternate Player Threes':'Made Threes','NBA 3 Point Shootout 1st Round Score':'Made Threes'}
        margin_line_list=['Line','Pick Your Own Line','1st Half Handicap','2nd Half Handicap','Q1 Handicap','Q2 Handicap','Q3 Handicap','Q4 Handicap']

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
                        
                        event_name_str_list=event_name.split(' v ')
                        home_team=event_name_str_list[0]
                        away_team=event_name_str_list[1]
                        sub_mkt_name=betting_type['EventName']
                        
                        url_id=str(betting_type['EventID'])
                        slug=str(betting_type['Slug'])
                        date_stamp=str(betting_type['DateSlug'])
                        
                        event_date=betting_type['AdvertisedStartTime'][:10]
                        outcomes=betting_type['Outcomes']
                        sub_mkt_name=re.sub(' (\+|\-)*\d+\.*\d*$','',sub_mkt_name)
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
                                
                                #do calc for player disposals
                                if re.search('To Get \d+ or more Disposals',sub_mkt_name):
                                    player_name=selection
                                    selection_code=player_name
                                    line=float(re.search('\d+',sub_mkt_name).group())-0.5
                                    over_under='Over'
                                    market_raw_output='Player Disposals'
                                elif re.search('Disposals Over/Under - ',sub_mkt_name):
                                    player_name=sub_mkt_name.replace('Disposals Over/Under - ','')
                                    selection_code=player_name
                                    line=float(re.search('(\+|\-)*\d+\.*\d*',selection).group())
                                    over_under=re.search('Over|Under',selection).group(0)
                                    market_raw_output='Player Disposals'

                                
                                #do calc for margin market
                                elif market_raw_output in margin_line_list:
                                    selection_code='Home'
                                    if home_team in selection:
                                        line=-line
                                        over_under='Over'
                                    elif away_team in selection:
                                        over_under='Under'
                            
                                elif market_raw_output in ['Head to Head','1st Half Winner']:
                                    selection_code='Home'
                                    line=0
                                    if home_team in selection:
                                        over_under='Over'
                                    elif away_team in selection:
                                        over_under='Under'

                                #do calc for total market
                                elif re.search('Alternate Total Points Over/Under|Total Points Over/Under|1st Half Over/Under|Total Points (Over/Under)',sub_mkt_name):
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
                                link='https://beteasy.com.au/sports-betting/australian-rules/afl/afl-matches/'+slug+'-'+date_stamp+'-'+str(event_id)+'-'+url_id

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
        
        sb_odds_df['Event_Name_std']=sb_odds_df['Home_Team_std']+' v '+sb_odds_df['Away_Team_std']
        sb_odds_df=sb_odds_df.drop(columns=['Home_Team_std','Away_Team_std'])
        market_mapping_df=self.read_sql_into_df(database,market_mapping_table)
        sb_odds_df=pandas.merge(sb_odds_df, market_mapping_df, on=['Bookie','Market_raw'], how='left')
        
        #map players
        sb_odds_df=self.map_player_name(sb_odds_df,database,engine,player_table='afl_dim_players',player_exception_table='afl_player_name_manual_mapping')

        #rearrange order
        sb_odds_df=sb_odds_df.reindex(columns=['Bookie','Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line','Odds', 'Over_odd', 'Under_odd', 'Event_Name_raw', 'Home_Team', 'Away_Team','Market_raw', 'Selection_raw', 'Selection_code','Home_Abb', 'Away_Abb','Link'])


        return sb_odds_df

    def get_afl_neds_odds_df(self,database,engine,team_mapping_table='afl_dim_teams',market_mapping_table='afl_dim_markets'):
        
        headers={'authority':'api.ladbrokes.com.au',
        'method':'GET',
        'path':'/v2/sport/event-request?category_ids=%5B%2223d497e6-8aab-4309-905b-9421f42c9bc5%22%5D&competition_id=87b78eee-0200-408b-b04f-6415ae4c415a',
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
        margin_line_list=['Line','1st Half Line','2nd Half Line','1st Quarter Line','2nd Quarter Line','3rd Quarter Line','4th Quarter Line']

        output=[]

        player_mkt_mapping={'Player Points Markets':'Points','Player Rebounds Markets':'Rebounds','Player Assists Markets':'Assists',"Player 3's Markets":'Made Threes'}
        responds=requests.get(r'https://api.ladbrokes.com.au/v2/sport/event-request?category_ids=%5B%2223d497e6-8aab-4309-905b-9421f42c9bc5%22%5D&competition_id=87b78eee-0200-408b-b04f-6415ae4c415a',headers=headers).json()
        events=responds['next_events']
        
        for event_id in events:
            #calculate_request headers
            game_str=responds['events'][event_id]['name'].replace(' ','-')
            link='https://www.ladbrokes.com.au/sports/australian-rules/afl/'+game_str+'/'+event_id
            headers={'accept-encoding': 'gzip, deflate, br','origin':'https://www.ladbrokes.com.au','referer':link,'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'}
            res=requests.get('https://api.ladbrokes.com.au/v2/sport/event-card',params={'id':event_id},headers=headers)

            
            try:
                data=res.json()
            #if entrants['events'][event_id]['event_type']['name']!='Match':
                    #next
                
                entrants=data['entrants']
                markets=data['markets']
                market_groups=data['market_type_groups']
                prices=data['prices']
                event_detail=data['events'][event_id]
                event_slug=event_detail['slug']
                event_name=event_detail['name']
                Home_Team=event_name.split(' vs ')[0]
                Away_Team=event_name.split(' vs ')[-1]
                event_date=event_detail['actual_start'][:10]

                market_group_list=event_detail['market_type_group_markets']
                for market_group in market_group_list:
                    
                    market_type_group_id=market_group['market_type_group_id']
                    market_type_group_name=market_groups[market_type_group_id]['name']
                    market_list=market_group['market_ids']
                    for market_id in market_list:
                        market_name=markets[market_id]['name']
                        entrant_ids=markets[market_id]['entrant_ids']
                        market_name2=market_name
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
                                line=float(markets[market_id]['handicap'])
                            except Exception:
                                try:
                                    line=float(re.search('(\+|\-)*\d+\.*\d*',market_name2).group())
                                    if line==int(line):
                                        line=line-0.5
                                        
                                except Exception:
                                    line=0


                            ####specifications for player Market
                            if market_type_group_name in player_mkt_mapping:
                                Player_Name=selection_name.replace(' ('+Home_Team+')','')
                                Player_Name=Player_Name.replace(' ('+Away_Team+')','')

                                selection_code=Player_Name

                                try:
                                    if re.search('Over|\+',market_name2):
                                        outcome='Over'
                                    elif re.search('Under',market_name2):
                                        outcome='Under'
                                except Exception:
                                    outcome=''

                                try:
                                    line=float(re.search('(\+|\-)*\d+\.*\d*',market_name2).group())
                                    if line==int(line):
                                        line=line-0.5
                                    
                                except Exception:
                                    line=0
                            
                            elif re.search('To Have \d+ Disposals',market_name2):
                                player_name=selection_name.replace(' ('+Home_Team+')','')
                                player_name=player_name.replace(' ('+Away_Team+')','')
                                selection_code=player_name
                                line=float(re.search('\d+',market_name2).group())-0.5
                                outcome='Over'
                                market_name='Player Disposals'

                            elif re.search('Total Disposals - ',market_name2):
                                player_name=market_name2.replace('Total Disposals - ','')
                                selection_code=player_name
                                line=float(re.search('(\+|\-)*\d+\.*\d*',selection_name).group())
                                outcome=re.search('Over|Under',selection_name).group()
                                market_name='Player Disposals'      


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
                            
                            link='https://www.neds.com.au/sports/australian-rules/afl/'+event_slug+'/'+event_id
                            link=link.replace(' ','%20')


                            #print([event_name,event_date,market_type_group_name,market_name,selection_name,odd])
                            row=['NEDS',event_name,event_date,Home_Team,Away_Team,market_name,selection_name,selection_code,outcome,line,odd,Over_odd,Under_odd,link]
                            output.append(row)
            except Exception:
                pass
        


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
        odds_df['Event_Name_std']=odds_df['Home_Team_std']+' v '+odds_df['Away_Team_std']
        odds_df=odds_df.drop(columns=['Home_Team_std','Away_Team_std'])
        market_mapping_df=self.read_sql_into_df(database,market_mapping_table)
        odds_df=pandas.merge(odds_df, market_mapping_df, on=['Bookie','Market_raw'], how='left')

        #map players
        odds_df=self.map_player_name(odds_df,database,engine,player_table='afl_dim_players',player_exception_table='afl_player_name_manual_mapping')

        #rearrange order
        odds_df=odds_df.reindex(columns=['Bookie','Event_Name_std', 'Event_Date', 'Match_Component', 'Selection_code_std', 'KPI_Name','Outcome', 'Line','Odds', 'Over_odd', 'Under_odd', 'Event_Name_raw', 'Home_Team', 'Away_Team','Market_raw', 'Selection_raw', 'Selection_code','Home_Abb', 'Away_Abb','Link'])

        return odds_df

    def get_afl_pb_odds_df(self,database,engine,team_mapping_table='afl_dim_teams',market_mapping_table='afl_dim_markets'):
        output=[]
        bookie='PB'

        games=requests.get('https://api.pointsbet.com/api/v2/competitions/7523/events/featured?includeLive=true').json()['events']
        margin_line_list=['1st Half Line','2nd Half Line','1st Quarter Line','2nd Quarter Line','3rd Quarter Line','4th Quarter Line']

        for game in games:
            gameid=game['key']
            event_name=game['name']
            Home_Team=game['homeTeam']
            Away_Team=game['awayTeam']
            event_date=game['startsAt'][:10]
            link='https://pointsbet.com.au/aussie-rules/AFL/'+gameid

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
                        line=float(re.search('(\+|\-)*\d+\.*\d*',selection_mod).group())
                        if line==int(line):
                            line=line-0.5
                    except Exception:
                        line=None
                    
                    #player_name
                    selection_code2=re.split(' Over | Under | To Get ',selection_code)[0]
                    selection_code3=selection_code



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



        df = pandas.DataFrame(output,columns =['Bookie','Event_Name_raw','Event_Date','Home_Team','Away_Team','Market_raw','Selection_raw','Selection_code','Outcome','Line','Odds','Over_odd','Under_odd','Link'])

                

        #get standard event_name
        odds_df=df
        team_df=self.read_sql_into_df(database,team_mapping_table)[['TEAM_NAME','TEAM_NAME_PB','TEAM_ABBREVIATION']]
        team_df.columns=['Home_Team_std','Home_Team','Home_Abb']
        odds_df=pandas.merge(odds_df, team_df, on=['Home_Team'], how='left',suffixes=('','_h'))
        team_df.columns=['Away_Team_std','Away_Team','Away_Abb']
        odds_df=pandas.merge(odds_df, team_df, on=['Away_Team'], how='left',suffixes=('','_a'))
        #odds_df=odds_df.drop(columns=['Home_Team_std_h','Away_Team_std_a'])
        odds_df['Event_Name_std']=odds_df['Home_Team_std']+' v '+odds_df['Away_Team_std']
        odds_df=odds_df.drop(columns=['Home_Team_std','Away_Team_std'])
        market_mapping_df=self.read_sql_into_df(database,market_mapping_table)
        odds_df=pandas.merge(odds_df, market_mapping_df, on=['Bookie','Market_raw'], how='left')

        #map players
        odds_df=self.map_player_name(odds_df,database,engine,player_table='afl_dim_players',player_exception_table='afl_player_name_manual_mapping')

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
                conso_df['SB']=0
            if 'Tab' not in conso_df:
                conso_df['Tab']=0
            if 'BE' not in conso_df:
                conso_df['BE']=0
            if 'NEDS' not in conso_df:
                conso_df['NEDS']=0
            if 'PB' not in conso_df:
                conso_df['PB']=0
            
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


        
        analysis_df['line_gap']=analysis_df['Line_u']-analysis_df['Line_o']
        #analysis_df['line_gap_pctg']=2*analysis_df['line_gap']/(analysis_df['Line_u']+analysis_df['Line_o'])
        analysis_df['line_gap_pctg']=np.where((analysis_df['Odds_o']>=1.5)&(analysis_df['Odds_u']>=1.5),analysis_df['line_gap']/(np.where(analysis_df['KPI_Name']=='Disposals',30,np.where(analysis_df['KPI_Name']=='Total_Points_Line',140,40))),0)
        analysis_df['line_gap_pctg']=analysis_df['line_gap_pctg'].abs()
        analysis_df['cost_index']=(1/analysis_df['Odds_o']+1/analysis_df['Odds_u'])-1
        
        #analysis_df['middle_return_kpi']=(analysis_df['line_gap_pctg']/analysis_df['cost_index'])/((analysis_df['Odds_u']-analysis_df['Odds_o']).abs()+1)

        filter=np.where(analysis_df['line_gap']>=1,True,False)
        #analysis_df=analysis_df[filter]

        arber_filter=np.where((analysis_df['cost_index']<0)|((analysis_df['cost_index']<=arber_threshold)&(analysis_df['line_gap']>0)),True,False)
        analysis_df=analysis_df[arber_filter]

        


        
        analysis_df['Over_Bookie']=np.where(analysis_df['Odds_o']==analysis_df['BE_o'],'BE',np.where(analysis_df['Odds_o']==analysis_df['SB_o'],'SB',np.where(analysis_df['Odds_o']==analysis_df['Tab_o'],'Tab',np.where(analysis_df['Odds_o']==analysis_df['NEDS_o'],'NEDS',np.where(analysis_df['Odds_o']==analysis_df['PB_o'],'PB','')))))
        analysis_df['Over_stake']=100*analysis_df['Odds_u']/(analysis_df['Odds_u']+analysis_df['Odds_o'])
        analysis_df['Under_Bookie']=np.where(analysis_df['Odds_u']==analysis_df['BE_u'],'BE',np.where(analysis_df['Odds_u']==analysis_df['SB_u'],'SB',np.where(analysis_df['Odds_u']==analysis_df['Tab_u'],'Tab',np.where(analysis_df['Odds_u']==analysis_df['NEDS_u'],'NEDS',np.where(analysis_df['Odds_u']==analysis_df['PB_u'],'PB','')))))
        analysis_df['Under_stake']=100*analysis_df['Odds_o']/(analysis_df['Odds_u']+analysis_df['Odds_o'])
        analysis_df['Margin']=analysis_df['Over_stake']*analysis_df['Odds_o']-100
        
        #1.1 plus calculated middle kpi
        analysis_df['middle_return_kpi']=0.5+analysis_df['line_gap_pctg']*(analysis_df['Over_stake']*analysis_df['Odds_o']+analysis_df['Under_stake']*analysis_df['Odds_u']-(analysis_df['Over_stake']+analysis_df['Under_stake']))/(-analysis_df['Margin'])

        analysis_df=analysis_df.merge(url_lookup, left_on=['Over_Bookie','Event_Name_std', 'Match_Component', 'Selection_code_std', 'KPI_Name'], right_on=['Bookie','Event_Name_std', 'Match_Component', 'Selection_code_std', 'KPI_Name'], suffixes=('', '_o'))
        analysis_df=analysis_df.merge(url_lookup, left_on=['Under_Bookie','Event_Name_std', 'Match_Component', 'Selection_code_std', 'KPI_Name'], right_on=['Bookie','Event_Name_std', 'Match_Component', 'Selection_code_std', 'KPI_Name'], suffixes=('', '_u'))

        analysis_df=analysis_df.sort_values(by='cost_index', ascending=True)

        
        return analysis_df

    def get_consolidated_odds(self,database,engine,arber_threshold=0,team_mapping_table='afl_dim_teams',market_mapping_table='afl_dim_markets',output_name='afl_all_bookies_cons'):
        
        df_list=[]


        try:
            tab=self.get_afl_tab_odds_df(database,engine,team_mapping_table='afl_dim_teams',market_mapping_table='afl_dim_markets')
            df_list.append(tab)
        except Exception:
            pass

        try:
            pb=self.get_afl_pb_odds_df(database,engine,team_mapping_table='afl_dim_teams',market_mapping_table='afl_dim_markets')
            df_list.append(pb)
        except Exception:
            pass

        try:
            sb=self.get_afl_sb_odds_df(database,engine,team_mapping_table='afl_dim_teams',market_mapping_table='afl_dim_markets')
            df_list.append(sb)
        except Exception:
            pass

        try:
            be=self.get_afl_be_odds_df(database,engine,team_mapping_table='afl_dim_teams',market_mapping_table='afl_dim_markets')
            df_list.append(be)
        except Exception:
            pass
        
        try:
            neds=self.get_afl_neds_odds_df(database,engine,team_mapping_table='afl_dim_teams',market_mapping_table='afl_dim_markets')
            df_list.append(neds)
        except Exception:
            pass


        
        
        

        consolidated_odds_all_bookies_df = pandas.concat(df_list, axis=0)

        #consolidated_odds_all_bookies_df.to_sql(output_name,engine,chunksize=50,method='multi',if_exists='replace',index=False)

        #consolidated_odds_all_bookies_df['Selection_code_std']=''


        return consolidated_odds_all_bookies_df
    
    def get_arber_notification_header_body(self,arber_notifying_df):
        
        to_notify_df=arber_notifying_df

        to_notify_df['Over_stake_str']=(to_notify_df['Over_stake']).map('{:,.2f}'.format)
        to_notify_df['Under_stake_str']=(to_notify_df['Under_stake']).map('{:,.2f}'.format)
        to_notify_df['Line_o_str']=(to_notify_df['Line_o']).map('{:,.1f}'.format)
        to_notify_df['Line_o_str2']=(-to_notify_df['Line_o']).map('{:,.1f}'.format)
        to_notify_df['Line_u_str']=(to_notify_df['Line_u']).map('{:,.1f}'.format)
        to_notify_df['Odds_o_str']=(to_notify_df['Odds_o']).map('{:,.2f}'.format)
        to_notify_df['Odds_u_str']=(to_notify_df['Odds_u']).map('{:,.2f}'.format)
        to_notify_df['Margin_str']=(to_notify_df['Margin']).map('{:,.2f}'.format)
        to_notify_df['Over_Bookie']=(to_notify_df['Over_Bookie']).map({'SB': 'SportsBet', 'BE': 'BetEasy','Tab':'Tab','NEDS':'NEDS','PB':'PointsBet'})
        to_notify_df['Under_Bookie']=(to_notify_df['Under_Bookie']).map({'SB': 'SportsBet', 'BE': 'BetEasy','Tab':'Tab','NEDS':'NEDS','PB':'PointsBet'})


        to_notify_df['SB_o_str']=(to_notify_df['SB_o']).map('{:,.2f}'.format)
        to_notify_df['Tab_o_str']=(to_notify_df['Tab_o']).map('{:,.2f}'.format)
        to_notify_df['NEDS_o_str']=(to_notify_df['NEDS_o']).map('{:,.2f}'.format)
        to_notify_df['PB_o_str']=(to_notify_df['PB_o']).map('{:,.2f}'.format)
        to_notify_df['BE_o_str']=(to_notify_df['BE_o']).map('{:,.2f}'.format)

        to_notify_df['SB_u_str']=(to_notify_df['SB_u']).map('{:,.2f}'.format)
        to_notify_df['Tab_u_str']=(to_notify_df['Tab_u']).map('{:,.2f}'.format)
        to_notify_df['NEDS_u_str']=(to_notify_df['NEDS_u']).map('{:,.2f}'.format)
        to_notify_df['PB_u_str']=(to_notify_df['PB_u']).map('{:,.2f}'.format)
        to_notify_df['BE_u_str']=(to_notify_df['BE_u']).map('{:,.2f}'.format)
        to_notify_df['home']=to_notify_df['Event_Name_std'].str.split(" v ", n = 1, expand = True)[0]
        to_notify_df['away']=to_notify_df['Event_Name_std'].str.split(" v ", n = 1, expand = True)[1]

        to_notify_df['odds_list']='Bookies  Over/Under'+'\nSB:       '+to_notify_df['SB_o_str']+'/'+to_notify_df['SB_u_str']+'\nTab:      '+to_notify_df['Tab_o_str']+'/'+to_notify_df['Tab_u_str']+'\nBE:       '+to_notify_df['BE_o_str']+'/'+to_notify_df['BE_u_str']+'\nPB:       '+to_notify_df['PB_o_str']+'/'+to_notify_df['PB_u_str']+'\nNEDS:     '+to_notify_df['NEDS_o_str']+'/'+to_notify_df['NEDS_u_str']
        
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

        to_notify_df['rating'] = to_notify_df.apply(star_ratings, axis = 1)
        to_notify_df['recommendation']=to_notify_df.apply(get_recom_string, axis = 1)


        body=to_notify_df['recommendation'].str.cat(sep='\n\n')


        header='Risk Free Betting!!! (DO NOT REPLY)'

        header_body_list=[header,body]
        
        return header_body_list

    def get_middle_notification_header_body(self,middle_notifying_df):
            
        to_notify_df=middle_notifying_df
        to_notify_df['middle_return']=to_notify_df['Over_stake']*to_notify_df['Odds_o']+to_notify_df['Under_stake']*to_notify_df['Odds_u']-(to_notify_df['Over_stake']+to_notify_df['Under_stake'])


        to_notify_df['Over_stake_str']=(to_notify_df['Over_stake']).map('{:,.2f}'.format)
        to_notify_df['Under_stake_str']=(to_notify_df['Under_stake']).map('{:,.2f}'.format)
        to_notify_df['Line_o_str']=(to_notify_df['Line_o']).map('{:,.1f}'.format)
        to_notify_df['Line_o_str2']=(-to_notify_df['Line_o']).map('{:,.1f}'.format)
        to_notify_df['Line_u_str']=(to_notify_df['Line_u']).map('{:,.1f}'.format)
        to_notify_df['Odds_o_str']=(to_notify_df['Odds_o']).map('{:,.2f}'.format)
        to_notify_df['Odds_u_str']=(to_notify_df['Odds_u']).map('{:,.2f}'.format)
        to_notify_df['Margin_str']=(to_notify_df['Margin']).map('{:,.2f}'.format)
        to_notify_df['middle_return_str']=(to_notify_df['middle_return']).map('{:,.2f}'.format)
        to_notify_df['Over_Bookie']=(to_notify_df['Over_Bookie']).map({'SB': 'SportsBet', 'BE': 'BetEasy','Tab':'Tab','NEDS':'NEDS','PB':'PointsBet'})
        to_notify_df['Under_Bookie']=(to_notify_df['Under_Bookie']).map({'SB': 'SportsBet', 'BE': 'BetEasy','Tab':'Tab','NEDS':'NEDS','PB':'PointsBet'})
        
        to_notify_df['SB_o_str']=(to_notify_df['SB_o']).map('{:,.2f}'.format)
        to_notify_df['Tab_o_str']=(to_notify_df['Tab_o']).map('{:,.2f}'.format)
        to_notify_df['NEDS_o_str']=(to_notify_df['NEDS_o']).map('{:,.2f}'.format)
        to_notify_df['PB_o_str']=(to_notify_df['PB_o']).map('{:,.2f}'.format)
        to_notify_df['BE_o_str']=(to_notify_df['BE_o']).map('{:,.2f}'.format)

        to_notify_df['SB_u_str']=(to_notify_df['SB_u']).map('{:,.2f}'.format)
        to_notify_df['Tab_u_str']=(to_notify_df['Tab_u']).map('{:,.2f}'.format)
        to_notify_df['NEDS_u_str']=(to_notify_df['NEDS_u']).map('{:,.2f}'.format)
        to_notify_df['PB_u_str']=(to_notify_df['PB_u']).map('{:,.2f}'.format)
        to_notify_df['BE_u_str']=(to_notify_df['BE_u']).map('{:,.2f}'.format)

        to_notify_df['home']=to_notify_df['Event_Name_std'].str.split(" v ", n = 1, expand = True)[0]
        to_notify_df['away']=to_notify_df['Event_Name_std'].str.split(" v ", n = 1, expand = True)[1]


        to_notify_df['odds_list']='Bookies  Over/Under'+'\nSB:       '+to_notify_df['SB_o_str']+'/'+to_notify_df['SB_u_str']+'\nTab:      '+to_notify_df['Tab_o_str']+'/'+to_notify_df['Tab_u_str']+'\nBE:       '+to_notify_df['BE_o_str']+'/'+to_notify_df['BE_u_str']+'\nPB:       '+to_notify_df['PB_o_str']+'/'+to_notify_df['PB_u_str']+'\nNEDS:     '+to_notify_df['NEDS_o_str']+'/'+to_notify_df['NEDS_u_str']
  

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


        to_notify_df['rating'] = to_notify_df.apply(star_ratings, axis = 1)


        to_notify_df['recommendation']=to_notify_df.apply(get_recom_string, axis = 1)    

        body=to_notify_df['recommendation'].str.cat(sep='\n\n')


        header='Betting on middle! (DO NOT REPLY)'

        header_body_list=[header,body]
        
        return header_body_list
     
    def get_notifying_dfv2(self,database,engine,compare_odds_df,sql_table_name='afl_arber_recommendation_log',arber_threshold=-0.05,middle_threshold=4):
        filter=np.where((compare_odds_df['middle_return_kpi']==0) & (compare_odds_df['cost_index']<=arber_threshold),True,False)
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
        
    def send_email_v2(self,header_body_list):
        #this function will send email on any additional item, and then update the sql table at the back
        distribution_list=self.get_email_distribution_list(workbook_name='NBA_email_subscribers',sheet_name='register')
        e_text=header_body_list[1]
        e_header=header_body_list[0]
        if e_text=='':
            pass
            print('no new info to be sent via email:---'+e_header)
        else:
            rz_send_email(user_name='nba.odds.analysis@gmail.com',password='Tianxiaomiao6',smtp_server='smtp.gmail.com',port='465',email_text=e_text,subject=e_header,distribution_list=distribution_list)
            print('update been sent')

    def telegram_bot_sendtext(self,chat_id='-1001362763351',bot_message='hello',bot_token='1004217287:AAFRu5CD-3s70TgnPLbdKqIkKoqWWywkrco'):     
        msgs=bot_message.split('\n\n\n\n')
        for msg in msgs:
            params = {"chat_id": str(chat_id),"text": msg,"parse_mode": "HTML"}
            requests.get("https://api.telegram.org/bot{}/sendMessage".format(bot_token),params=params)

    def map_player_name(self,input_odds_df,database,engine,player_table='afl_dim_players',player_exception_table='afl_player_name_manual_mapping'):
    

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
                given_name=name_str_list[0][:1]
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
        unmapped_player_df['Player_name_raw']=unmapped_player_df['Selection_code']
        unmapped_player_df=unmapped_player_df[['Bookie','Home_Abb','Away_Abb','Player_name_raw']]
        player_excp_df=player_excp_df.append(unmapped_player_df,sort=False)
        player_excp_df=player_excp_df.groupby(['Bookie','Player_name_raw']).agg({'Bookie':'first','Player_name_raw':'first','Player_name_std':'first'}).reset_index(drop=True)
        player_excp_df.to_sql(player_exception_table,engine,chunksize=50,method='multi',if_exists='replace',index=False)

        #return output
        return output_odds_df
    
    def get_mins(self,MIN_string):
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

    def refresh_tableau(self):
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

    def nba_refresh_all(self,database,engine,distribution_list=[['Ronny ', 'zitian1211@gmail.com']]):
   

        self.backup_tables(database,engine)
        self.get_nba_stats(database,engine)
        self.get_nba_sportsbet_odds(database,engine)
        self.analyse_odds(database,engine)
        self.prepare_commentary(database,engine)
        self.send_email(database,engine,sql_table_name='nba_recommendation_log',threshold=1.5,distribution_list=distribution_list)
        self.backup_tables(database,engine)
        self.refresh_tableau()

        print('completed')

    def bookies_wrapper(self,database,engine,spread):
        try:
            dw=database
            consolidated_odds_all_bookies_df = self.get_consolidated_odds(dw,engine)
            arber_analysis_df=self.get_compare_odds_df(dw,engine,consolidated_odds_all_bookies_df,arber_threshold=0.1)
            try:
                spread.df_to_sheet(arber_analysis_df, index=False, sheet='afl arber analysis', start='A1', replace=True)
            except Exception:
                print('arber analysis into googlesheet failed')
            
            notify_list=self.get_notifying_dfv2(dw,engine,arber_analysis_df,sql_table_name='afl_arber_recommendation_log',arber_threshold=-0.05,middle_threshold=2)##-0.05, 2
            
            get_arber_notifying_df=notify_list[0]
            get_middle_notifying_df=notify_list[1]

            if get_arber_notifying_df.shape[0]>0:
            
                email_body_arb=self.get_arber_notification_header_body(get_arber_notifying_df)
                try:                    
                    self.telegram_bot_sendtext(chat_id='-1001362763351',bot_message=email_body_arb[1],bot_token='1004217287:AAFRu5CD-3s70TgnPLbdKqIkKoqWWywkrco')

                    #self.telegram_bot_sendtext(chat_id='1029752482',bot_message=email_body_arb[1],bot_token='1004217287:AAFRu5CD-3s70TgnPLbdKqIkKoqWWywkrco')
                    self.send_email_v2(email_body_arb)
                    print('arber update sent')

                except Exception:
                    print('telegram arber update failed')   

            if get_middle_notifying_df.shape[0]>0:
                
                email_body_mid=self.get_middle_notification_header_body(get_middle_notifying_df)
                try:                    

                    self.telegram_bot_sendtext(chat_id='-1001362763351',bot_message=email_body_mid[1],bot_token='1004217287:AAFRu5CD-3s70TgnPLbdKqIkKoqWWywkrco')

                    #self.telegram_bot_sendtext(chat_id='1029752482',bot_message=email_body_mid[1],bot_token='1004217287:AAFRu5CD-3s70TgnPLbdKqIkKoqWWywkrco')
                    self.send_email_v2(email_body_mid)
                    print('middle update sent')

                except Exception:
                    print('telegram middle update failed')       





            now = datetime.datetime.now()
            current_time = now.strftime("%H:%M:%S")
            
            print('AFL arber analysis refreshed refreshed @'+current_time)

        except Exception:
            print('AFL bookies analysis failed')


def main():
    #daily_bs_into_sql()
    #schedule.every().day.at("20:00").do(daily_bs_into_sql)
    #schedule.every().day.at("05:00").do(daily_bs_into_sql)
    #schedule.every().day.at("13:30").do(daily_bs_into_sql)
    #while True:
    #    schedule.run_pending()
    #    time.sleep(1)

    afl=AFL()
    dw=pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-UJKV8TQ\DEV1;DATABASE=datawarehouse;Trusted_Connection=yes;')
    engine = sqlalchemy.create_engine("mssql+pyodbc://sa:Tianxiaomiao6!@DESKTOP-UJKV8TQ\DEV1/datawarehouse?driver=SQL+Server")

    #dw=pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=127.0.0.1;DATABASE=datawarehouse;UID=sqlserver;PWD=Tianxiaomiao6!')
    #engine = sqlalchemy.create_engine("mssql+pyodbc://sqlserver:Tianxiaomiao6!@127.0.0.1/datawarehouse?driver=SQL+Server")

    engine.connect()
    spread=gspread_pandas.Spread('NBA_ANALYSIS')
    #distribution_list=[['Ronny ', 'zitian1211@gmail.com'],['Jason','wance605@gmail.com'],['Lee','dogginoz@hotmail.com'],['老婆','tianwenqinggd@gmail.com'],['赵星宇','zhaoxingyu331@163.com']]

    #afl.prepare_commentary(dw,engine)

    def scheduler():
        afl.bookies_wrapper(database=dw,engine=engine)

   
    schedule.every(5).minutes.do(scheduler)



   
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()

