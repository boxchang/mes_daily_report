import configparser
import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from lib.utils import Utils
import pandas as pd
from datetime import datetime, timedelta
from database import mes_database, mes_olap_database, lkmes_database, lkmes_olap_database, lkedc_database, \
    vnedc_database


class MES_IPQC_SUMMARY(object):

    def __init__(self, start_date, end_date):
        config_file = "..\mes_daily_report.config"
        config = configparser.ConfigParser()
        config.read(config_file, encoding="utf-8")
        self.location = config.get("Settings", "location", fallback=None)

        if self.location in "GD":
            self.mes_db = mes_database()
            self.mes_olap_db = mes_olap_database()
            self.vnedc_db = vnedc_database()
        elif self.location in "LK":
            self.mes_db = lkmes_database()
            self.mes_olap_db = lkmes_olap_database()
            self.vnedc_db = lkedc_database()
        else:
            self.mes_db = None
            self.mes_olap_db = None
            self.vnedc_db = None

        year, week_no = Utils().get_week_data_df(self.vnedc_db, start_date)
        self.year = year
        self.week_no = week_no
        self.start_date = start_date
        self.end_date = end_date

    def main(self):
        sql = f"""
                    SELECT ipqc.RunCardId Runcard, ipqc.OptionName, ipqc.InspectionStatus, ipqc.InspectionValue, ipqc.Lower_InspectionValue, ipqc.Upper_InspectionValue, ipqc.DefectCode
                      FROM [PMGMES].[dbo].[PMG_MES_IPQCInspectingRecord] ipqc
                      JOIN [PMGMES].[dbo].[PMG_MES_RunCard] r on ipqc.RunCardId = r.Id
                      WHERE ((r.InspectionDate = '{self.start_date}' AND r.Period BETWEEN 6 AND 23)
                            OR (r.InspectionDate = '{self.end_date}' AND r.Period BETWEEN 0 AND 5)) 
                """
        ipqc_rows = self.mes_db.select_sql_dict(sql)
        data_df = pd.DataFrame(ipqc_rows)

        distinct_runcards = data_df['Runcard'].unique()
        runcard_list = "(" + ", ".join(f"'{rc}'" for rc in distinct_runcards) + ")"

        self.ipqc_data_delete(runcard_list)
        self.ipqc_data_insert(data_df)


    def ipqc_data_delete(self, runcard_list):

        sql = f"""
                DELETE FROM [MES_OLAP].[dbo].[mes_ipqc_data] WHERE 
                Runcard in {runcard_list}
                """
        self.mes_olap_db.execute_sql(sql)


    def ipqc_data_insert(self, data_df):
        year = self.year
        week_no = self.week_no

        distinct_runcard_df = data_df[['Runcard']].drop_duplicates().reset_index(drop=True)

        OptionName = ['Weight', 'Width', 'Length', 'Tensile', 'Elongation', 'Roll', 'Cuff', 'Palm', 'Finger', 'FingerTip', 'Pinhole']

        results = []

        for index, row in distinct_runcard_df.iterrows():

            runcard = row["Runcard"]

            ipqc = {}
            for option in OptionName:

                value = None
                status = None
                lower = None
                upper = None
                defect = None
                ipqc_option_df = data_df[data_df["OptionName"] == option]
                runcard_ipqc_value_df = ipqc_option_df[ipqc_option_df["Runcard"] == runcard]

                if not runcard_ipqc_value_df.empty:
                    value = float(runcard_ipqc_value_df.iloc[0]["InspectionValue"]) if runcard_ipqc_value_df.iloc[0]["InspectionValue"] != None else None
                    status = runcard_ipqc_value_df.iloc[0]["InspectionStatus"]
                    lower = float(runcard_ipqc_value_df.iloc[0]["Lower_InspectionValue"]) if runcard_ipqc_value_df.iloc[0]["Lower_InspectionValue"] else None
                    upper = float(runcard_ipqc_value_df.iloc[0]["Upper_InspectionValue"]) if runcard_ipqc_value_df.iloc[0]["Upper_InspectionValue"] else None
                    defect = runcard_ipqc_value_df.iloc[0]["DefectCode"]
                ipqc[option] = {'value': value, 'status': status, 'lower': lower, 'upper': upper, 'defect': defect}

            tensile_status = '' if ipqc['Tensile']['status'] == None else ipqc['Tensile']['status']
            tensile_value = 0 if ipqc['Tensile']['status'] == None else ipqc['Tensile']['value']
            tensile_limit = '' if ipqc['Tensile']['status'] == None else str(ipqc['Tensile']['lower']) + " ~ " + str(ipqc['Tensile']['upper'])
            tensile_defect = '' if ipqc['Tensile']['defect'] == None else ipqc['Tensile']['defect']

            elongation_value = 0 if ipqc['Elongation']['status'] == None else ipqc['Elongation']['value']
            elongation_limit = '' if ipqc['Elongation']['status'] == None else str(ipqc['Elongation']['lower']) + " ~ " + str(ipqc['Tensile']['upper'])
            elongation_status = '' if ipqc['Elongation']['status'] == None else ipqc['Elongation']['status']
            elongation_defect = '' if ipqc['Elongation']['defect'] == None else ipqc['Elongation']['defect']

            roll_value = 0 if ipqc['Roll']['status'] == None else ipqc['Roll']['value']
            roll_limit = '' if ipqc['Roll']['status'] == None else str(ipqc['Roll']['lower']) + " ~ " + str(ipqc['Roll']['upper'])
            roll_status = '' if ipqc['Roll']['status'] == None else ipqc['Roll']['status']
            roll_defect = '' if ipqc['Roll']['defect'] == None else ipqc['Roll']['defect']

            cuff_value = 0 if ipqc['Cuff']['status'] == None else ipqc['Cuff']['value']
            cuff_limit = '' if ipqc['Cuff']['status'] == None else str(ipqc['Cuff']['lower']) + " ~ " + str(ipqc['Cuff']['upper'])
            cuff_status = '' if ipqc['Cuff']['status'] == None else ipqc['Cuff']['status']
            cuff_defect = '' if ipqc['Cuff']['defect'] == None else ipqc['Cuff']['defect']

            palm_value = 0 if ipqc['Palm']['status'] == None else ipqc['Palm']['value']
            palm_limit = '' if ipqc['Palm']['status'] == None else str(ipqc['Palm']['lower']) + " ~ " + str(ipqc['Palm']['upper'])
            palm_status = '' if ipqc['Palm']['status'] == None else ipqc['Palm']['status']
            palm_defect = '' if ipqc['Palm']['defect'] == None else ipqc['Palm']['defect']

            finger_value = 0 if ipqc['Finger']['status'] == None else ipqc['Finger']['value']
            finger_limit = '' if ipqc['Finger']['status'] == None else str(ipqc['Finger']['lower']) + " ~ " + str(ipqc['Finger']['upper'])
            finger_status = '' if ipqc['Finger']['status'] == None else ipqc['Finger']['status']
            finger_defect = '' if ipqc['Finger']['defect'] == None else ipqc['Finger']['defect']

            fingerTip_value = 0 if ipqc['FingerTip']['status'] == None else ipqc['FingerTip']['value']
            fingerTip_limit = '' if ipqc['FingerTip']['status'] == None else str(ipqc['FingerTip']['lower']) + " ~ " + str(ipqc['FingerTip']['upper'])
            fingerTip_status = '' if ipqc['FingerTip']['status'] == None else ipqc['FingerTip']['status']
            fingerTip_defect = '' if ipqc['FingerTip']['defect'] == None else ipqc['FingerTip']['defect']

            weight_value = 0 if ipqc['Weight']['status'] == None else ipqc['Weight']['value']
            weight_limit = '' if ipqc['Weight']['status'] == None else str(ipqc['Weight']['lower']) + " ~ " + str(ipqc['Weight']['upper'])
            weight_status = '' if ipqc['Weight']['status'] == None else ipqc['Weight']['status']
            weight_defect = '' if ipqc['Weight']['defect'] == None else ipqc['Weight']['defect']

            length_value = 0 if ipqc['Length']['status'] == None else ipqc['Length']['value']
            length_limit = '' if ipqc['Length']['status'] == None else str(ipqc['Length']['lower']) + " ~ " + str(ipqc['Length']['upper'])
            length_status = '' if ipqc['Length']['status'] == None else ipqc['Length']['status']
            length_defect = '' if ipqc['Length']['defect'] == None else ipqc['Length']['defect']

            width_value = 0 if ipqc['Width']['status'] == None else ipqc['Width']['value']
            width_limit = '' if ipqc['Width']['status'] == None else str(ipqc['Width']['lower']) + " ~ " + str(ipqc['Width']['upper'])
            width_status = '' if ipqc['Width']['status'] == None else ipqc['Width']['status']
            width_defect = '' if ipqc['Width']['defect'] == None else ipqc['Width']['defect']

            pinhole_value = 0 if ipqc['Pinhole']['status'] == None else ipqc['Pinhole']['value']
            pinhole_limit = '' if ipqc['Pinhole']['status'] == None else str(ipqc['Pinhole']['lower']) + " ~ " + str(ipqc['Pinhole']['upper'])
            pinhole_status = '' if ipqc['Pinhole']['status'] == None else ipqc['Pinhole']['status']
            pinhole_defect = '' if ipqc['Pinhole']['defect'] == None else ipqc['Pinhole']['defect']
            cosmetic_value = ''
            cosmetic_status = ''

            sql = f"""
                        Insert into [MES_OLAP].[dbo].[mes_ipqc_data]([Year],Week_No,Runcard,
                        Tensile_Value,Tensile_Limit,Tensile_Status,Tensile_Defect,
                        Elongation_Value,Elongation_Limit,Elongation_Status,Elongation_Defect,
                        Roll_Value,Roll_Limit,Roll_Status,Roll_Defect,
                        Cuff_Value,Cuff_Limit,Cuff_Status,Cuff_Defect,
                        Palm_Value,Palm_Limit,Palm_Status,Palm_Defect,
                        Finger_Value,Finger_Limit,Finger_Status,Finger_Defect,
                        FingerTip_Value,FingerTip_Limit,FingerTip_Status,FingerTip_Defect,
                        Weight_Value,Weight_Limit,Weight_Status,Weight_Defect,
                        Length_Value,Length_Limit,Length_Status,Length_Defect,
                        Width_Value,Width_Limit,Width_Status,Width_Defect,
                        Pinhole_Value,Pinhole_Limit,Pinhole_Status,Pinhole_Defect, 
                        create_at)
                        Values({self.year},'{self.week_no}','{runcard}',
                        {tensile_value},'{tensile_limit}','{tensile_status}','{tensile_defect}',
                        {elongation_value},'{elongation_limit}','{elongation_status}','{elongation_defect}',
                        {roll_value},'{roll_limit}','{roll_status}','{roll_defect}',
                        {cuff_value},'{cuff_limit}','{cuff_status}','{cuff_defect}',
                        {palm_value},'{palm_limit}','{palm_status}','{palm_defect}',
                        {finger_value},'{finger_limit}','{finger_status}','{finger_defect}',
                        {fingerTip_value},'{fingerTip_limit}','{fingerTip_status}','{fingerTip_defect}',
                        {weight_value},'{weight_limit}','{weight_status}','{weight_defect}',
                        {length_value},'{length_limit}','{length_status}','{length_defect}',
                        {width_value},'{width_limit}','{width_status}','{width_defect}',
                        {pinhole_value},'{pinhole_limit}','{pinhole_status}', '{pinhole_defect}', 
                        GETDATE()
                        )
                    """
            print(sql)
            self.mes_olap_db.execute_sql(sql)


report_date1 = datetime.today() - timedelta(days=1)
report_date1 = report_date1.strftime('%Y%m%d')

report_date2 = datetime.today()
report_date2 = report_date2.strftime('%Y%m%d')

# report_date1 = "20250403"
# report_date2 = "20250404"

ipqc = MES_IPQC_SUMMARY(report_date1, report_date2)
ipqc.main()