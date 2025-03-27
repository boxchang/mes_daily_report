from database import vnedc_database, mes_database
import pandas as pd
from datetime import datetime, timedelta

class Utils(object):
    week_range = 15

    def get_week_date_df(self):
        vnedc_db = vnedc_database()
        date1 = datetime.now()
        date2 = (date1 - timedelta(days=7)).strftime('%Y-%m-%d')
        date1 = date1.strftime('%Y-%m-%d')

        sql = f"""
            SELECT TOP ({self.week_range}) *
              FROM [MES_OLAP].[dbo].[week_date]
              WHERE CONVERT(DATETIME, '{date1}', 121) > end_date and CONVERT(DATETIME, '{date2}', 121) <= end_date
              AND enable = 1
              ORDER BY year desc, month desc, month_week
        """

        print(sql)
        raws = vnedc_db.select_sql_dict(sql)
        df = pd.DataFrame(raws)
        return df

    def get_week_date_dist(self):
        result = None
        vnedc_db = vnedc_database()
        date1 = datetime.now()
        date2 = (date1 - timedelta(days=7)).strftime('%Y-%m-%d')
        date1 = date1.strftime('%Y-%m-%d')

        sql = f"""
            SELECT TOP ({self.week_range}) *
              FROM [MES_OLAP].[dbo].[week_date]
              WHERE CONVERT(DATETIME, '{date1}', 121) > end_date and CONVERT(DATETIME, '{date2}', 121) <= end_date
              AND enable = 1
              ORDER BY year desc, month desc, month_week
        """

        print(sql)
        raws = vnedc_db.select_sql_dict(sql)

        if len(raws) > 0:
            result = raws[0]

        return result

    def generate_previous_weeks_with_dates(self, data_date):
        vnedc_db = vnedc_database()
        weeks_to_generate = 15

        weeks_list = []
        week_dates = []

        sql = f"""
                    SELECT TOP ({weeks_to_generate}) *
                      FROM [MES_OLAP].[dbo].[week_date]
                      WHERE start_date < '{data_date}' AND enable = 1
                      ORDER BY [year] desc, month desc, week_no desc
                """
        raws = vnedc_db.select_sql_dict(sql)

        for data in raws:
            week = data['week_no']
            week_start = data['start_date']
            week_end = data['end_date']
            weeks_list.append(f"{week}")
            week_dates.append([datetime.strptime(week_start, "%Y-%m-%d").date(), datetime.strptime(week_end, "%Y-%m-%d").date()])

        weeks_list.reverse()
        week_dates.reverse()

        return weeks_list, week_dates

    def chart_y_label(self, max_data, step_data):
        yticks_positions = []
        yticks_labels = []
        # step_data = 10
        rounded_max_data = int(
            (((max_data / (10 ** (len(str(max_data)) - 2))) // step_data) * step_data + step_data) * (
                    10 ** (len(str(max_data)) - 2)))
        rounded_step_data = step_data * (10 ** (len(str(max_data)) - 2))

        if len(str(max_data)) >= 7:
            yticks_positions = list(range(0, rounded_max_data + 1 * rounded_step_data, rounded_step_data))
            yticks_positions.append(int(rounded_max_data + 2 * rounded_step_data))
            yticks_labels = [
                f"{int(i//(10**(len(str(max_data)) - 1)))}" + '百萬' if len(str(i)) > 6 else f"{i}" for i
                in yticks_positions]

        elif 4 < len(str(max_data)) < 7:
            yticks_positions = list(range(0, rounded_max_data, rounded_step_data))
            yticks_positions.append(int(rounded_max_data + 3 * rounded_step_data))
            # yticks_labels = [f"{int(i//(10**(len(str(max_data)) - 3)))}" + '萬 PCS' if len(str(i)) > 4 and int(
            #     i // (10 ** (len(str(max_data)) - 3))) % 60 == 0 else "" for i in yticks_positions]
            yticks_labels = [f"{int(i/10000)} 萬" if i > 0 else 0 for i in yticks_positions]

        return yticks_positions, yticks_labels
