import datetime
import io
import os
import time
from email.mime.text import MIMEText
import pandas as pd
import openpyxl
import string
import smtplib
import psycopg2
from UliPlot.XLSX import auto_adjust_xlsx_column_width
from psycopg2 import sql
from sqlalchemy import create_engine
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
import xlsxwriter
from django.conf import settings

load_dotenv()  # take environment variables from .env.


def dict_geo_log(dict_geo_notfound):
    if len(dict_geo_notfound) != 0:
        a = ', '.join(
            dict_geo_notfound) + ' месторождений нет в списке dict.geo.(Возможно присутствуют пробелы в названиях месторождений)'
        return a
    else:
        a = ''
        return a


def empty_value_log(empty_value):
    if len(empty_value) != 0:
        a = ' '.join(empty_value) + ' имеет пустую колонку.'
        return a
    else:
        a = ''
        return a


def dict_well_log(dict_well_notfound):
    if len(dict_well_notfound) != 0:
        a = ' '.join(dict_well_notfound) + ' скважины нет в списке dict.well.'
        return a
    else:
        a = ''
        return a


def main(mail, excel_file, excel_byte):
    excel_file = pd.ExcelFile(excel_file)
    df = pd.read_excel(excel_file, sheet_name=None)
    try:
        connection = psycopg2.connect(
            host=settings.DB_CONNECTION.get('HOST'),
            database=settings.DB_CONNECTION.get('DATABASE'),
            user=settings.DB_CONNECTION.get('USER'),
            password=settings.DB_CONNECTION.get('PASSWORD'),
            port=settings.DB_CONNECTION.get('PORT'),
        )
    except:
        return "Postgres connection exception occurred"

    cursor = connection.cursor()
    connection.autocommit = True
    try:
        engine = create_engine(settings.ENGINE.get('engine'))
        dict_geo = pd.read_sql('select * from dict.geo', engine)
    except:
        return "Postgres connection exception occurred"

    pref = pd.DataFrame(dict_geo[dict_geo['name_ru'].isin(list(excel_file.sheet_names))][
                            ['name_ru', 'field_code']]).reset_index(drop=True)
    dict_geo_notfound = []
    for i in list(excel_file.sheet_names):
        if i in pref['name_ru'].unique():
            df.get(i)['Скважина'] = df.get(i)['Скважина'].apply(
                lambda x: str(pref[pref['name_ru'] == i]['field_code'].values[0]) + '_' + (
                        (4 - len(str(x))) * '0' + str(x)))
        else:
            dict_geo_notfound.append(i)
            del df[i]

    new_df = pd.DataFrame(
        columns=['Скважина', 'Глубина, м', 'Абсолютная отметка, м', 'Зенитный угол, градус', 'Азимут, градус',
                 'Удлинение, м', 'Смещение, м', 'Координата юг-север, м', 'Координата запад-восток, м',
                 'Смещение север(+) - юг(-), м', 'Смещение восток(+) - запад(-), м', 'Дирекционный угол, градус',
                 'Вертикальная глубина, м', 'Интенсивнось искривления, гр/10м'])

    for sheetname, i in df.items():
        df.get(sheetname).rename(columns={
            f'{df.get(sheetname).columns[1]}': 'Глубина, м',
            f'{df.get(sheetname).columns[2]}': 'Абсолютная отметка, м',
            f'{df.get(sheetname).columns[3]}': 'Зенитный угол, градус',
            f'{df.get(sheetname).columns[4]}': 'Азимут, градус',
            f'{df.get(sheetname).columns[5]}': 'Удлинение, м',
            f'{df.get(sheetname).columns[6]}': 'Смещение, м',
            f'{df.get(sheetname).columns[7]}': 'Координата юг-север, м',
            f'{df.get(sheetname).columns[8]}': 'Координата запад-восток, м',
            f'{df.get(sheetname).columns[9]}': 'Смещение север(+) - юг(-), м',
            f'{df.get(sheetname).columns[10]}': 'Смещение восток(+) - запад(-), м',
            f'{df.get(sheetname).columns[11]}': 'Дирекционный угол, градус',
            f'{df.get(sheetname).columns[12]}': 'Вертикальная глубина, м',
            f'{df.get(sheetname).columns[13]}': 'Интенсивнось искривления, гр/10м',
        }, inplace=True)
        new_df = pd.concat([new_df, i])

    new_df = new_df.loc[:, 'Скважина': 'Интенсивнось искривления, гр/10м']
    try:
        MINIO_CLIENT = Minio(settings.MINIO_CONNECTION.get('HOST'),
                             access_key=settings.MINIO_CONNECTION.get('ACCESS KEY'),
                             secret_key=settings.MINIO_CONNECTION.get('SECRET KEY'), secure=False)
    except:
        return "MINIO exception occurred"

    empty_value = list(zip(
        new_df[['Скважина', 'Глубина, м', 'Зенитный угол, градус', 'Азимут, градус']][
            new_df[['Скважина', 'Глубина, м', 'Зенитный угол, градус', 'Азимут, градус']].isna().any(axis=1)][
            'Скважина'].values,
        new_df[['Скважина', 'Глубина, м', 'Зенитный угол, градус', 'Азимут, градус']][
            new_df[['Скважина', 'Глубина, м', 'Зенитный угол, градус', 'Азимут, градус']].isna().any(axis=1)][
            ['Скважина']].index.astype(str).to_list()))
    empty_value = [i[0] + ' с индексом ' + i[1] for i in empty_value]

    dict_well = pd.read_sql('select * from dict.well', engine)

    dict_well_notfound = list()
    for i in new_df['Скважина'].unique():
        if i not in dict_well['uwi'].unique():
            dict_well_notfound.append(i)

    intercept = dict_well[dict_well['uwi'].isin(new_df['Скважина'].unique())][['id', 'uwi']].reset_index(drop=True)
    intercept.rename({'id': 'well'}, inplace=True)

    a = (dict_geo_log(dict_geo_notfound), empty_value_log(empty_value), dict_well_log(dict_well_notfound))
    a = ' '.join(a)

    if len(a) == 2:
        filename = list()
        filesize = list()
        if type(excel_byte) == io.BytesIO:
            excel_byte.seek(0)
            file_size = excel_byte.getbuffer().nbytes
            file_data = excel_byte
        else:
            excel_byte.open('rb')
            file_size = excel_byte.size
            content = excel_byte.read(file_size)
            file_data = io.BytesIO(content)
        try:
            MINIO_CLIENT.put_object(bucket_name="inclinometer",
                                    object_name=f'Исходник_{datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S")}.xlsx',
                                    data=file_data, length=file_size)
        except:
            return 'MINIO exception occurred'

        for i in new_df['Скважина'].unique():
            buf = io.BytesIO()

            writer = pd.ExcelWriter(buf, engine="xlsxwriter")
            new_df1 = new_df[new_df['Скважина'] == i]
            new_df1.to_excel(writer, index=False)
            worksheet = writer.sheets['Sheet1']
            worksheet.set_column('A:A', 15)
            worksheet.set_column('B:B', 20)
            worksheet.set_column('C:C', 30)
            worksheet.set_column('D:D', 25)
            worksheet.set_column('E:E', 30)
            worksheet.set_column('F:F', 30)
            worksheet.set_column('G:G', 30)
            worksheet.set_column('H:H', 30)
            worksheet.set_column('I:I', 30)
            worksheet.set_column('J:J', 30)
            worksheet.set_column('K:K', 30)
            worksheet.set_column('L:L', 30)
            worksheet.set_column('M:M', 30)
            worksheet.set_column('N:N', 40)
            writer.save()
            buf.seek(0)

            MINIO_CLIENT.put_object(bucket_name="inclinometer",
                                    object_name=f'{i}.xlsx',
                                    data=buf, length=buf.getbuffer().nbytes)
            filename.append(time.time_ns())
            filesize.append(buf.getbuffer().nbytes)

        new_df.rename(columns={
            f'{new_df.columns[1]}': 'md',
            f'{new_df.columns[2]}': 'z',
            f'{new_df.columns[3]}': 'incl',
            f'{new_df.columns[4]}': 'azim',
            f'{new_df.columns[5]}': 'ext',
            f'{new_df.columns[6]}': 'thl',
            f'{new_df.columns[7]}': 'x',
            f'{new_df.columns[8]}': 'y',
            f'{new_df.columns[9]}': 'dx',
            f'{new_df.columns[10]}': 'dy',
            f'{new_df.columns[11]}': 'da',
            f'{new_df.columns[12]}': 'tvd',
            f'{new_df.columns[13]}': 'dls',
            'id_y': 'well_incl'
        }, inplace=True)
        new_df['md'] = new_df['md'].apply(lambda x: str(x).replace(',', '.'))
        new_df['z'] = new_df['z'].apply(lambda x: str(x).replace(',', '.'))
        new_df['incl'] = new_df['incl'].apply(lambda x: str(x).replace(',', '.'))
        new_df['azim'] = new_df['azim'].apply(lambda x: str(x).replace(',', '.'))
        new_df['ext'] = new_df['ext'].apply(lambda x: str(x).replace(',', '.'))
        new_df['thl'] = new_df['thl'].apply(lambda x: str(x).replace(',', '.'))
        new_df['x'] = new_df['x'].apply(lambda x: str(x).replace(',', '.'))
        new_df['y'] = new_df['y'].apply(lambda x: str(x).replace(',', '.'))
        new_df['dx'] = new_df['dx'].apply(lambda x: str(x).replace(',', '.'))
        new_df['dy'] = new_df['dy'].apply(lambda x: str(x).replace(',', '.'))
        new_df['da'] = new_df['da'].apply(lambda x: str(x).replace(',', '.'))
        new_df['tvd'] = new_df['tvd'].apply(lambda x: str(x).replace(',', '.'))
        new_df['tvd'] = new_df['tvd'].apply(lambda x: str(x).replace(',', '.'))
        new_df['dls'] = new_df['dls'].apply(lambda x: str(x).replace(',', '.'))
        new_df.replace({'nan': 'NULL'})

        test_well_incl = pd.read_sql(
            f"select id,well from test.well_incl where well in ({', '.join(str(x) for x in intercept['id'].values)})",
            engine)
        test_well_incl_data = pd.DataFrame(columns=['id', 'well_incl'])
        if len(test_well_incl) != 0:
            test_well_incl_data = pd.read_sql(
                f"select * from test.well_incl_data where well_incl in ({', '.join(str(x) for x in test_well_incl['id'].values)})",
                engine)
        else:
            test_well_incl = pd.DataFrame(columns=['id', 'well'])

        if len(test_well_incl_data) != 0:
            query_data = sql.SQL(
                f"delete from test.well_incl_data where well_incl in ({', '.join(str(x) for x in test_well_incl_data['well_incl'])})")
            cursor.execute(query_data)
        if len(test_well_incl) != 0:
            query = sql.SQL(
                f"delete from test.well_incl where well in({', '.join(str(x) for x in test_well_incl['well'])})")
            cursor.execute(query)
            query2 = sql.SQL(
                f"delete from test.conn_files where well in({', '.join(str(x) for x in test_well_incl['well'])})")
            cursor.execute(query2)

        for i in range(len(intercept)):
            max_id = int(pd.read_sql((f"(select max(id) from test.well_incl)"), engine).values)
            query = sql.SQL(
                f"insert into test.well_incl  values ({max_id + 1}, {list(intercept.id)[i]} , NULL, NULL, NULL, NULL, NULL )")

            cursor.execute(query)
            df2 = new_df[new_df['Скважина'] == intercept['uwi'][i]].reset_index(drop=True)
            df2 = df2.replace({'nan': 'NULL'})
            for j in range(len(df2)):
                query = sql.SQL(
                    f"insert into test.well_incl_data(id, well_incl, md, z, incl, azim, ext, thl, x, y, dx, dy, da, tvd, dls) values(nextval('seq'), {max_id + 1},{list(df2['md'])[j]}, {list(df2['z'])[j]}, {list(df2['incl'])[j]}, {list(df2['azim'])[j]}, {list(df2['ext'])[j]}, {list(df2['thl'])[j]}, {list(df2['x'])[j]}, {list(df2['y'])[j]}, {list(df2['dx'])[j]}, {list(df2['dy'])[j]}, {list(df2['da'])[j]}, {list(df2['tvd'])[j]}, {list(df2['dls'])[j]})")
                cursor.execute(query)
            max_id_doc = int(pd.read_sql((f"(select max(id) from test.documentt)"), engine).values)
            today = datetime.date.today()  # .strftime('%Y-%M-%D')
            query = sql.SQL(
                f"insert into test.documentt values({max_id_doc + 1}, 47, '{today}', 'Инклинометрия', '{today}', NULL, NULL, NULL)")
            cursor.execute(query)
            max_id_conn = int(pd.read_sql((f"(select max(id) from test.conn_files)"), engine).values)
            query = sql.SQL(
                f"insert into test.conn_files values({max_id_conn + 1}, {list(intercept.id)[i]}, {max_id_doc + 1}, 4)")
            cursor.execute(query)
            max_id_file_storage = int(pd.read_sql((f"(select max(id) from test.file_storage)"), engine).values)
            query = sql.SQL(
                f"insert into test.file_storage values({max_id_file_storage + 1}, '{list(intercept.uwi)[i]}.xlsx', {filename[i]}, NULL, NULL, NULL, {filesize[i]}, 'file_{filename[i]}', 'moduleincl')")
            cursor.execute(query)
            query = sql.SQL(
                f"insert into test.document_file values((select max(id)+1 from test.document_file),{max_id_file_storage + 1}, {max_id_doc + 1})")
            cursor.execute(query)

            a = 'Данные успешно заполнены'
        try:
            smtp_server = smtplib.SMTP(settings.EMAIL.get('smtp'), 587)
            smtp_server.starttls()
            email_getter = [settings.EMAIL.get('email_getter'), mail]
            msg = MIMEText(a)
            smtp_server.login(settings.EMAIL.get('email_sender'), os.getenv('outlook_pass'))
            smtp_server.sendmail("bduser4@niikmg.kz", email_getter, msg.as_string())
            return a
        except:
            return "Mail exception occurred"
    else:
        try:
            smtp_server = smtplib.SMTP(settings.EMAIL.get('smtp'), 587)
            smtp_server.starttls()
            email_getter = [settings.EMAIL.get('email_getter'), mail]
            msg = MIMEText(a)
            smtp_server.login(settings.EMAIL.get('email_sender'), os.getenv('outlook_pass'))
            smtp_server.sendmail(settings.EMAIL.get('email_sender'), email_getter, msg.as_string())
            return a
        except:
            return "Mail exception occurred"
