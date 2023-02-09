from dotenv import load_dotenv
out = load_dotenv(dotenv_path='/Users/bantanalu.anitha/PycharmProjects/IDS_extractor-main/Conf/local.env')
import os
import traceback
import camelot
from PyPDF2 import PdfFileReader
import pandas as pd
import json
import re
from datetime import datetime
#from src.ai_correction_layer.ai_correction_without_confidence import correction_layer
#from src.utils import db_connection
# Accepting the path of Downloaded file
input_dir = os.path.join(os.getenv('DATA_PATH'), os.getenv('APPLICATION_PATH'))
from src.utils.logmodule import get_module_logger


req_log_file_name = os.path.join(os.getenv('LOG_DIR'), 'doc_892.log')
reqLogger = get_module_logger(req_log_file_name)


def getData(f): # file path
    inputPDF = PdfFileReader(open(f, "rb"))
    noofpages = inputPDF.numPages

    data = {}
    data['US'] = []
    data['Non_US'] = []
    data['NPL'] = []

    try:

        for i in list(range(1,noofpages))+[0]:
            print(i)
            tables = camelot.read_pdf(f, pages=str(i))

            # Hadnling Table 1 Data (Always US)
            print(tables[1].df)
            for j,r in tables[1].df.iterrows():
                app = {}
                if j > 0:
                    if r[2] != 'US-' and len(r[2]) != 0:

                        try:
                            res = re.search('([A-Z]{2}-)([\w\d,\/\.\s]+)(-[[A-Z12]{1,2})*', r[2])
                            if res:
                                if res.group(2):
                                    app['ref_kc'] = res.group(3).replace("-","").strip()
                                else:
                                    app['ref_kc'] = ''
                                app['ref_no'] = res.group(2).replace(",","").replace("/","").replace("-","").strip()
                                app['ref_cc'] = 'US'
                            else:
                                app['ref_kc'] = ''
                                app['ref_no'] = r[2]
                                app['ref_cc'] = 'US'
                        except:
                            print(traceback.format_exc())
                            reqLogger.info('Error in Breaking the r[2] >>> '+str(r[2]))
                            app['ref_kc'] = ''
                            app['ref_no'] = r[2]
                            app['ref_cc'] = 'US'






                        app['ref_date'] = str(datetime.strptime(re.sub(r'\s','',r[3]), '%m-%Y').date())
                        app['ref_applicant'] = r[4]
                        app['ref_CPC'] = r[5]

                        if len(r) == 7:
                            app['ref_USC'] = r[6]
                        data['US'].append(app)

            # Handling Table 2 Data (Always Non-US)

            for j,r in tables[2].df.iterrows():
                app = {}
                if j > 0:
                    if len(r[2]) != 0:
                        r[2] = r[2].replace(' ', '')
                        app['ref_no'] = r[2]
                        if r[2] == 'J P-2000282081-A':
                            print('hellow')
                        else:
                            print(r[2])

                        try:
                            res = re.search('([A-Z]{2}-)([\w\d,\/\.\s]+)(-[[A-Z12]{1,2})*', r[2])
                            if res:
                                if res.group(3):
                                    app['ref_kc'] = res.group(3).replace("-","").strip()
                                else:
                                    app['ref_kc'] = ''
                                app['ref_no'] = res.group(2).replace(",","").replace("/","").replace("-","").strip()
                                app['ref_cc'] = res.group(1).replace("-","").strip()
                            else:
                                app['ref_kc'] = ''
                                app['ref_no'] = r[2]
                                app['ref_cc'] = ''
                        except:
                            print(traceback.format_exc())
                            reqLogger.info('Error in Breaking the r[2] >>> '+str(r[2]))
                            app['ref_kc'] = ''
                            app['ref_no'] = r[2]
                            app['ref_cc'] = ''

                        app['ref_date'] = str(datetime.strptime(re.sub(r'\s','',r[3]), '%m-%Y').date())
                        app['ref_cc'] = r[4]
                        app['ref_applicant'] = r[5]

                        if len(r) == 7:
                            app['ref_cpc'] = r[6]
                        data['Non_US'].append(app)

            # Handling Table 3 Data (Always NPLs)

            for j, r in tables[3].df.iterrows():
                app = {}
                if j > 0:
                    if len(r[2]) != 0:
                        # app['NPL'] = r[2]
                        data['NPL'].append(r[2])

        return data['US'], data['Non_US'], data['NPL']
    except:
        print(traceback.format_exc())






def write_db(db_df, application_name, dms_id, customer, tableName = ''):
    conn = db_connection.MysqlAlchemy()
    print('Now in writeToDB... :' + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    conn.deleteRowsForApplication(application_name, dms_id, customer, tableName=tableName)
    conn.writeToDB(db_df, tableName)
    return



def run_all_892(application_no,doc_code,dms_id,customer):
    file_name = doc_code+'_'+dms_id
    file_path = '/Users/bantanalu.anitha/PycharmProjects/IDS_extractor-main/src/DOC_892/892_Extaction_issues/892_regex.pdf'
    reqLogger.info('Starting the 892 Extraction for >>> '+str(file_name))
    print('Starting the 892 Extraction for >>> '+str(file_name))
    try:
        if os.path.isfile(file_path):
            us_892, nonus_892, npl_892 = getData(file_path)
            df_892_data = {'standard_application_no': application_no, 'file_name': file_name, 'dms_id': dms_id,'file_type':'pdf','customer':customer,'us': json.dumps(us_892), 'non_us': json.dumps(nonus_892),'npl': json.dumps(npl_892)}
            corrected_data = {}
            try:
                reqLogger.info('Calling AI Validation Layer.')
                corrected_data = correction_layer(df_892_data,'doc_892')
                reqLogger.info('AI Validation Done.')

            except:
                reqLogger.error('Error on AI Validation Layer')
                reqLogger.error(traceback.format_exc())

            if len(corrected_data)!=0:
                corrected_data['npl_raw'] = corrected_data['npl']
                df_892 = pd.DataFrame(corrected_data, index=[0])
            else:
                df_892_data['us_raw'] = df_892_data['us']
                df_892_data['non_us_raw'] = df_892_data['non_us']
                df_892_data['npl_raw'] = df_892_data['npl']
                df_892_data['modified'] = False
                df_892 = pd.DataFrame(df_892_data, index=[0])

            print(df_892_data)

            if not df_892.empty:
                db_col_structure = ['standard_application_no','file_name','dms_id','file_type','customer','us','non_us','npl','us_raw','non_us_raw','npl_raw','modified']
                df_892 = df_892[db_col_structure]
                # write_db(df_892,application_no,dms_id,customer,tableName='doc_892')


        return 'SUCCESS'
    except:
        reqLogger.info(traceback.format_exc())
        print(traceback.format_exc())
        return 'ERROR'


if __name__ == '__main__':
    run_all_892('test','892','dmsid','customer')

