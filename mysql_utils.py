import simplejson as json
import logging
import datetime
import os
import re
import mysql.connector as mysql

from red_fin import settings
from accounts.models import CustomUser
from red_fin import mysql_models
from django.apps import apps
from collections import OrderedDict
from pytz import timezone
from red_fin.tasks import email
from red_fin.s3_bucket_utils import s3_save_csv

logger = logging.getLogger('dashboard')
get_model = apps.get_model

local_tz = timezone(settings.TIME_ZONE)

def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

# Make mysql query to get the objects
def get_object(select, model, filters, order_by, rename_fields, instance_name, user_id, limit=0, user_email=False, remove_id=False, generate_sql=False, summarize_array=[], rollup=False):
    try:
        # load in the datamodel that was created with 'datamodel_script' task (in readme)
        path = os.path.join(settings.STATIC_ROOT, "app", "dist", "assets", "mysql_datamodel.json")
        f = open(path,'r')
        datamodel = json.loads(f.read())
        f.close()
        join = ""
        filter_str = ""
        group_by_str = ""
        headers = []

        # Prevent bugs when there is only 1 or no filters
        try:
            if filters[0].__len__() == 1:
                filters = []
        except:
            filters = []

        # only run if there are no nested fields to select and you want to only output the raw sql
        if not select[model]['children'] and generate_sql:
            sql = "SELECT "
            index = 0
            for field in select[model]['fields']:
                headers.append(field)
                if index == 0: sql += "%s" % (field)
                else: sql += ", %s" % (field)
                index += 1
        elif summarize_array.__len__() >= 1:
            if generate_sql:
                sql = "SELECT "
            else:
                sql = "SELECT " % model
            group_by_str = ' GROUP BY '
            for field in summarize_array:
                headers.append("%s_%s" % (field['table'], field['field']))
                sql += "%s.%s AS %s_%s, " % (field['table'], field['field'], field['table'], field['field'])
                group_by_str += "%s_%s, " % (field['table'], field['field'])
            for field in select[model]['fields']:
                header = "%s_%s" % (model, field)
                if header in headers:
                    continue
                if datamodel[model]['fields'][field]['type'] == 'number':
                    sql += "sum(%s.%s) AS %s_%s_sum, " % (model, field, model, field)
                else:
                    sql += "count(%s.%s) AS %s_%s_count, " % (model, field, model, field)
            sql = sql[:-2]
            group_by_str = group_by_str[:-2]
        elif generate_sql:
            sql = "SELECT "
            index = 0
            for field in select[model]['fields']:
                if index == 0: sql += "%s.%s AS %s_%s" % (select[model]['label'], field, select[model]['label'], field)
                else: sql += ", %s.%s AS %s_%s" % (select[model]['label'], field, select[model]['label'], field)
                index += 1
        
        else:
            sql = "SELECT "
            index = 0
            if rename_fields:
                for field in select[model]['fields']:
                    if index == 0: sql += "%s.%s AS %s_%s" % (select[model]['label'], field, select[model]['label'], field)
                    else: sql += ", %s.%s AS %s_%s" % (select[model]['label'], field, select[model]['label'], field)
                    index += 1
            else: #getting belongs to fields
                for field in select[model]['fields']:
                    if index == 0:
                        if field == 'id': sql += "%s.%s AS id" % (select[model]['label'], field)
                        else: sql += "%s.%s AS display" % (select[model]['label'], field)
                    else: 
                        if field == 'id': sql += ", %s.%s AS id" % (select[model]['label'], field)
                        else: sql += ", %s.%s AS display" % (select[model]['label'], field)
                    index += 1
            if index == 0:
                sql = sql[:-2]

        index = 0
        if filters.__len__() > 0:
            where_operators = list(map(lambda x : x['where_operator'], filters))
            for query in filters:
                index += 1
                if index == 1:
                    filter_str += " WHERE "
                    if index < filters.__len__():
                        if filters[index]['where_operator'] == 'OR':
                            filter_str += '('
                else:
                    filter_str += ' %s ' % (query['where_operator'])
                    if index < filters.__len__():
                        if (filters[index-1]['where_operator'] == 'AND') and (filters[index]['where_operator'] == 'OR'):
                            filter_str += '('
                if generate_sql:
                    filter_str += '' + query['table'] + '.' + query['field']
                else:
                    filter_str += '' + query['table'] + '.' + query['field']
                if query["type"] in ["datetime"] and query.__len__() == 6:
                    query['value'] = (datetime.datetime.strptime(query["value"][:19].replace('T',' '), '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')
                if query["operator"] == "Equals":
                    if query["type"] in ["date", "datetime"]:
                        temp_date = datetime.datetime.strptime(query["value"][:10], '%Y-%m-%d') + datetime.timedelta(days=2)
                        filter_str += " >= '%s' AND %s.%s < '%s'" % (query["value"], query['table'], query['field'], temp_date.strftime('%Y-%m-%d'))
                    else:
                        filter_str += " = '" + query["value"] + "'"
                elif query["operator"] == "Contains": filter_str += " LIKE '%%" + query["value"] + "%%'"
                elif query["operator"] == "Greater than or equal": filter_str += " >= '" + query["value"] + "'"
                elif query["operator"] == "Less than or equal":
                    if query["type"] in ["date", "datetime"]:
                        temp_date = datetime.datetime.strptime(query["value"][:10], '%Y-%m-%d') + datetime.timedelta(days=1)
                        filter_str += " <= '%s'" % temp_date.strftime('%Y-%m-%d')
                    else:
                        filter_str += " <= '" + query["value"] + "'"
                elif query["operator"] == "Greater than": filter_str += " > '" + query["value"] + "'"
                elif query["operator"] == "Less than": filter_str += " < '" + query["value"] + "'"
                elif query["operator"] == "Not equal to": filter_str += " != '" + query["value"] + "'"
                elif query["operator"] == "Exists": filter_str += " IS NOT NULL"
                elif query["operator"] == "Does not exist": filter_str += " IS NULL"
                if (filters[index-1]['where_operator'] == 'OR') and (index == filters.__len__()):
                    filter_str += ')'
                else:
                    if (filters[index-1]['where_operator'] == 'OR') and (filters[index]['where_operator'] == 'AND'):
                        filter_str += ')'

        if summarize_array.__len__() >= 1:
            join_str, select_str, header_arr = get_all_values_summarized(select[model]['children'], model, datamodel, '', '', headers)
        else:
            join_str, select_str, header_arr = get_all_values(select[model]['children'], model, '', '', headers)
        if generate_sql and sql == 'SELECT ':
            select_str = select_str[2:]
        
        sql = "%s%s FROM %s%s%s" % (sql, select_str, model, join_str, filter_str)
        if group_by_str:
            sql += group_by_str
            if rollup:
                sql += " WITH ROLLUP"
        elif filters.__len__() > 0:
            if order_by:
                sql += " ORDER BY %s" % order_by
            else:
                sql += " ORDER BY %s.row_num DESC" % model
        if limit:
            sql += " LIMIT %s" % limit

        if generate_sql:
            return json.dumps({"sql": sql})
        # Get object model
        rows = []
        db = mysql.connect(
            host = "***",
            user = "***",
            passwd = "***",
            database = instance_name
        )
        cursor = db.cursor()
        CustomUser.objects.filter(id=user_id).update(mysql_connection_id=db.connection_id)
        cursor.execute(sql)
        result = cursor.fetchall()
        headers=[x[0] for x in cursor.description] #this will extract row headers
        db.close()
        for res in result:
            temp_dict = OrderedDict()
            for key in range(len(res)):
                if isinstance(res[key], datetime.datetime):
                    temp_dict[headers[key]] = res[key].strftime('%Y-%m-%d %H:%M:%S')
                else:
                    temp_dict[headers[key]] = res[key]
            rows.append(temp_dict)

        if user_email:
            url = 'temp/%s_%s.csv' % (user_email,model)
            if s3_save_csv(headers, rows, url):
                content = 'Click the link to open your csv: \n\r %s/document/%s' % (settings.LOCAL_WEB_SERVER, url)
                email(content=content,
                    subject='Query table CSV is ready',
                    to_address=user_email,
                    from_address='no-reply@modalityapps.com')
            else:
                email(content='CSV export failed',
                    subject='CSV export failed',
                    to_address=user_email,
                    from_address='no-reply@modalityapps.com')
            return
        else:
            result = {"headers":headers, "rows":rows}
            
            json_content = json.dumps(result,indent=1,default=default)
            return json_content
    except Exception as e:
        logger.error(str(e))
        db.close()
        return json.dumps({'error': str(e)})


def get_raw_sql(raw_sql, instance_name, user_id):
    try:
        rows = []
        db = mysql.connect(
            host = "***",
            user = "***",
            passwd = "***",
            database = instance_name
        )
        cursor = db.cursor()
        CustomUser.objects.filter(id=user_id).update(mysql_connection_id=db.connection_id)
        cursor.execute(raw_sql)
        result = cursor.fetchall()
        headers=[x[0] for x in cursor.description] #this will extract row headers
        db.close()
        for res in result:
            temp_dict = OrderedDict()
            for key in range(len(res)):
                if isinstance(res[key], datetime.datetime):
                    temp_dict[headers[key]] = res[key].strftime('%Y-%m-%d %H:%M:%S')
                else:
                    temp_dict[headers[key]] = res[key]
            rows.append(temp_dict)

        result = {"rows":rows, "headers": headers}
            
        json_content = json.dumps(result,indent=1,default=default)
        CustomUser.objects.filter(id=user_id).update(mysql_connection_id='')
        return json_content
    except Exception as e:
        db.close()
        return json.dumps({'error': str(e)})


def get_objects_count(model, filters, select, instance_name):
    filter_str = ""
    index = 0
    try:
        where_operators = list(map(lambda x : x['where_operator'], filters))
        try:
            if filters[0].__len__() == 1:
                filters = []
        except:
            filters = []
        for query in filters:
            index += 1
            if index == 1:
                filter_str += " WHERE "
                if index < filters.__len__():
                    if filters[index]['where_operator'] == 'OR':
                        filter_str += '('
            else:
                filter_str += ' %s ' % (query['where_operator'])
                if index < filters.__len__():
                    if (filters[index-1]['where_operator'] == 'AND') and (filters[index]['where_operator'] == 'OR'):
                        filter_str += '('
            filter_str += '' + query['table'] + '.' + query['field']
            if query["operator"] == "Equals":
                if query["type"] in ["date", "datetime"]:
                    temp_date = datetime.datetime.strptime(query["value"][:10], '%Y-%m-%d') + datetime.timedelta(days=2)
                    filter_str += " >= '%s' AND %s.%s < '%s'" % (query["value"], query['table'], query['field'], temp_date.strftime('%Y-%m-%d'))
                else:
                    filter_str += " = '" + query["value"] + "'"
            elif query["operator"] == "Contains": filter_str += " LIKE '%%" + query["value"] + "%%'"
            elif query["operator"] == "Greater than or equal":
                filter_str += " >= '" + query["value"] + "'"
            elif query["operator"] == "Less than or equal":
                    if query["type"] in ["date", "datetime"]:
                        temp_date = datetime.datetime.strptime(query["value"][:10], '%Y-%m-%d') + datetime.timedelta(days=1)
                        filter_str += " <= '%s'" % temp_date.strftime('%Y-%m-%d')
                    else:
                        filter_str += " <= '" + query["value"] + "'"
            elif query["operator"] == "Greater than": filter_str += " > '" + query["value"] + "'"
            elif query["operator"] == "Less than": filter_str += " < '" + query["value"] + "'"
            elif query["operator"] == "Not equal to": filter_str += " != '" + query["value"] + "'"
            elif query["operator"] == "Exists": filter_str += " IS NOT NULL"
            elif query["operator"] == "Does not exist": filter_str += " IS NULL"
            if (filters[index-1]['where_operator'] == 'OR') and (index == filters.__len__()):
                filter_str += ')'
            else:
                if (filters[index-1]['where_operator'] == 'OR') and (filters[index]['where_operator'] == 'AND'):
                    filter_str += ')'

        join_str, select_str, header_arr = get_all_values(select[model]['children'], model, '', '', [])
        sql = "SELECT count(*) AS id FROM %s%s%s" % (model, join_str, filter_str)
        # Get object model
        model_name = model.title().replace('_','')
        db = get_model("red_fin", model_name)
        result = db.objects.using(instance_name).raw(sql)

        for res in result:
            temp_dict = res.__dict__
        return {"success": temp_dict['id']}
    except Exception as e:
        logger.error(str(e))
        return {"failed": str(e)}


def get_fields(model):
    fields_to_ignore = ['row_num', 'updated_at', 'password', 'field_ver', 'display', 'terms_acceptance', 'face_photo', 'id', 'webhook_trigger']
    embed_fields = ['agent_id','agent_commission_rule_id','area_id','branch_id','broadcast_id','cashbox_category_id','category_id','company_id','compuscan_id','document_type_id','employer_id','loan_insurance_id','loan_product_id','operator_id','payout_method_id','product_addon_type_id','purpose_id','repayment_method_id','role_id','transaction_category_id','worker_id']
    fields = []
    try:
        db = get_model('red_fin', model)
        all_fields = db._meta.fields
        for field in all_fields:
            # Remove sensitive and redundant fields
            if field.name in fields_to_ignore:
                continue
            if field.name[-3:] == '_id':
                # Only add relevant belongs_to fields
                if field.name in embed_fields:
                    fields.append({
                        "name": field.column,
                        "type": str(field.description)
                    })
            else:
                fields.append({
                    "name": field.column,
                    "type": str(field.description)
                })
        return json.dumps(fields)
    except Exception as e:
        logger.error(str(e))


def get_all_values(nested_dictionary, parent, join_str='', select_str='', header_arr=[]):
    if nested_dictionary.__len__() > 0:
        for key, value in nested_dictionary.items():
            join_str += " LEFT JOIN %s AS %s ON %s.%s_id = %s.id" % (value['model'], value['label'], parent, value['label'], value['label'])
            for field in value['fields']:
                header_arr.append("%s_%s" % (value['label'], field))
                select_str += ", %s.%s AS %s_%s" % (value['label'], field, value['label'], field)
            if value['children']:
                join_str, select_str, header_arr = get_all_values(value['children'], value['label'], join_str, select_str, header_arr)
        return join_str, select_str, header_arr
    else:
        return '', '', header_arr


def get_all_values_summarized(nested_dictionary, parent, datamodel, join_str='', select_str='', header_arr=[]):
    if nested_dictionary.__len__() > 0:
        for key, value in nested_dictionary.items():
            join_str += " LEFT JOIN %s AS %s ON %s.%s_id = %s.id" % (value['model'], value['label'], parent, value['label'], value['label'])
            for field in value['fields']:
                if "%s_%s" % (value['label'], field) in header_arr:
                    continue
                if datamodel[value['model']]['fields'][field]['type'] == 'number':
                    header_arr.append("%s_%s_sum" % (value['label'], field))
                    select_str += ", sum(%s.%s) AS %s_%s_sum" % (value['label'], field, value['label'], field)
                else:
                    header_arr.append("%s_%s_count" % (value['label'], field))
                    select_str += ", count(%s.%s) AS %s_%s_count" % (value['label'], field, value['label'], field)
            if value['children']:
                join_str, select_str, header_arr = get_all_values_summarized(value['children'], value['model'], datamodel, join_str, select_str, header_arr)
        return join_str, select_str, header_arr
    else:
        return '', '', header_arr


def kill_query(conn_id, instance_name, user_id):
    try:
        db = mysql.connect(
            host = "***",
            user = "***",
            passwd = "***",
            database = instance_name
        )
        cursor = db.cursor()
        cursor.execute('KILL ' + conn_id)
        db.close()
        CustomUser.objects.filter(id=user_id).update(mysql_connection_id='')
        return json.dumps({'done': 'Query ' + conn_id + ' killed'})
    except Exception as e:
        logger.error(str(e))
        return json.dumps({'error': str(e)})