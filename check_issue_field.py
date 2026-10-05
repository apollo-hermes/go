import xmlrpc.client
import os
import pprint
from dotenv import dotenv_values

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env = dotenv_values(os.path.join(BASE_DIR, '.env'))

common = xmlrpc.client.ServerProxy(env['ODOO_URL'] + '/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(env['ODOO_DB'], env['ODOO_USERNAME'], env['ODOO_API_KEY'], {})
models = xmlrpc.client.ServerProxy(env['ODOO_URL'] + '/xmlrpc/2/object', allow_none=True)

try:
    fields = models.execute_kw(
        env['ODOO_DB'], uid, env['ODOO_API_KEY'],
        'project.task', 'fields_get', [], {'attributes': ['type', 'relation', 'selection']}
    )
    if 'x_issue_type' in fields:
        print("x_issue_type definition:")
        pprint.pprint(fields['x_issue_type'])
    else:
        print("x_issue_type field DOES NOT EXIST on project.task!")
except Exception as e:
    print("Error checking fields:", e)
