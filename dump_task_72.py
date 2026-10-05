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
    task = models.execute_kw(
        env['ODOO_DB'], uid, env['ODOO_API_KEY'],
        'project.task', 'read', [[72]],
        {'fields': ['name', 'user_ids', 'x_assignee_id', 'x_reporter_id', 'create_uid', 'write_uid']}
    )
    print("=== TASK 72 ===")
    pprint.pprint(task)
except Exception as e:
    print("Error reading task 72:", e)
