import xmlrpc.client
import os
from dotenv import dotenv_values

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env = dotenv_values(os.path.join(BASE_DIR, '.env'))

common = xmlrpc.client.ServerProxy(env['ODOO_URL'] + '/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(env['ODOO_DB'], env['ODOO_USERNAME'], env['ODOO_API_KEY'], {})
models = xmlrpc.client.ServerProxy(env['ODOO_URL'] + '/xmlrpc/2/object', allow_none=True)

try:
    types = models.execute_kw(
        env['ODOO_DB'], uid, env['ODOO_API_KEY'],
        'project.task.type', 'search_read', [[]], {'fields': ['name']}
    )
    print("Project Task Types:", types)
except Exception as e:
    print("Error fetching project.task.type:", e)

try:
    issue_types = models.execute_kw(
        env['ODOO_DB'], uid, env['ODOO_API_KEY'],
        'project.task.issue.type', 'search_read', [[]], {'fields': ['name']}
    )
    print("\nProject Task Issue Types:", issue_types)
except Exception as e:
    print("\nError fetching project.task.issue.type:", e)
