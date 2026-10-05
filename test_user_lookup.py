import xmlrpc.client
import os
from dotenv import dotenv_values

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env = dotenv_values(os.path.join(BASE_DIR, '.env'))

common = xmlrpc.client.ServerProxy(env['ODOO_URL'] + '/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(env['ODOO_DB'], env['ODOO_USERNAME'], env['ODOO_API_KEY'], {})
models = xmlrpc.client.ServerProxy(env['ODOO_URL'] + '/xmlrpc/2/object', allow_none=True)

identifier = "DanielDa"
domain = [
    '|', ('login', 'ilike', identifier),
    '|', ('email', 'ilike', identifier),
         ('name', 'ilike', identifier)
]

users = models.execute_kw(
    env['ODOO_DB'], uid, env['ODOO_API_KEY'],
    'res.users', 'search_read',
    [domain],
    {'fields': ['id', 'name', 'login', 'email']}
)

print(f"Lookup for '{identifier}':")
for u in users:
    print(u)
