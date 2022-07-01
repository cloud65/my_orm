from metadata import *
from datetime import datetime
md = Metadata(r'd:\tmp')   
md.add_table('users', User)
md.add_table('clients', Client)

usr=md.users.read(guid='7936083c-27e3-43a7-ac61-3d296a23e6d4')
print(type(usr.last_date), usr.last_date)
print(type(usr.setting), usr.setting)
usr.set_password('123')
usr.setting = {"a": 56}
usr.last_date = datetime.now()
usr.write()


client = md.clients.read(guid='106bfe97-e62f-4641-94af-b48a3ab53f69')
client.name = 'eeeeeeeee'
client.last_date = datetime.now()
print(client.user.login)
client.write()