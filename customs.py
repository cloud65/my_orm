from uuid import uuid4
from datetime import datetime
from hashlib import md5
from .metadata import *

class UserRight:
    def __init__(self, **kwargs):
        self.set(**kwargs)

    def set(self, admin=False, user=False, operator=False):
        self.admin, self.user, self.operator = admin, user, operator

    def get_dict(self):
        result = dict()
        for name in ["admin","user","operator"]:
            result[name] = getattr(self, name)
        return result
        
    def __str__(self):
        for name in ["admin","operator","user"]:    
            if getattr(self, name):
                return name
        
        

class User(MetadataObject):
    def __init__(self, login="", password=None, name=None):
        super().__init__()  
        self.fields = Fields(
                name=FieldStr(), 
                login=FieldStr(), 
                password=FieldStr(), 
                setting=FieldDict(), 
                disabled=FieldBool(),
                last_date=FieldDateTime(),
                last_ip=FieldStr()
            )
        self.set_default()
        self.login = login
        if password:
            self.password = md5(password.encode()).hexdigest()
        self.name = name or login
        self.right = UserRight()
        self.setting = dict()
        
        
    def set_password(self, password: str, old_passwod:str = None)-> bool:
        md5_password = md5(password.encode()).hexdigest()
        if not old_passwod is None:
            if self.password!=md5(old_passwod.encode()).hexdigest():
                return False
                self.password = md5_password
        return True
    
    
    def read(self):
        super().read()
        self.right = UserRight(**self.setting.get("right", dict()))
    
        
    def write(self, *args, **kwargs):
        if not self.login:
            raise MetadataException('Attribute `login` is empty')
        if not self.name:
            self.name = self.login
        self.setting["right"] = self.right.get_dict()
        super().write(*args, **kwargs)
        
        
    def get_dict(self):
        result = super().get_dict()
        result.pop("password")
        result.pop("setting")
        result['right'] = self.right.get_dict()
        return result
        
        
    def load(self, value):
        password, last_date = self.password, self.last_date
        super().load(value)        
        self.password, self.last_date = password, last_date
        right = value.get('right', None)
        if right:
            self.right = UserRight(**right)
        
        

class Client(MetadataObject):
    def __init__(self, name=None, table_ports=None):
        super().__init__()
        self.fields = Fields(
                name=FieldStr(), 
                last_ip=FieldStr(), 
                setting=FieldDict(), 
                http_exchange=FieldBool(),
                http_interval=FieldInt(default=10),
                last_date=FieldDateTime(),
                ssh_host=FieldStr(),
                ssh_key=FieldStr()
            )
        self.set_default()
        self.name = name or "New client"        
        self.__ports = list() 
    
    @property
    def ports(self):
        return self.__ports
    
    
    @ports.setter
    def ports(self, ls):
        self.__ports = ls
    
    
    def read(self):        
        super().read()
        """print(list(self.metadata.ports.select(client=self.guid)))
        if self.__table_ports:
            self.__ports = list(self.__table_ports.select(client=self.guid))"""
         
         

class Port(MetadataObject):
    def __init__(self):
        super().__init__()        
        self.fields = Fields(name=FieldStr(), 
                            client=FieldObject(Client), 
                            host=FieldStr(), 
                            port=FieldInt(), 
                            f_port=FieldInt(), 
                            is_open=FieldBool(), 
                            app=FieldStr(), 
                            last_date=FieldDateTime())
        self.set_default()
        self.name = ""
        self.host = "127.0.0.1"
        self.port = 22
        self.f_port = 0
         

    def write(self, *args, **kwargs):
        if not isinstance(self.client, MetadataObject):
            raise MetadataException('Attribute `client` is not MetadataObject')
        elif not self.client.guid:
            raise MetadataException('Attribute `client` is empty')
        
        for f in self.metadata.ports.select(host=self.host, port=self.port, client=self.client.guid):
            if self!=f:
                raise MetadataException('Host and port is not unique')
        if self.f_port:
            for f in self.metadata.ports.select(f_port=self.f_port):
                if self!=f:
                    raise MetadataException('Forwarding port is not unique')
        else:
            self.f_port = self.new_forwarding_port()
        if not self.name:
            self.name = f"{self.host}:{self.port}"
        super().write(*args, **kwargs)
    
    
    def new_forwarding_port(self):
        min_port = int(self.metadata.get_option('min_port') or 20000)
        
        try:
            f = next(self.metadata.ports.select(order_by='f_port DESC', limit=1))
            new_port = max(min_port, f.f_port+1)
        except StopIteration:
            new_port = min_port
        return new_port    
        cursor = self.metadata.db.connection.cursor()
        query = f"""SELECT `f_port` AS port from `ports` ORDER BY `f_port` DESC LIMIT 1"""
        cursor.execute(query)        
        row = cursor.fetchone()
        cursor.close()
        new_port = row["port"] or min_port-1 if row else 0                
        return max(min_port, new_port+1)
        
        

class ClientDatabases(MetadataObject):
    def __init__(self, name=None):
        super().__init__()
        self.fields = Fields(
                client=FieldObject(Client),
                name=FieldStr(), 
                description=FieldStr(),
                login=FieldStr(),
                password=FieldStr(),                
                db_guid=FieldStr(),
                dbms=FieldStr(), 
                db_name=FieldStr(),
                db_server=FieldStr(), 
                setting=FieldDict(),
                size=FieldInt()
            )
        self.set_default()
        self.name = name or "New client"