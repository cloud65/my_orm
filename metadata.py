import os
import sqlite3
from uuid import uuid4
from json import dumps as json_dumps
from json import loads as json_loads
from datetime import datetime
from dateutil.parser import parse as dateparser
#dateparser = lambda x: datetime.fromisoformat(x)

def json_convert(val):
    if isinstance(val, MetadataObject):
        return {k: v for k, v in vars(val).items() if not "__" in k}
    elif isinstance(val, datetime):
        return val.strftime('%Y-%m-%d %H:%M:%S')


class Fields:
    def __init__(self, **kwargs):
        self.__names = list()
        for item in kwargs.items():
            self.__names.append(item[0])
            setattr(self, item[0], item[1])
    
    def items(self):
        for key in self.__names:
            yield (key, getattr(self, key))
            
    def get(self, key, default):
        return getattr(self, key, default)
    


class __Field:
    def __init__(self, default=None):
        self.default = default
        self.type = "TEXT"    
    
    def to_db(self, val):
        return val        
    
    def from_db(self, val, md=None):
        return val


class FieldStr(__Field):
    pass


class FieldInt(__Field):
    def __init__(self, default=0):
        self.default = default
        self.type = "INTEGER"
    

class FieldFloat(__Field):
    def __init__(self, default=0):
        self.default = default
        self.type = "REAL"
    
    
class FieldBool(__Field):
    def __init__(self, default=False):
        self.default = default
        self.type = "BOOL"
    
    def to_db(self, val):
        return 1 if val else 0 
        
    def from_db(self, val, md=None):
        return val==1
    

class FieldDict(__Field):
    def __init__(self, default=dict()):
        self.default = default
        self.type = "TEXT"
    
    def to_db(self, val):
        return json_dumps(val)         
    
    def from_db(self, val, md=None):
        return json_loads(val) if isinstance(val, str) else self.default
        

class FieldDateTime(__Field):
    def __init__(self, default=None):
        self.default = default or datetime(1970,1,1)
        self.type = "TEXT"
    
    def to_db(self, val):
        return val.strftime('%Y-%m-%d %H:%M:%S')        
    
    def from_db(self, val, md=None):
        return dateparser(val) if isinstance(val, str) else self.default 
        
class FieldDate(FieldDateTime):
    def to_db(self, val):
        return val.strftime('%Y-%m-%d')        
    
        
class FieldObject(__Field):
    def __init__(self, cls, default=None):
        self.default = default
        self.type = "TEXT"
        self.__class = cls    
    
   
    def to_db(self, val):
        return val.guid   
    
    
    def from_db(self, val, md):
        tab = md.get_table(self.__class.__name__)
        try:
            res = tab.read(guid=val)
        except MetadataException:
            res = self.__class()
        return res
    
    
class MetadataException(Exception):
    pass


class MetadataObject:
    def __init__(self):
        self.guid = None
        self.deleted = False
        self.fields = Fields()
        self.__record = MetadataRecord(None, None)
    
    def set_default(self):
        for key, field in self.fields.items():
            setattr(self, key, field.default)
    
    
    def read(self):
        self.__record.read(self)
        return self
        
    def write(self, new_guid=None, reg=True):
        self.__record.write(self, new_guid)
        if reg and self.table.reg:
            self.metadata.changes.set(self)
        return self
        
    def init_record(self, table, db, guid=None):
        self.__record = MetadataRecord(table, db, guid or self.guid)
    
    @property
    def table(self):
        return self.__record.table    
    
    @property
    def metadata(self):
        return self.__record.table.metadata
        
    def __str__(self):
        return str(vars(self))
        
    def __eq__(self, other):
        return self.guid==other.guid
    
    def get_dict(self, deep=True):        
        result = json_loads(json_dumps(self, default = json_convert))
        if not deep:
            for key, filed in self.fields.items():
                if isinstance(filed, FieldObject):
                    result[key] = result[key]['guid']
        result.pop('fields')
        return result

    def load(self, value):
        for key, field in self.fields.items():
            val = value.get(key, None)
            if not val is None:
                setattr(self, key, field.from_db(val, self.metadata))
            

class MetadataRecord:
    def __init__(self, table, db, guid=None):
        self.__db = db
        self.table = table        
        self.guid = guid   
        
    
    def read(self, obj: MetadataObject):
        if not self.guid:
            raise MetadataException("Нет идентификатора записи ")
        cursor = self.__db.connection.cursor()
        query = f"""SELECT * from `{self.table.name}` WHERE `guid`='{self.guid}'"""
        cursor.execute(query)        
        row = cursor.fetchone()        
        if not row:
            raise MetadataException(f"Нет записи в таблице `{self.table.name}`")        
        
        for item in row.items():
            key = item[0]
            val = item[1]
            field_type = obj.fields.get(key, FieldStr())
            setattr(obj, key, field_type.from_db(val, self.table.metadata))
     
    
    def write(self, obj: MetadataObject, new_guid=None):
        fields_type = { key: value for key, value in obj.fields.items() }
        fields_type['guid'] = FieldStr()
        fields_type['deleted'] = FieldBool()
        data = dict()
        for field in fields_type.items():
            default = field[1].default
            data[field[0]] = field[1].to_db(getattr(obj, field[0], default))
        
        if data.get("guid"):
            fields = list()
            for key, value in data.items():
                if value is None:
                    fields.append(f"`{key}`=NULL")
                elif fields_type[key].type=='TEXT':
                    fields.append(f"`{key}`='{value}'")
                else:
                    fields.append(f"`{key}`={value}")
            query = f"""UPDATE `{self.table.name}` SET {', '.join(fields)} WHERE `guid`='{data.get("guid")}'"""
        else:
            data["guid"] = new_guid or str(uuid4())
            fields = [f"`{i[0]}`" for i in data.items()]            
            values = list()            
            for key, value in data.items():
                if value is None:
                    values.append("NULL")
                elif fields_type[key].type=='TEXT':
                    values.append(f"'{value}'")
                else:
                    values.append(f"{value}")
            query = f"""INSERT INTO `{self.table.name}` ({', '.join(fields)}) VALUES ({', '.join(values)})"""  

        cursor = self.__db.connection.cursor()
        cursor.execute(query)
        #if not cursor.fetchall():
        #    raise MetadataException('Не удалось выполнить запись')        
        self.__db.connection.commit()
        obj.guid = data["guid"]
        cursor.close()
        

class MetadataTable:
    def __init__(self, md, name, cls, reg=True):
        self.__md = md
        self.__db = md.db
        self.name = name
        self.reg = reg
        self.__class = cls
        self.__init_structure()        
    
    
    def is_class(self, obj):
        if isinstance(obj, MetadataObject):
            return type(obj) is self.__class
        else: 
            return obj == self.__class.__name__
    
    
    @property
    def metadata(self):
        return self.__md
        

    def __init_structure(self):
        obj = self.__class()
        cursor = self.__db.connection.cursor()        
        
        query = f"SELECT * FROM `sqlite_master` WHERE `name`='{self.name}' AND `type`='table'"        
        cursor.execute(query)
        if not cursor.fetchone():
            self.__create_table()
            return
        
        query = f"PRAGMA  table_info(`{self.name}`)"        
        cursor.execute(query)
        rows = cursor.fetchall()
        table_fields = set([ row['name'] for row in rows])
        for field in obj.fields.items():
            if not field[0] in table_fields:
                query = f"ALTER TABLE `{self.name}` ADD COLUMN `{field[0]}` {field[1].type}"
                cursor.execute(query)
                self.__db.connection.commit()
        cursor.close()
    
    
    def __create_table(self):
        obj = self.__class()
        str_fields = [ 
          f"""`{name}` {field.type} """ for name, field in obj.fields.items() if name!='guid'
        ]
        str_fields.insert(0, '`deleted` BOOL')
        str_fields.insert(0, '`guid` TEXT PRIMARY KEY')
        
        cursor = self.__db.connection.cursor()
        query = f"CREATE TABLE `{self.name}` ({','.join(str_fields)})"
        cursor.execute(query)
        self.__db.connection.commit()
        cursor.close()
    
    
    def add(self):
        obj = self.__class()
        obj.init_record(self, self.__db)
        return obj
    
    
    def read(self, guid):
        obj = self.__class()
        obj.init_record(self, self.__db, guid=guid)
        obj.read()
        return obj
        
   
    def select(self, **filter_and_order):
        order_by = filter_and_order.get("order_by", "")
        if order_by:
            filter_and_order.pop('order_by')
            order_by = f"ORDER BY {order_by}"
        limit = filter_and_order.get("limit", "")
        if limit:
            filter_and_order.pop('limit')
            limit = f"LIMIT {limit}"    
            
        ls = [f"`{item[0]}`='{item[1]}'" for item in filter_and_order.items()]
        if not ls:
            ls.append('1=1')
        cursor = self.__db.connection.cursor()        
        query = f"""SELECT `guid` from `{self.name}` WHERE {" AND ".join(ls)} {order_by} {limit}""" 
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        for row in rows:
            yield self.read(row['guid'])
        

class Database:
    @classmethod
    def dict_factory(t, cursor, row):          
        d = {}  
        for idx, col in enumerate(cursor.description):  
            d[col[0]] = row[idx]  
        return d
        
        
    def __init__(self, path):
        self.connection = None
        full_path = os.path.join(path, 'nexum.db')
        try:
            self.connection = sqlite3.connect(full_path, check_same_thread=False)
            self.connection.row_factory = Database.dict_factory
        except sqlite3.Error as error:
            print(f"Ошибка при подключении к sqlite ({full_path}): {error}")
            self.connection = None


class Setting(MetadataObject):
    def __init__(self):
        super().__init__()  
        self.fields = Fields(name=FieldStr(), value=FieldStr(), description=FieldStr(), val_type=FieldStr())


class DataChanges:
    def __init__(self):
        self.__data = dict()
    
    def __key(self, obj: MetadataObject):
        return type(obj).__name__ + "/" + obj.guid
    
    def set(self, obj: MetadataObject, send: bool=False):         
        self.__data[self.__key(obj)] = {"obj": obj, "send": send}
    
    def clear(self, obj_list: list=None):
        for obj in obj_list:
            if isinstance(obj, MetadataObject):
                key = self.__key(obj)
            elif isinstance(obj, tuple): # (<name class>, <guid>)
                key = obj[0]+'/'+ obj[1]
            else: # "name/guid"
                key = obj
            if self.__data[key]['send']:
                self.__data.pop(key)
    
    def select(self, cls=None, send=None):        
        for key, value in self.__data.items():
            if not cls is None and not isinstance(value.get("obj"), cls):
                continue            
            if not send is None and send!=value.get("send"):                
                continue
            yield (value.get("obj"), value.get("send"))   


class Metadata:    
    def __init__(self, path: str):
        self.db = Database(path)
        self.__tables = list()
        self.add_table('_setting', Setting)
        self.__setting_cache = dict()
        self.changes = DataChanges()
        
       
       
    def add_table(self, name, cls, reg=True):        
        setattr(self, name, MetadataTable(self, name, cls, reg))
        self.__tables.append(name)
    
    
    def get_table(self, obj):
        for tab_name in self.__tables:
            tab = getattr(self, tab_name)
            if tab.is_class(obj):
                return tab
        return None
    
    def select_options(self, **filt):
        return self._setting.select(**filt)
    
    
    def get_option(self, key):
        value = self.__setting_cache.get(key)
        if not value is None:
            return value
        try:
            s = next(self._setting.select(name=key))
        except StopIteration:
            return None
        if s.val_type=='bool':
            value = s.value=="1"
        elif s.val_type=='int':
            value = int(s.value)
        else: 
            value = s.value
        self.__setting_cache[key] = s.value
        return s.value


    def set_option(self, name, value, description=None, val_type=None):        
        self.__setting_cache[name] = value
        try:
            s = next(self._setting.select(name=name))
        except StopIteration:
            s = self._setting.add()
            s.name = name                    
        if not description is None:
            s.description = description
        if not val_type is None:
            s.val_type = val_type
            if val_type=='bool':
                value = 1 if value else 0
        s.value = str(value)
        s.write()
        
    def init_setting(self, data):
        keys = list()
        for opt in self._setting.select():        
            if opt.name in data:
                keys.append(opt.name)
        for key, value in data.items():
            if not key in keys:
                self.set_option(key, value[0], description=value[2], val_type=value[1])
            