from datetime import datetime
import pytz
import json
import requests
from time import sleep
import snap7
import struct
import csv
from snap7.exceptions import Snap7Exception
from requests.exceptions import RequestException

class etl_ap: 
    """
    Conexión y adquisición de datos de PLCs Siemens, utilizando el protocolo S7-Connection.
    Formateo de datos.
    """

    def __init__(self, ip:str, rack:int, slot:int, name:str, route_log:str, url_api:str, token:str, variables:list, db:int, period:int=60, verbose:bool=False, port:int=102):             
        """
        Constructo de la clase.
        :param ip: Dirección ip del PLC
        :param rack: Rack del PLC
        :param slot: Slot interfaz de red del PLC
        :param name: Nombre asignado al PLC (no es el nombre PROFINET)
        :param port: Puerto TCP para conexión con el PLC (por defecto 102)
        """
        self.ip = ip
        self.rack = rack
        self.slot = slot
        self.name = name
        self.route_log = route_log
        self.url_api = url_api
        self.token = token
        self.variables = variables
        self.db = db
        self.period = period
        self.verbose = verbose
        self.port = port

    def get_LReal(self, quantity: int, offset: int, data: bytearray):

        """
        Formatea un array de bytes en datos de tipo LReal (Real de 8 byte).

        :param quantity: Cantidad de datos de tipo LReal a formatear (cada dato contiene 8 bytes)
        :param offset: Offset del byte desde donde se debe formatear los datos 
        :param data: Array de bytes adquiridos desde el PLC 
        :return: lista de datos de tipo Float
        :rtype: list Float 
        """
        output = []
        for i in range(quantity):
            offset_aux = offset + i*8
            lreal_bytes = data[offset_aux:offset_aux + 8]
            lreal_value = struct.unpack('>d', lreal_bytes)[0]
            output.append(lreal_value )
        return output
    
    def get_DInt(self, quantity: int, offset: int, data: bytearray): 
        """
        Formatea un array de bytes en datos de tipo Dint (Int con signo de 4 bytes).

        :param quantity: Cantidad de datos de tipo LReal a formatear (cada dato contiene 4 bytes)
        :param offset: Offset del byte desde donde se debe formatear los datos 
        :param data: Array de bytes adquiridos desde el PLC 
        :return: lista de datos de tipo Int
        :rtype: list Int
        """
        output = []
        for i in range(quantity):
            offset_aux = offset + i*4
            dint_bytes = data[offset_aux:offset_aux + 4]
            dint_value = struct.unpack('>i', dint_bytes)[0]
            output.append(dint_value)
        return output
    
    def get_UInt(self, quantity: int, offset: int, data: bytearray):
        """
        Formatea un array de bytes en datos de tipo UInt (Int sin signo de 2 bytes).

        :param quantity: Cantidad de datos de tipo UInt a formatear (cada dato contiene 2 bytes)
        :param offset: Offset del byte desde donde se debe formatear los datos 
        :param data: Array de bytes adquiridos desde el PLC 
        :return: lista de datos de tipo Int
        :rtype: list Int
        """
        output = []
        for i in range(quantity):
            offset_aux = offset + i*2
            uint_bytes = data[offset_aux:offset_aux + 2]
            uint_value = struct.unpack('>H', uint_bytes)[0]
            output.append(uint_value)
        return output
    
    def get_Byte(self, quantity: int, offset: int, data: bytearray): 
        """
        Formatea un array de bytes en datos de tipo Byte (Int sin signo de 1 byte).

        :param quantity: Cantidad de datos de tipo Byte a formatear (cada dato contiene 1 byte)
        :param offset: Offset del byte desde donde se debe formatear los datos 
        :param data: Array de bytes adquiridos desde el PLC 
        :return: lista de datos de tipo Int
        :rtype: list Int
        """ 
        output = []
        for i in range(quantity):
            offset_aux = offset + i*1
            byte_bytes = data[offset_aux:offset_aux + 1]
            byte_value = struct.unpack('>B', byte_bytes)[0]
            output.append(byte_value)
        return output
    
    def get_Real(self, quantity: int, offset: int, data: bytearray): 
        """
        Formatea un array de bytes en datos de tipo Real (Real de 4 bytes).

        :param quantity: Cantidad de datos de tipo Real a formatear (cada dato contiene 4 bytes)
        :param offset: Offset del byte desde donde se debe formatear los datos 
        :param data: Array de bytes adquiridos desde el PLC 
        :return: lista de datos de tipo Float
        :rtype: list Float
        """ 
        output = []
        for i in range(quantity):
            offset_aux = offset + i*4
            real_bytes = data[offset_aux:offset_aux + 4]
            real_value = struct.unpack('>f', real_bytes)[0]
            output.append(real_value)
        return output
    
    def get_bytes_plc(self, db: int, offset: int, quantity_bytes: int):
        """
        Leer bytes de una DB del PLC.

        :param db: Número de la DB a leer.
        :param offset: Offset del byte desde donde se debe leer los datos.
        :param quantity bytes: Cantidad de datos que se desea leer.
        :return: Array de bytes leídos
        :rtype: bytearray
        """ 
        
        try:
            client = snap7.client.Client()
            client.connect(self.ip, self.rack, self.slot, self.port)
            data= client.db_read(db, offset, quantity_bytes)
            client.disconnect()
            return data
        except (Snap7Exception, Exception) as e:
            self.log(message=f'Error de conexión o lectura de datos del PLC: {e}')

    def format_bytes(self, array_bytes):
        formated_data = self.get_LReal(5, 0, array_bytes) + self.get_DInt(1, 40, array_bytes) + self.get_UInt(1, 44, array_bytes) + self.get_Byte(6, 46, array_bytes) + self.get_UInt(7, 52, array_bytes) +  self.get_Real(12, 66, array_bytes) + self.get_UInt(5, 114, array_bytes) + self.get_Byte(3, 124, array_bytes) + self.get_UInt(1, 128, array_bytes) + self.get_Real(2, 130, array_bytes) + self.get_Byte(3, 138, array_bytes) + self.get_UInt(1, 142, array_bytes) + self.get_Real(2, 144, array_bytes) + self.get_Byte(3, 152, array_bytes) + self.get_UInt(1, 156, array_bytes) + self.get_Real(2, 158, array_bytes) + self.get_Byte(3, 166, array_bytes) + self.get_UInt(1, 170, array_bytes) + self.get_Real(2, 172, array_bytes) + self.get_Byte(3, 180, array_bytes) + self.get_UInt(1, 184, array_bytes) + self.get_Real(2, 186, array_bytes) + self.get_Byte(3, 194, array_bytes) + self.get_UInt(1, 198, array_bytes) + self.get_Real(2, 200, array_bytes) + self.get_Byte(3, 208, array_bytes) + self.get_UInt(1, 212, array_bytes) + self.get_Real(2, 214, array_bytes) + self.get_Byte(3, 222, array_bytes) + self.get_UInt(1, 226, array_bytes) + self.get_Real(2, 228, array_bytes) + self.get_Byte(3, 236, array_bytes) + self.get_UInt(1, 240, array_bytes) + self.get_Real(2, 242, array_bytes) + self.get_Byte(3, 250, array_bytes) + self.get_UInt(1, 254, array_bytes) + self.get_Real(2, 256, array_bytes)

        return formated_data
         
    def log(self, message):
        with open(self.route_log, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([datetime.now(), message])

    def create_dictionary(self, data):
        my_dictionary = {}
        for i in range(len(self.variables)):
            key = self.variables[i]
            value = data[i]
            my_dictionary[key] = value
        return my_dictionary

    def api_post(self, data):


        headers = {
            'Authorization': f'Token {self.token}',
        }
        
        # Realizar la solicitud POST a la API
        try:
            response = requests.post(self.url_api, json=self.create_dictionary(data), headers=headers)
            # Verificar la respuesta
            if response.status_code == 201:
                if self.verbose:
                    self.log(message='data enviados con éxito. Código de respuesta: {response.status_code}, {response.text}')
                else: pass

            else:
                self.log(message=f'Error al enviar datos a través de la API. Código de respuesta: {response.status_code}, {response.text}')

        except (RequestException, Exception) as e:
            self.log(message=f'Error al intentar cargar datos a través de la API: {e}')

    def run(self):
        colombia_tz = pytz.timezone("America/Bogota")
        self.log(message='Inicio de ejecución de la ETL')
        while True:
            timestamp = datetime.now(colombia_tz)
            array_bytes = self.get_bytes_plc(db=self.db, offset=0, quantity_bytes=264)
            if array_bytes:
                data = [timestamp.strftime("%Y-%m-%dT%H:%M:%S%z")] + self.format_bytes(array_bytes=array_bytes)
                self.api_post(data=data)
            else:
                pass           
            sleep(self.period)
