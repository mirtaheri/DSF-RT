
'''
Copyright [2018] [ISMB]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''






'''_
simulation module is the core of (DSF-SE) framework. It coordinates other sub modules and handles the simulation run.

This module contains one class named SimulationManager that shares the simulation parameters with two subclasses named 
OffLineSimulator and RealTime Simulators. A user based on its case would exploit one or another.
For the analysis of distribution system OffLineSimulator is more convenient given that is 3 or 4 times faster than 
RealTimeSimulator, and the latter one is designed for the interactive scenarios like Hybrid Simulation.

'''
###_____________________________________________________________________________________________________________________

'''
Other modules are imported first of all
'''

import application_management.application as ext_app
import control_management.decision_maker as ctrl
import data_management.grid_DB as gDB
import data_management.DWH_connector as dwh
import data_management.monitor as mon
import data_management.api as api
from data_management.api import Connectors
from prediction_management import solar

import datetime as dt
import numpy as np
from time import *
import sys, os, shutil, pathlib
import paho.mqtt.client as mqtt
import requests

import threading
import json
import ast

# ---------------------------------------- #

NAME_LOAD = "ER_Load"
NAME_PV   = "ER_Pv"

# Received MQTT message ('0':P_GRID,'1':Q_GRID,'2':SOC_RATE,'3':SOC_V,'4':P_PV):
# MQTT FIXED POSITIONS:
MQTT_POS_PGRID    = '0'
MQTT_POS_QGRID    = '1'
MQTT_POS_SOC_RATE = '2'
MQTT_POS_SOC_V    = '3'
MQTT_POS_PV       = '4'

# Internal Buffer (Dictionary used as array-like)
POS_LOAD     = 0 # Position of Load inside the Local Buffer
POS_PV       = 1 # Position of PV inside the Local Buffer
POS_SOC_RATE = 2
POS_SOC_V    = 3
POS_PGRID    = 4
POS_QGRID    = 5
# 
FACTOR_PV = -1
INTERNAL_TOPIC = "INTERNAL/ER"
# BROKER_ADDR    = "localhost"
# ER MQTT BROKER ADDRESS FOR INPUTS
BROKER_ADDR    = "10.8.0.24"
BROKER_PORT    = 1883






#__________________________________________________________________________________________________________________________

#mqttlocalclient = mqtt.Client()
localbuffer = {} # Internal Buffer (Dictionary used as array-like)

def on_data_message(mqttlocalclient, obj, msg):
   # Here we receive messages containing array of values about ('0':P_GRID,'1':Q_GRID,'2':SOC_RATE,'3':SOC_V,'4':P_PV)
   # UPDATE the local buffer with received data (and a new value: P_LOAD, built based on the others)
   # By overwriting the previous values with the current ones (Avoid too fast solution leading to singularity)
   # payload = str(msg.payload)

   ###print("MESSAGE: " + str(msg.payload))
#   print("MESSAGE(type): " + str(type(msg.payload.decode())))

   try:
      # print("MESSAGE A: " + str(type(ast.literal_eval(msg.payload.decode() ) ) ))

      # converted = json.loads(ast.literal_eval(msg.payload.decode()))
      converted = json.loads(msg.payload.decode())
      ##print("MESSAGE A1")
      payload = dict(converted)
      ###print("MESSAGE A2")
      # (IF REQUIRED)
      # (The critical section will start here):
      #self.sem.acquire()
      # print("MESSAGE RECEIVED: " + str(payload['0']))
      ###print("MESSAGE RECEIVED2: " + str(payload["0"]))
      # Payload dictionary keys are fixed [Depends on S4G-DSF-Hybrid-Remote.py app]
      localbuffer[POS_PV]       = payload[MQTT_POS_PV]
      localbuffer[POS_PGRID]    = payload[MQTT_POS_PGRID]
      localbuffer[POS_QGRID]    = payload[MQTT_POS_QGRID]
      localbuffer[POS_SOC_RATE] = payload[MQTT_POS_SOC_RATE]
      localbuffer[POS_SOC_V]    = payload[MQTT_POS_PV] 
      # Load is the only value built starting from the others:
      
      localbuffer[POS_LOAD]     = buildLoad(localbuffer[POS_PGRID],localbuffer[POS_PV],localbuffer[POS_SOC_V]) 

      ###print("Buffer Updated")
   except Exception as e:
      print("On_DATA Error: %s" %e)

   #self.sem.release()

# ---------------------------------------------------
def startLocalSubscriber():
   try:
      mqttlocalclient.on_message    = on_data_message
      mqttlocalclient.on_connect    = on_data_connect
      mqttlocalclient.on_disconnect = on_data_disconnect
      mqttlocalclient.connect(BROKER_ADDR, int(BROKER_PORT), 60)
      
      # mqttlocalclient.subscribe(INTERNAL_TOPIC)
   except Exception as e:
      print("[SIM][MQTT-GLOBAL] Start Stopped %s" %e)

   print("MQTT client configured")
   mqttlocalclient.loop_start()     # This create a non-BLOCKING Thread!!
   
   print("MQTT client started")

# 2019/02/13
def on_data_disconnect(mqttlocalclient, obj, msg):

   try:
      mqttlocalclient.connect(BROKER_ADDR, int(BROKER_PORT), 60)
      
   except Exception as e:
      print("[SIM][MQTT-GLOBAL] Reconnect Stopped %s" %e)

# 2019/02/13
def on_data_connect(mqttlocalclient, userdata, flags , rc):

   print("MQTT client going to subscribe")
   mqttlocalclient.subscribe(INTERNAL_TOPIC)
   
   print("MQTT client subscribed: " + str(INTERNAL_TOPIC))
# ------------------------------------------------------------------------------------------------
# ER MQTT BROKER ADDRESS FOR CONTROL
#cntrl_mqtt_broker = "10.8.0.46" # Local Tests (ismb RPi)

cntrl_mqtt_broker = "10.8.0.11" # REAL ER (UPB)  
# AAAAA HERE

#cntrl_mqtt_broker = "10.8.0.24"
cntrl_mqtt_port   = 1883

selected_er_topic_start = "/DSF/SMX/EnergyRouterInverter/DRCC1.DERStr.ctlNum"

selected_er_topic_pv    = "/DSF/SMX/EnergyRouterPV/MMDC1.Watt.subMag.f"

selected_er_topic_ess   = "/DSF/SMX/EnergyRouterInverter/ZBTC1.BatChaPwr.setMag.f"

MQTT_pub_disconnected = True

mqtt_local_pub = mqtt.Client()


# ------------------------------------------------------------------------------------------------
def on_cntrl_disconnect(client, userdata, rc):

   global MQTT_pub_disconnected

   global cntrl_mqtt_broker

   global cntrl_mqtt_port

   MQTT_pub_disconnected = True


   while MQTT_pub_disconnected:

      time.sleep(10)

      try:

         if(int(mqtt_local_pub.connect(cntrl_mqtt_broker, cntrl_mqtt_port, 60)) == 0):

            MQTT_pub_disconnected = False

      except Exception as e:

         print("[MQTT-GLOBAL][CONTROL] Failure %s" %e)

         MQTT_pub_disconnected = True


# ------------------------------------------------------------------------------------------------

def startPublisher():
   
   global cntrl_mqtt_broker
   
   global cntrl_mqtt_port
   
   global mqtt_local_pub
   
   global MQTT_pub_disconnected

   while MQTT_pub_disconnected:
      
      try:
         
         mqtt_local_pub.on_disconnect = on_cntrl_disconnect
         
         if(int(mqtt_local_pub.connect(cntrl_mqtt_broker, int(cntrl_mqtt_port), 60)) == 0):
            
            MQTT_pub_disconnected = False
      
      except Exception as e:
         
         print("[MQTT-GLOBAL][CONTROL] Stopped %s " %e)
         
         MQTT_pub_disconnected = True
         time.sleep(1)

   mqtt_local_pub.loop_start()	

#__________________________________________________________________________________________________________________________


class SimulationManager(object):

    api: Connectors
    instance_counter = 0
    
    def __init__(self):
        '''
        The main parameters and functions of the simulation such as time, position and generally simulation setup
        are shared between both simulators within this class

        ATTENTION! some of the parameters below are there for the development phase and can be unused
        '''
        
        SimulationManager.instance_counter += 1
        
        self.directory = os.getcwd()
        
        self.user_id = None
        
        self.start, self.end, self.step_size, self.steps_total = None, None, 60, None
        
        self.current_simulation_time = None
        
        self.simulation_run_time = 0  # in seconds
        
        self.simulation_status = 'STOP'  # RESET, READY, GO, END
        
        self.simulation_timestamp_array = []
        
        self.lat, self.long = None, None
        
        self.irradiation_test = []
        
        self.section_parent = None
        
        self.grid_object, self.library_object, self.monitor_object, self.api = None, None, None, None
        
        self.app = None
        
        self.controller = None
        
        self.controllables = {}
        
        self.extraLoggerMsg = {'classname': self.__class__.__name__, 'controlelement': self.controlElement}
        
        self.formatter             = logging.Formatter('%(asctime)s {%(levelname)s}: [%(module)s]-->[%(classname)s], Client:%(client)s, PID:%(process)d, process(%(processName)s), Thread:%(thread)d, %(message)s')
      
        self.errosLoggingFile      = "logFiles/dsf_hybrid_simulator.log"

        self.logger   = self.getLoggerObject('hybrid simulator', self.executionLoggingFile, level=logging.DEBUG)
        
    def __del__(self):
        '''

        :return: it is as a TO DO reminder just to not forget the fact of instance destruction, but must be completed!!!
        '''
        print('\n',self.user_id, 'destructed')
        
        SimulationManager.instance_counter -= 1

    def set_simulation(self, start, end, step_size, lat, long, u, section=None, toEpoch=lambda x: x.timestamp()):
        '''
        essentially this function is the first one to be called by both simulators to set all simulation parameters
        :param start: start of the simulation period set by the user
        :param end: end of the simulation period set by the user
        :param step_size: simulation step size set by the user
        :param lat: simulation use case latitude
        :param long: simulation use case longitude. These geographical coordination and date and time of simulation
        are used for the solar irradiation calculation
        :param u: user id
        :param section: metadata of the section of interest to be simulated. (still to be defined how)
        :param toEpoch: a function to convert datetime format to epoch
        :return:
        '''
        
        self.datetime_start = start
        
        self.datetime_end = end
        
        self.start = toEpoch(start)
        
        self.end = toEpoch(end)
        
        self.step_size = step_size
        
        self.dtStart, self.dtStop = start, end
        
        self.steps_total = (self.end - self.start)/self.step_size
        
        self.lat, self.long = lat, long
        
        self.user_id = u
        
        if (section):
        
            self.gridMetadata(section)

        else:
            
            print('select the section of interest')

        '''
        INSTANCE CREATION OF OTHER CLASSES
        once simulation object has its main parameters set up, it create instance from other classes while passing 
        to them an instance of itself that contains the those parameters
        
        - grid_object(from DM)      -> to read data from DB (local folders) and write them in a OpenDSS-liked format
        - api (from DM)             -> to communicate with other modules and external services. 
        - app (from AM)             -> app is an instance of OpenDSSSolver which handles the interaction with the 
        OpenDSS 
        - library_object (from DM)  -> it handles the time series data to be used in simulation like calculates the pure
         solar irradiation curve for the period of simulation the it gets weather updates to calculate solar irradiation
          forecast based on cloud presence. Also load profiles are elaborated there.
        - monitor_object (from DM)  -> results of the simulation are recorded and shaped in a desired format
        - controller (from CM)      -> the intelligence of simulation will be in this class. One class could simply be 
        an optimization model or simply some rules of control.
        '''
        
        self.grid_object = gDB.GridInstance(self, self.section_parent)
        
        self.api = api.Connectors(self)
        
        self.api.getWeather()
        
        self.app = ext_app.OpenDSSSolver(self)
        
        self.app.write_irradiation_file()
        
        self.library_object = dwh.Templates(self)
        
        self.monitor_object = mon.Monitor(self)
        
        self.controller = ctrl.dumbControl(self)



    def gridMetadata(self, x_section):
        '''
        to deat with the grid portion selection of interest, now ha no functionality
        :param x_section: is a metadata indicating specific section from a table (TBD)
        :return:
        '''

        # TODO there must be a table like csv or xlsx from which encoded portion of grid can be find

        if x_section == 2:

            self.section_parent = 'feeder_613032_T01_02'

        else:

            self.sim.logger.info('Not still defined!', extra=self.extraLoggerMsg)


class OffLineSimulator(SimulationManager):

    def __init__(self):
        '''
        inherits from SimulationManager
        '''
        SimulationManager.__init__(self)



    def setMonitors(self):
        '''
        the way of monitoring is different based on use case, in offline case monitor objects must be written in
        OpenDSS format
        :return:
        '''
        self.monitor_object.set_monitor_offline()
        
        self.app.text_interface.Command = 'Redirect monitor.txt'



    def initialize_simulation(self):
        '''
        initialize simulation for offline case
        :return:
        '''
        self.setMonitors()
        
        self.current_simulation_time = self.start
        
        self.simulation_status = 'READY'



    def runSimulation(self):
        '''
        in offline simulation once the commands are written in an OpenDSS-like format, the simulation in run and
        returns the results in csv format in results folder
        :return:
        '''
        self.start_simulation_record = time()
        
        self.app.text_interface.Command = 'set mode=daily stepsize=' + (str(self.step_size/60)) +'h number=' + \
                                          str((self.end - self.start) / self.step_size)

        self.app.text_interface.Command = 'solve'
        
        self.monitor_object.exportMonitors()
        
        print('\n Simulation lasted ', time() - self.start_simulation_record, 'seconds')



class RealTimeSimulator(SimulationManager):

    def __init__(self):
        '''
        This simulator manages a real time simulation and also a hardware-in-loop simulation. A normal simulation can
        be run as well by this.
        '''
        SimulationManager.__init__(self)
        
        self.simulation_clock = 1
        
        self.start_simulation_record, self.start_cycle_record, self.start_flag = 0, 0, 0
        
        self.idle, self.total_execution_time = 0, 0
        


        self.MQTT_GLOBAL_DATA_TOPICS = None

        self.MQTT_GLOBAL_CONTROL_TOPICS = None

        self.url = "http://10.8.0.50:8080/v1.0/Sensors(xxx)/Datastreams"
        
        self.load_scale, self.pv_scale, self.sim_run = 1, 10, 0
        # 2019/02/13         
        localbuffer[POS_PV]       = 0
        
        localbuffer[POS_PGRID]    = 0
        
        localbuffer[POS_QGRID]    = 0
        
        localbuffer[POS_SOC_RATE] = 0
        
        localbuffer[POS_SOC_V]    = 0
        
        localbuffer[POS_LOAD]     = 0
        #print("MQTT STARTS")

    def initialize_simulation(self):
    
        self.current_simulation_time = self.start
        
        self.simulation_status = 'READY'
        
        #offset = self.api.getOff_set()  ### the daylight saving time in March and October must be handeled before
        
        offset = 3600 # it is up to user to set it based on local time. In future work is enough just to set the line above
        
        self.zero_ref = dt.datetime.combine(self.dtStart.date(), dt.time(0, 0)).timestamp() + offset
        
        self.current_simulation_time_dayNight = (self.dtStart.timestamp() - self.zero_ref) / self.step_size
        
        self.ess_controllables = [i for i, j in self.grid_object.all_elements['storages'].items() if j['control']['state'] == 'ON']
        
        #self.load_controllables = [i for i, j in self.grid_object.all_elements['loads'].items() if j['control']['state'] == 'ON']
        
        self.powerFlow() # this is once run just to make OpenDSS get know with the defined components, nothing is
        
        # written in monitors


    def runSimulation(self, idle_state):
        '''
        the simulation enters in a finite loop with the span equals to to simulation period.
        at each step updates the model of the grid according to the profiles of each object, writes the records and
        checks the condition and control algorithm.
        In case of hybrid simulation it calculates the consumed time at each step and then goes to idle state, so
        ynchronizes itself with the realtime
        :return:
        '''
        
        if self.simulation_status == 'READY' : self.simulation_status = 'GO'
        
        while self.simulation_status == 'GO':
            
            if not self.start_flag:
            
                self.start_flag = 1
                
                self.start_simulation_record = time()
            
            self.start_cycle_record = time()
            
            self.powerFlow()
            
            self.updateVariableElements_rt() 


            self.updateVariableElements()
            
            self.advanceClock()
            
            self.triggerMonitor()
            
            self.triggerControl()
            
            self.watchDog()
            
            self.check_external_interrupt()
            
            self.screen_handler()
            
            self.total_execution_time = time() - self.start_cycle_record
            
            self.idle = self.simulation_clock - self.total_execution_time
            
            #TODO: more refined for hybrid simulation case
            if (idle_state):
                
                sleep(self.idle)
            
            if (0):
                
                if self.current_simulation_time_dayNight % 15:
                    
                    machine_clock, world_clock = self.syncTime()
                
                weather = self.api.getWeather()


    
    def updateVariableElements(self):
    
        self.updateLoads(int(self.current_simulation_time_dayNight))
        
        self.updatePVs(int(self.current_simulation_time_dayNight))



    def updateLoads(self, current_simulation_time_dayNight):
        '''
        updates each single load based on load profile of that load
        :param current_simulation_time_dayNight: is the simulation time considering an span of 24 hours given that
        the profiles are in 24 hours in standard shape, so if user for instance sets the simulation period for mode
        than 24 hours, it can understand the starting and ending point of the profile.
        :return:
        '''
        
        for (key, value) in self.library_object.loads.items():
            
            self.app.dssCircuit.Loads.name = key
            
            self.app.dssCircuit.Loads.kW = self.library_object.loads[key]['profile'][key][current_simulation_time_dayNight]\
                                           * self.library_object.loads[key]['rated_power']


    def updatePVs(self, current_simulation_time_dayNight, temperatureEff = lambda x:(np.arange(100,50, -1)-1)[x]):
        '''

        :param current_simulation_time_dayNight: the same as for loads explained
        :param temperatureEff: it is kind of linear lookup table, given that the PV power is also a function of
        temperature. For sure to be refined.
        :return:
        '''
        
        for i in range(len(self.library_object.PVs['name'])):
            
            self.app.dssPVs.name = self.library_object.PVs['name'][i]
            
            scaled_irradiation = self.library_object.irradiation[current_simulation_time_dayNight]
                                 #/ self.library_object.max_absolute_irradiation
            
            T_ = self.library_object.T_profile[int(self.current_simulation_time_dayNight)]
            
            scaled_irradiation = scaled_irradiation * (temperatureEff(int(round(T_)))/100)
            
            self.app.dssPVs.Irradiance = scaled_irradiation
            
            self.irradiation_test.append(scaled_irradiation)
            
            

    def updateLoadsUnix(self, current_simulation_time_dayNight):
        '''
        updates each single load based on load profile of that load
        :param current_simulation_time_dayNight: is the simulation time considering an span of 24 hours given that
        the profiles are in 24 hours in standard shape, so if user for instance sets the simulation period for mode
        than 24 hours, it can understand the starting and ending point of the profile.
        :return:
        '''

        for (key, value) in self.library_object.loads.items():
        
            if key == self.hw_in_loop:
            
                self.updateLoads_rt()
            else:
            
                self.app.dssLoads.Name(key)
                
                self.app.dssLoads.kW(self.library_object.loads[key]['profile'][current_simulation_time_dayNight] \
                                     * self.library_object.loads[key]['rated_power'] * self.load_scale)

    def updatePVsUnix(self, current_simulation_time_dayNight, temperatureEff = lambda x:(np.arange(100,50, -1)-1)[x]):
        '''
        :param current_simulation_time_dayNight: the same as for loads explained
        :param temperatureEff: it is kind of linear lookup table, given that the PV power is also a function of
        temperature. For sure to be refined.
        :return:
        '''
        for i in range(len(self.library_object.PVs['name'])):
            '''
            FOR LINUX LIB DOES NOT WORK YET
            self.app.dssPVs.Name(self.library_object.PVs['name'][i])
            scaled_irradiation = self.library_object.irradiation[current_simulation_time_dayNight]
            T_ = self.library_object.T_profile[int(self.current_simulation_time_dayNight)]
            scaled_irradiation = scaled_irradiation * (temperatureEff(int(round(T_)))/100)
            '''
            if self.library_object.PVs['name'][i] == 'pv'+self.hw_in_loop:   
                
                self.updatePVs_rt()
            
            else:   
                
                self.app.dssLoads.Name(self.library_object.PVs['name'][i])
                
                scaled_irradiation = self.library_object.irradiation[current_simulation_time_dayNight]
                
                T_ = self.library_object.T_profile[self.sim_run]
                
                scaled_irradiation = scaled_irradiation * (temperatureEff(int(round(T_)))/100)
                
                self.app.dssLoads.kW(scaled_irradiation * self.library_object.PVs['kw'][i] * self.pv_scale)


    def triggerMonitor(self):
    
        self.monitor_object.write_voltages()
                
        HEADERS = {'content-type': 'application/json'}
        
        message = requests.get(url = self.url, headers = HEADERS)
        
        self.monitor_object.hybridSimulation(message.json())
        
        
    def triggerControl(self):
        self.controller.BESS_Control_execution()

    def powerFlow(self):
        self.app.dssSolution.solve()

    def advanceClock(self):
        '''
        it is in fact clock generator, but also checks the validity of simulation time in dayNight and simulation period
        :return:
        '''
        if self.current_simulation_time_dayNight % 900:
            pass
            #self.current_simulation_time = self.syncTime()
        
        if self.current_simulation_time < self.end:
            
            if self.simulation_status == 'GO':
                
                self.simulation_timestamp_array.append(
                    
                    strftime('%m/%d/%Y %H:%M:%S', gmtime(self.current_simulation_time)))
                
                self.current_simulation_time += self.step_size
                
                self.current_simulation_time_dayNight += 1
                
                if self.current_simulation_time_dayNight > self.steps_total - 1:
                    
                    self.current_simulation_time_dayNight = 0
        
        else:
            
            print('\n Simulation lasted ', time() - self.start_simulation_record, 'seconds')
            
            self.simulation_status = 'RESET'

    def watchDog(self):
        '''
        for realtime simulation if enabled, guarantees the duration of each step with the simulation clock
        :return:
        '''
        if self.idle > self.simulation_clock:
            
            self.simulation_status = 'RESET'
            
            raise ValueError('Mi dispiace, qualcosa è andata male :(')


    def syncTime(self):
        '''
        :return:
        '''
        return self.api.getNow_WC(), self.api.getNow_PC()


    def screen_handler(self):
        '''
        just writes the simulation process in the percentage and simulation virtual time on the screen
        :return:
        '''
        
        if self.simulation_status == 'GO':
        
            percent_sim = 1 - (self.end - self.current_simulation_time) / (self.end - self.start)
            
            time_msg = "\rSimulation run time: " + (self.simulation_timestamp_array[-1]) + "   Simulation progress: {0:.1%} ".format(percent_sim)
            
            sys.stdout.write(time_msg)
            
            sys.stdout.flush()



    def pause_simulation(self):
        '''
        TBD once the architecture is more clear
        :return:
        '''

        self.simulation_status = 'PAUSE'



    def resume_simulation(self):
        '''
        TBD once the architecture is more clear
        :return:
        '''
        self.simulation_status = 'GO'


    def check_external_interrupt(self):
        pass

    # TODO: reset simulation


    def get_simulation_timestamp(self):
        return strftime('%m/%d/%Y %H:%M:%S', gmtime(self.current_simulation_time))
        
        
    def internet_check():
        
        try:
            
            host = socket.gethostbyname('www.google.com')
            
            s = socket.create_connection((host, 80), 2)
            
            return True
        
        except:
            
            self.sim.logger.info('No network is available. Please check the internet connection first.', extra=self.extraLoggerMsg)

            return False     