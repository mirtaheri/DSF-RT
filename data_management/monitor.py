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



import pandas as pd
import os
from time import *

class Monitor:
    '''
    monitoring all events is made by triggering relevant function call from this class. for now, in development phase
    only the voltages are being monitored.
    Upon the creation of this class an object for each element to be monitored is being created
    '''
    def __init__(self, sim):

        self.sim = sim

        self.phases = ['R','S','T']

        self.Timestamp = []

        for i in self.sim.app.dssCircuit.AllBusNames:

            setattr(self, 'bus_'+i, dict(state = None, R = [], S = [], T=[],address = None))

        self.extraLoggerMsg = {'classname': self.__class__.__name__, 'controlelement': self.sim.controlElement}



    def writeMeasurements(self):

        self.write_voltages()
        
        if self.sim.LOGG_PER_STEP == True:
        
            self.sim.logger.info('Voltages are retreived...', extra=self.extraLoggerMsg)

        # TODO other variable such as voltage angle, losses, kVA, .... must come here
        '''
        related to RealTimeSimulator
        :return: 
        '''

    def write_voltages(self):

        for i in range(len(self.sim.app.dssCircuit.AllBusNames)):

            self.sim.app.dssCircuit.SetActiveBus(self.sim.app.dssCircuit.AllBusNames[i])

            puList = self.sim.app.dssBus.puVmagAngle[0::2]

            for j in range(len(self.phases)):

                getattr(self, 'bus_' + self.sim.app.dssCircuit.AllBusNames[i])[self.phases[j]].append(puList[j])

        self.Timestamp.append(self.sim.simulation_timestamp_array[-1])



    def read_voltages(self, control_nodes):
        '''
        related to RealTimeSimulator
        :return:
        '''

        if type(control_nodes) != list:

            control_nodes = control_nodes.split()

        voltages = {}

        for key in control_nodes:

            for i in range(len(self.phases)):

                voltages[key+self.phases[i]] = getattr(self, 'bus_' + key)[self.phases[i]]

        return pd.DataFrame(voltages, index = self.Timestamp)



    def set_element_monitor(self):
        '''
        related to OffLineSimulator
        :return:
        '''

        self.monitored = []

        os.chdir('OpenDSSIntegration\DB\components\monitors')

        txt_ = open('monitor.dss', 'w')

        for i in self.sim.grid_object.all_elements.keys():

            for k, v in self.sim.grid_object.all_elements[i].items():

                if 'monitor' in self.sim.grid_object.all_elements[i][k].keys():

                    if v['monitor']['state']:

                        for var in v['monitor']['var']:

                            self.monitored.append(k + '_' + var)

                            tmp_var = 'New monitor.' + k + '_' + var + ' element=' + \
                                      i[:-1] + '.' + self.sim.grid_object.all_elements[i][k]['tech_id']

                            if var == 'voltage': tmp_var += ' mode=0'


                            elif var == 'kW': tmp_var += ' mode=1'

                            elif var == 'kVA': tmp_var += ' mode=1'

                            elif var == 'loss': tmp_var += ' mode=3'

                            else: tmp_var += ' mode=3'

                            txt_.write(tmp_var + '\n')

        txt_.close()




    def set_monitor_offline(self):
        '''
        related to OffLineSimulator
        :return:
        '''
        
        directory = self.sim.directory + '\DB\components\monitors'
        
        if not os.path.exists(directory):
        
            os.makedirs(directory)
        
        self.set_element_monitor()


    def exportMonitors(self):
        '''
        related to OffLineSimulator
        :return:
        '''
        
        self.sim.app.engine.Datapath = self.sim.directory + '/results'
        
        for m in self.monitored:
            
            self.sim.app.text_interface.Command = 'Export mon ' + m
            
            
            
            
            
            