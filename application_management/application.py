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

'''
This module will host the interfaces with the external software such as distribution system analysis software 
'''
import win32com.client
import sys, os, glob, random

class OpenDSSSolver(object):
    '''
    Up to now the OpenDSS has been integrated to the framework, this class contains the interfaces with that software
    '''

    def __init__(self, sim, pf_solver = 'OpenDSS'):
        self.engine = win32com.client.Dispatch("OpenDSSEngine.DSS")
        self.engine.Start("0")
        self.engine.ClearAll()
        self.text_interface = self.engine.Text
        self.power_flow_engine = pf_solver
        self.sim = sim
        self.path = self.sim.directory + '\DB\components'
        self.filename = self.sim.user_id
        self.initOffLine()
        self.extraLoggerMsg = {'classname': self.__class__.__name__, 'controlelement': self.sim.controlElement}


    def activate_use_case(self, filename):
        '''
        NOT FUNCTIONAL!!!
        :param filename:
        :return:
        '''
        os.chdir(self.path+'\Alperia_test_feeder')
        self.text_interface.Command = "compile " + filename
        self.get_dss_objects()

    def initOffLine(self):
        '''
        creating an object from the class OpenDSSSolver, will be followed with a call to this function, this function
        first opens a COM interface and upon that a dss file as main file, the redirect those written files in
        OpenDSS-like format by DM (loads, PVs, lines, etc.), but in right dependency order presented in element_pririty
        :return:
        '''
        os.chdir(self.path)
        element_priority = ['Sources', 'Trafos', 'Irradiations', 'Linecodes', 'PV_absorb_effs', 'PV_temp_effs',\
                            'Temperatures', 'Lines', 'Loads', 'PVSystems', 'Storages']
        existing_elements = [os.path.splitext(j)[0] for j in glob.glob('*.txt')]
        self.get_dss_objects()
        f = open(self.filename + '.dss', 'a')
        self.text_interface.Command = 'clear'
        self.text_interface.Command = 'Redirect profiles/Loadshapes.txt'
        #self.text_interface.Command = 'Redirect profiles/irradiation_forecast.txt'

        try:
            for i in element_priority:
                if i in existing_elements:
                    self.text_interface.Command = 'Redirect ' + i + '.txt'
            self.text_interface.Command = 'set mode = snapshot'
            f.close()
            self.dssSolution.solve()    #### one time power-flow calculation must be run, so OpenDSS knows calculations converges or not 
            self.sim.logger.info('Simulation started successfully...', extra=self.extraLoggerMsg)
        except Exception as error:
            self.sim.logger.error('Failed to initialize because of {}'.format(error), extra=self.extraLoggerMsg)




    def get_dss_objects(self):
        '''
        gets OpenDSS various interfaces, however in the current platform the text interface is used mainly
        :return:
        '''
        try:
        
            self.dssCircuit = self.engine.ActiveCircuit
            self.dssSolution = self.dssCircuit.Solution
            self.dssCktElement = self.dssCircuit.ActiveCktElement
            self.dssBus = self.dssCircuit.ActiveBus
            self.dssMeters = self.dssCircuit.Meters
            self.dssPDElement = self.dssCircuit.PDElements
            self.dssLoads = self.dssCircuit.Loads
            self.dssLines = self.dssCircuit.Lines
            self.dssTransformers = self.dssCircuit.Transformers
            self.dssPVs = self.dssCircuit.PVSystems
            self.dssMonitors = self.dssCircuit.Monitors

            self.get_buses()
            self.get_pvs()
            self.sim.logger.info('All objects are mapped', extra=self.extraLoggerMsg)
        except Exception as error:
            self.sim.logger.error('Failed to get DSS objects since: {}'.format(error), extra=self.extraLoggerMsg)
        

    def write_irradiation_file(self):
        '''
        converts last updates to the OpenDSS convention
        '''
        f = open('Irradiations.txt', 'w')
        f.write('New Loadshape.irrad_area_001 ' + 'npts=' + str(self.sim.steps_total) + ' interval=1 mult=[File=irradiation_forecast.txt]')
        f.close()

    def get_buses(self):
        '''
        gets the name of buses from DSS CIRCUIT api
        '''
        self.AllBusNames = [self.dssCircuit.AllBusNames[i] for i in range(0, self.dssCircuit.NumBuses)]

    def get_loads(self):
        '''
        gets the name of Loads from DSS LOAD api
        '''
        self.all_model_loads = self.dssLoads.Allnames


    def get_pvs(self):
        '''
        gets the name of PVs from DSS PV api
        '''
        self.allPvNames = self.dssPVs.AllNames

    def get_voltages(self):
        '''
        gets the Voltages magnitude and angles for all buses
        '''
        try:
            voltages = []
            for i in self.AllBusNames:
                self.dssCircuit.SetActiveBus(i)
                puList = self.dssBus.puVmagAngle[0::2]
                voltages.append(puList)
            V_R, V_S, V_T = zip(*voltages)
            self.v_r, self.v_s, self.v_t = list(V_R), list(V_S), list(V_T)
            #self.sim.logger.info('Voltage of the buses are obtained', extra=self.extraLoggerMsg)

        except Exception as error:
            self.sim.logger.error('Failed to get Voltages, the problem was: {}'.format(error), extra=self.extraLoggerMsg)
        
        