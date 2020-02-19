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
GENERAL OBSERVATION:
In the current work, it is avoided to use a database since only one specific grid is subject to the simulations
therefore, in general many naming convention might be reffering to database concept while they use local repositories
'''



import sys, os, json, pandas, glob


class GridDB_Interface:
    '''
    Since there hasn't been specific DB for the grid, although the naming might refer to the databse, but all data regarding grid configuration are stored in local respositories
    And, some data are hard-coded.
    In the next version, an appropriate DB will be integrated
    '''
    DSO = 'XXX'
    
    
    def __init__(self):
    
        self.parent_substation = None
        
        self.children_substations = []
        
        self.children_feeders = []
        
        self.jsonGrid, self.json_o, self.all_elements = None, None, dict()
        
        self.area_of_interest, self.feeder_of_interest, self.sim_instance, self.user_instance = None, None, None, None

        self.extraLoggerMsg = {'classname': self.__class__.__name__, 'controlelement': self.sim.controlElement}


    def db_interface(self):
        '''
        up to the end of the project no DB is dedicated for this job
        Current parsers for the RT-simulator (Hybrid Simulator) are referring therefore to first versions of the grid definition 
        return:
        '''
        
        # TODO this must be MongoDB, for now is a local file which act as DB
        os.chdir('\OpenDSSIntegration\DB')
        
        with open('grid.json') as data_file:
            
            self.jsonGrid = json.load(data_file)
        
        self.sim_instance = self.jsonGrid['hv_mv_substation']
        
        if self.parent_substation:
            
            self.user_instance = self.jsonGrid['hv_mv_substation']['mv_substations']['parent_substation']
            
        if self.children_feeders:
           
            self.feeder_of_interest = self.children_feeders
        
        self.trim_grid()


    def trim_grid(self):
        pass


class GridInstance(GridDB_Interface):
    '''
    This class manages the GridDB
    '''
    def __init__(self, sim, where):
    
        GridDB_Interface.__init__(self)
        
        self.substation_parent = None; self.sim_instance = sim
        
        self.feeder_parent = None
        
        self.substation_children = None
        
        self.feeder_children = None
        
        self.jO = None
        
        self.readDB()
        
        self.where = where #TODO this will be used to identify the portion subject to simulation, but now the portion \
        #TODO in DB as default and has no effect in the following function



    def writeTransformers(self):
        '''
        a transformer can be 3 winding, so it requires apart form of definition
        :return:
        '''
        
        # the following codes are parsing the data from json to the DSS languge
        trafo_txt_file = open(os.path.join(os.getcwd() + '/components', 'Trafos' + '.txt'), 'w')
        
        
        trafo_txt_file.write('Var @default = 0.0' + "\n")
        
        for (k, v) in self.jO.items():
            
            tmp_var = 'New Transformer.' + v['tech_id'] + ' phases=' + str(v['phases']) + ' windings=' + str(
                v['windings']) + ' xhl=' + str(v['xhl'])
            
            trafo_txt_file.write(tmp_var + "\n")
            
            tmp_var = '~ wdg=1' + ' bus=' + v['bus1'] + ' kV=' + str(v['kv1']) + ' kVA=' + str(v['kva1']) + ' conn=' + \            
                v['conn1'] + ' %r=' + str(v['r1'])
            
            
            trafo_txt_file.write(tmp_var + "\n")
            
            tmp_var = '~ wdg=2' + ' bus=' + v['bus2'] + ' kV=' + str(v['kv2']) + ' kVA=' + str(v['kva2']) + ' conn=' + \
                      v['conn2'] + ' %r=' + str(v['r2'])
            
            if (v['windings'] > 2):
                
                trafo_txt_file.write(tmp_var + "\n")
                
                tmp_var = '~ wdg=3' + ' bus=' + v['bus3'] + ' kV=' + str(v['kv3']) + ' kVA=' + str(
                    v['kva3']) + ' conn=' + v['conn3'] + ' %r=' + str(v['r3'])
            
            trafo_txt_file.write(tmp_var + "\n")
        
        trafo_txt_file.close()
        # goes one directory back to parent one
        os.chdir('..')


    def writePV_absorb_effs(self):
        ''' 
        converts the PV absorbing look-up table to the DSS language
        '''
        default_array = ['npts', 'xarray', 'yarray']
        
        self.writeCommonElements('PV_absorb_eff', default_array, 'XYCurve')


    def writePV_temp_effs(self):
    
        default_array = ['npts', 'xarray', 'yarray']
    
        self.writeCommonElements('PV_temp_eff', default_array, 'XYCurve')
        
        
    def writeZone_irradiation(self):
        
        default_array = ['npts', 'interval', 'mult']
        
        self.writeCommonElements('Irradiation', default_array, 'Loadshape')


    def writeZone_temperature(self):

        default_array = ['npts', 'interval', 'temp']

        self.writeCommonElements('Temperature', default_array, 'TShape')


    def writeLinecodes(self):

        default_array = ['R1', 'X1', 'C0', 'Units']

        self.writeCommonElements('Linecode', default_array)


    def writeLoads(self):

        default_array = ['bus1', 'kV', 'kW', 'pf', 'phases', 'daily', 'conn', 'model']

        self.writeCommonElements('Load', default_array)



    def writeLoadshapes(self):

        default_array = ['npts', 'interval', 'mult']

        self.writeCommonElements('Loadshape', default_array)



    def writePVs(self):

        default_array = ['phases', 'bus1', 'kV', 'kVA', 'irrad', 'Pmpp', 'temperature', 'PF', 'effcurve',
                         'P-TCurve', 'Daily', 'TDaily']

        self.writeCommonElements('PVSystem', default_array)


    def writeLines(self):

        default_array = ['phases', 'bus1', 'bus2', 'length', 'linecode']

        self.writeCommonElements('Line', default_array)



    def writeStorages(self):

        default_array = ['phases', 'bus1', 'kV', 'kWrated', 'kWhrated', 'kWhstored',
                          'PF', 'dispmode']

        self.writeCommonElements('Storage', default_array)



    def writeSource(self):

        default_array = ['phases', 'bus1', 'Isc3', 'Isc1', 'basekv', 'pu']

        self.writeCommonElements('Source', default_array, 'Circuit')



    def writeCommonElements(self, element, attributes, name=None):
        '''
        just to reduce code
        :param element: this is the active element in jO object
        :param attributes: can be different from one component to another, so it is passed as vector
        :param name: in some cases name of element is different with the name of the object, e.g. loadshape
        :return:
        '''

        if not (name): name = element

        txt_file = open(os.path.join(os.getcwd() + '/components', element + 's.txt'), 'w')

        txt_file.write('Var @default = 0.0' + "\n")

        flex_step_elements = ['loadshape', 'irradiation', 'temperature']

        for (k, v) in self.jO.items():

            if 'npts' in next(iter(self.jO.values())):

                if element.lower() in flex_step_elements:

                    self.jO[k]['npts'] = self.sim_instance.steps_total

            tmp_var = 'New ' + name + '.'

            tmp_var += v['tech_id']

            for key in attributes:

                tmp_var += ' ' + key + '='

                if (v[key]):

                    tmp_var += str(v[key])

                else:

                    tmp_var += '@default'

            txt_file.write(tmp_var + "\n")

        txt_file.close()

        os.chdir('..')




    def readDB(self):
        '''
        once an object of this class is created by SM in consequence of a call by user, this function is called and then
        reads the gridDB (now a local folder), one by one, and writes the elements in OpenDSS-like format
        :return:
        '''

        my_dir = self.sim_instance.directory + '\DB'

        os.chdir(my_dir)

        self.elements = [os.path.basename(f) for f in glob.glob('*.json')]

        for element in self.elements:

            os.chdir(my_dir)

            with open(element) as data_file:

                self.jO = json.load(data_file)

                self.all_elements[os.path.splitext(element)[0]] = self.jO

                getattr(GridInstance, 'write' + element[0].upper() + os.path.splitext(element)[0][1:])(self)

        self.sim.logger.info('Grid Data has been completely converted and written to DSS language', extra=self.extraLoggerMsg)
