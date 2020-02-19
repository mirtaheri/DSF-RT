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


import os, glob
import pandas as pd
import matplotlib.pylab as plt
import random
import math
from datetime import *
from pysolar.solar import *
import pytz as tz
import numpy as np

class Templates:

    def __init__(self, sim):
        '''
        with the creation of instance from this class, a solar irradiance profile (normalized) is being created
        :param sim:
        '''
        # DSS object
        self.pf_solver = sim.app
        # simulator object
        self.sim = sim
        # the only logger handler
        self.extraLoggerMsg = {'classname': self.__class__.__name__, 'controlelement': self.sim.controlElement}

        self.model_loads = list(self.pf_solver.dssLoads.Allnames)
        self.loads = {}    #{'load_name': None, 'profile':[], 'rated_power': 0}
        self.load_dict = {}
        self.df_1_ph_monitored = {}
        #self.random_load_profiles()
        
        self.max_absolute_irradiation = self.solarIrradiation()
        
        self.scaled_irradiation = None
        
        self.PVs = self.solarProfile()
        
        self.T_profile = self.temperatureProfile()
        
        self.determined_load_profiles()
        
        # write the data into text files. This is one of the ways that DSS handles the interactions with its apis
        pd.DataFrame(self.irradiation).to_csv(self.sim.directory +'\DB\components\irradiation_forecast.txt', index= False, header=False)
        
        pd.DataFrame(self.T_profile).to_csv(self.sim.directory+'\DB\components\T.txt', index= False, header=False)


    def determined_load_profiles(self):
        '''
        associates the existing load profiles in the database(local files now) to the right load (user) in the system model
        :return:
        '''
        os.chdir('sq_rescaled_profiles/')
        # loops over all loads in the current simulation 
        for i, j in self.sim.grid_object.all_elements['loads'].items():
        
            name = self.sim.grid_object.all_elements['loads'][i]['tech_id']
            
            self.loads[name] = {'profile': None, 'rated_power': 0}
            
            file = self.sim.grid_object.all_elements['loadshapes'][j['daily']]['mult'][6:-1]
            
            profile = pd.read_csv(file, names=[name])
            
            self.loads[self.sim.grid_object.all_elements['loads'][i]['tech_id']]['profile'] = profile
            
            self.loads[self.sim.grid_object.all_elements['loads'][i]['tech_id']]['rated_power'] = j['kW']
            
            self.sim.logger.info('Profiles are loaded. PROFILES ARE ALREADY DETERMINED!', extra=self.extraLoggerMsg)
                        
        except Exception as error:
        
            self.sim.logger.error('Error occured: {}'.format(error), extra=self.extraLoggerMsg)


    def random_load_profiles(self):
        '''
        it is used when there were no load profile information from test sites and it randomly
        would associate the existing profiles in dataset randomly with the model loads
        :return:
        '''
        os.chdir(os.getcwd() + '\sq_rescaled_profiles')
        
        file = 'residential_profile.xlsx'
        # gets typical residential profile 
        xls = pd.ExcelFile(file)
        
        df_3_ph = xls.parse(xls.sheet_names[0])
        
        self.df_3_ph = df_3_ph['Working days'].repeat(15)
        
        self.profiles = [file for file in glob.glob('*.txt')]
        
        for i in self.model_loads:
            
            self.pf_solver.dssLoads.name = i
            
            self.load_dict[i] = self.pf_solver.dssLoads.kw
        
        self.load_profiles = [self.profiles[k] for k in random.sample(range(0, len(self.profiles)), len(self.model_loads))]
        
        self.import_load_profiles()
        

    def import_load_profiles(self):
        '''
        This function is used for the visual observation of the selected load profiles
        :return: plots all selected (randomly/specified) load profiles
        '''
        
        # tries to fit the plotting window to the range of the loads
        for i in range(len(self.load_profiles)):
        
            self.loads[self.model_loads[i]] = pd.read_csv(self.load_profiles[i], names=[self.load_profiles[i]])
        
        if len(self.model_loads) < 4:
            
            ncols = len(self.model_loads)
        
        elif len(self.model_loads) >= 1:
            
            ncols = 2
        
        else: ncols = 4
        
        nrows = round(math.ceil(len(self.load_profiles)/ncols))
        
        fig, axes = plt.subplots(figsize=(18,14), nrows=nrows, ncols=ncols)
        
        _index = 0
        
        for ax in axes.flat:
        
            
            ax.plot(self.loads[self.model_loads[_index]])
            
            ax.set_title(self.model_loads[_index].replace('.txt', ''), size=14)
            
            _index+=1
            
            if _index>=len(self.load_profiles):
                
                break
        if True:
        
            fig.tight_layout()
            
            plt.show()
            
            fig.savefig('1_ph_profiles.png')

    def solarIrradiation(self, default = False):
        '''
        This function calculates the maximum solar irradiation which coincides with June 21th at midday. This value
        will be used to normalize the power output of the PV panels.
        :param default: is to be set in case is used somewhere in southern hemisphere or somewhere else :D
        :return: a unitless value which is the maximum absolute irradiation for that specific coordination of simulation
        '''
        if (default == False):
        
            max_value = 66.94319268813125 # maximum irradiation on June 21th, applies this orientation angle
        else:
        
            
            start_epoch = self.sim.datetime_start
            
            end_epoch = self.sim.datetime_end
            
            step = 3600
            
            lat, lon = self.sim.lat, self.sim.long
            
            irradiation = []
            
            time_list = []
            
            while start_epoch < end_epoch:
                
                my_dt = datetime.datetime.fromtimestamp(start_epoch)
                
                irrad = get_altitude(lat, lon, my_dt)
                
                irradiation.append(irrad)
                
                time_list.append(my_dt)
                
                start_epoch += step
            
            max_value = max(irradiation)
            
            max_index = irradiation.index(max_value)
            
            max_time = time_list[max_index]

            start_epoch = max_time - datetime.timedelta(days=1)
            
            end_epoch = max_time + datetime.timedelta(days=1)
            
            start_epoch = start_epoch.timestamp()
            
            end_epoch = end_epoch.timestamp()
            
            step = 60
            
            irradiation = []
            
            time_list = []
            
            while start_epoch < end_epoch:
                
                my_dt = datetime.datetime.fromtimestamp((start_epoch))
                
                irrad = get_altitude(lat, lon, my_dt)
                
                irradiation.append(irrad)
                
                time_list.append(my_dt)
                
                start_epoch += step
            
            max_value = max(irradiation)
            
            max_index = irradiation.index(max_value)
        
        return (max_value)

    def solarProfile(self):
        '''
        once the maximum absolute irradiation (June 21 midday) is known, this function calculates the perfect
        irradiation for the period of simulation and again normalizes it wrt that maximum value.
        :return: set of photovoltaics in the test grid, and also updates the solar irradiation profile accordingly
        '''
        
        PVs = dict(name=[], kw=[])
        
        start_epoch = datetime.datetime.combine(datetime.datetime.fromtimestamp(self.sim.start).date(),
                                                                                    datetime.time(0, 0)).timestamp()
        counter = 0
        
        step = 60
        
        irradiation, time_list = [], []
        
        while counter < 1440:
            
            my_dt = datetime.datetime.fromtimestamp(start_epoch)
            
            irrad = get_altitude(self.sim.lat, self.sim.long, my_dt)
            
            irradiation.append(irrad)
            
            time_list.append(my_dt)
            
            counter += 1
            
            start_epoch += step
        
        self.irradiation = np.array(irradiation); max_sim_time = np.max(self.irradiation)
        
        self.irradiation[np.where(self.irradiation < 0)] = 0
        
        self.irradiation = self.add_sun_Noise()
        
        self.irradiation = self.irradiation * (max_sim_time / self.max_absolute_irradiation)
        
        PVs['name'] = self.pf_solver.dssPVs.AllNames
        
        for i in range(len(PVs['name'])):
            
            self.pf_solver.dssPVs.name = PVs['name'][i]
            
            PVs['kw'].append(self.pf_solver.dssPVs.kw)
        
        return (PVs)


    def add_sun_Noise(self):
        '''
        once the perfect solar irradiation is known from the solarProfile function, this function is being called to
        add noise to that perfect profile. The noise is an interpretation of cloud presence forecast got from third
        :return: resulting_irradiation is the final shape for the irradiation profile, very close to the reality
        '''
        #TODO ToBeHandled: the weather forecast is available for the comnig days not for simulations in the past or in future
        
        noise_array = np.zeros(len(self.irradiation))
        
        resulting_noise = np.zeros(len(self.irradiation))
        
        irradiation = self.irradiation / max(self.irradiation)
        
        clouds = self.sim.api.weather_forecast['cloud_presence'].values.repeat(3600 / self.sim.step_size)#[:1440]
        
        scaled_array = clouds/100
        
        for i in range(len(self.irradiation)):
            
            noise_level = np.random.normal(loc = clouds[i])
            
            noise_array[i] = np.random.choice(2, 1, p=[1-scaled_array[i], scaled_array[i]])[0]
            
            resulting_noise[i] =  np.random.normal((noise_level/100), 1)
        
        resulting_noise = np.clip((resulting_noise * noise_array), np.zeros(len(irradiation)), irradiation)
        
        resulting_irradiation = np.clip(irradiation - resulting_noise, 0, 1)
        
        return self.smoothArray(resulting_irradiation)



    def temperatureProfile(self):
        '''
        cuts down the available forecast for the period of simulation
        :return: temperature profile with slots equal to siimulation step size and window span equal to simualtion duration
        '''
        return self.sim.api.weather_forecast['temperature'].values.repeat(3600 / self.sim.step_size)[:int(self.sim.steps_total)]

    def smoothArray(self, array):
        
        tau_multiplier = 7
        
        rectangle = np.ones(tau_multiplier) / tau_multiplier
        
        return np.convolve(array, rectangle, mode='same')




class Users:

    def __init__(self, user_id, sim):
        """
        :param user_id:
        :param app:
        :param sim:
        this must be adapted and modified with DWHAdapter
        """
        
        self.user = user_id
        
        self.pf_app = sim.app
        
        self.sim_app = sim
        
        self.Name = None
        
        self.df_1_ph = {}
        
        self.load_dict = {}


    
    def rewriteLoadProfile(self):
        
        def reduceWindow(origin):
            
            new_array = []
            
            i=j=k=temp_var=0
            
            while (i<len(origin.values)):
                
                j=temp_var=k=0
                
                # 15 must be adapted with the way the data is being received from user side
                while(j<15):
                    
                    temp_var+=origin.values[i][0]
                    
                    i += 1
                    
                    j += 1
                
                while(k<1):
                    
                    new_array.append(temp_var/15)
                    
                    k+=1
            
            return new_array

        def makeSmooth(y, box_pts):
            
            box = np.ones(box_pts)/box_pts
            
            y_smooth = np.convolve(y, box, mode='same')
            
            return y_smooth

        os.chdir('profiles\P')
        # TODO: this directores must be DB indeed

        p_profiles = [file for file in glob.glob('*.txt')]
        
        for i in p_profiles:
            
            os.chdir('profiles\P')
            
            original_profile = pd.read_csv(i, header=None)
            
            reduced_array = reduceWindow(original_profile)
            
            smooth_array = makeSmooth(reduced_array, 3)

            os.chdir('profiles\hse_db\P')

            with open(i,'w') as f:
                
                for step in smooth_array:
                    
                    f.write(str(step) + "\n")

    