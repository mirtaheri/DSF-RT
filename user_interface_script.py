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


from datetime import *
from time import *
import pytz as tz
import matplotlib.pylab as plt
import sys, os
import simulation_management.simulation as SE
plt.rcParams['figure.figsize'] = (16,6)

#SE.directory = os.getcwd()

### User  setting
SIMULATION_START = datetime(2017,8,18, 0, 0, 0, tzinfo=tz.utc)
SIMULATION_END =   datetime(2017,8,19, 0, 0, 0, tzinfo=tz.utc)
STEP_SIZE = 60
latitude, longitude = 46.497, 11.354
user = 'user_001234'
section_metadata = 2

if not SE.internet_check():
    raise ValueError('Please check the internet connection, it is necessary to proceed')

if(0):
    '''
    offline simulation instantiation
    '''
    sim_off = SE.OffLineSimulator()

    '''
    setting simulation parameters
    '''
    sim_off.set_simulation(SIMULATION_START, SIMULATION_END, STEP_SIZE, latitude, longitude, user, section_metadata)

    '''
    simulation initialization, sets the load flow parameters and switches the simulation states to READY
    '''
    sim_off.initialize_simulation()

    '''
    runs the simulation for the entire simulation period at once and writes the results in results folder
    '''
    sim_off.runSimulation()
    plt.plot(sim_off.library_object.irradiation)
    plt.show()
else:
    '''
    real-time simulation instantiation
    '''
    sim_real = SE.RealTimeSimulator()

    '''
    setting simulation parameters
    '''
    sim_real.set_simulation(SIMULATION_START, SIMULATION_END, STEP_SIZE, latitude, longitude, user, section_metadata)

    '''
    simulation initialization, sets the load flow parameters and switches the simulation states to READY
    '''
    #sim_off.grid_object.all_elements['loads']['load_130108_S']['monitor'] = {'monitoring':True, 'variables':['voltage', 'kW', 'kVA']}
    sim_real.initialize_simulation()

    '''
    runs the simulation for the entire simulation period at once and writes the results in results folder
    '''
    sim_real.runSimulation(True) ### False deactivates the realtime synchronization. True which is more intended for the hybrid simulation takes time

