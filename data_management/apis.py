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
This module will host the interfaces the external services such as third parties
two main connectors needed for Hybrid Simulation are:
    - World clock time: to get the offset between the target and control machine that can be remotly connected
    - Weather data: to calculate the PV production
    
'''


import requests, datetime
import numpy as np
import pandas as pd
import matplotlib.pylab as plt

class Connectors:
    '''
    this class is considered as the connectors, and will be the connector for the third parties developed so far
    '''

    def __init__(self, sim):
        '''
        there are third party keys for world clock and weather so far...
        :param sim:
        '''
        self.sync_now = None
        
        w_id, w_key = '39df55d0', 'afce27bf61cdfc4cdd3ae5b5281e39dc'
        
        self.time_api = 'http://api.timezonedb.com/v2/list-time-zone?key='+self.sim.KEY+'&format=json&country='+self.sim.ZONE+''
        
        self.weather_api = 'http://'+self.sim.THIRD_SERVICE_PROVIDER+'/api/forecast/'\
                           +str(sim.lat)+','+str(sim.long)+'?app_id='+w_id+'&app_key='+w_key
                          
        self.weather_forecast = self.getWeather()
        
        self.extraLoggerMsg = {'classname': self.__class__.__name__, 'controlelement': self.sim.controlElement}


    def getOff_set(self):
        '''
        gets time offset between the local (machine) time and world clock. For now was inactivated, first the daylight
        time change must be defined for it
        :return:
        '''
        try:
        
            resp = requests.get(self.time_api)

            self.sim.logger.info('Server replied with valid response', extra=self.extraLoggerMsg)
        
        except:
        
            self.sim.logger.error('Failed to get World Clock data because of {}'.format(error), extra=self.extraLoggerMsg)
        
        return resp.json()['zones'][0]['gmtOffset']

    def getNow_PC(self):
        '''
        gets the machine clock time
        :return:
        '''
        return datetime.datetime.now()

    def getNow_WC(self):
        '''
        gets the World Clock time
        
        :return:
        '''
        try:
            # request to the service provide for clock synchronization between target element to be controlled remotlly
            resp = requests.get(self.time_api)
            
            # check the response 
            if resp.status_code == 200:
                
                # calculated the time offset between cpu clock and world clock
                sync_time_now = resp.json()['zones'][0]['timestamp'] - resp.json()['zones'][0]['gmtOffset']
                
            self.sim.logger.info('Offset dataq has been calculated successfully', extra=self.extraLoggerMsg)
            
            return  datetime.datetime.fromtimestamp(sync_time_now).strftime('%Y-%m-%d %H:%M:%S'), sync_time_now
            
        except Exception as error:
        
            self.sim.logger.error('Failed to get World Clock data because of {}'.format(error), extra=self.extraLoggerMsg)

    def getWeather(self):
        '''
        make query to the weather data service provider
        :return: hourly cloud and temperature dict
        '''
        
        try:
            
            response = requests.get(self.weather_api)
            
            # in this model important indicators for the PV power are the temperature and density of the clouds             
            cloud_presence = []
            
            temperature = []
            # following lines of codes parse the format of specific 3rd party service provider which are packed into 7 days with steps of 3 hours
            
            for d in range(len(response.json()['Days'])):
                
                # the steps in each day are 3 hours long
                for h in range(len(response.json()['Days'][d]['Timeframes'])):
                    
                    # parse cloud density data
                    cloud_presence.append(response.json()['Days'][d]['Timeframes'][h]['cloud_mid_pct'])
                    
                    # parse temperature data
                    temperature.append(response.json()['Days'][d]['Timeframes'][h]['temp_c'])
            
            self.sim.logger.info('Data from 3rd party has been successfully retreived...', extra=self.extraLoggerMsg)
            
            # if the operation was succesfsul the data will be returned
            return pd.DataFrame(dict(cloud_presence = np.array(cloud_presence).repeat(3),
                                                      temperature = np.array(temperature).repeat(3)))
                                                      
        except Exception as error:
            
            self.sim.logger.error('Failed to fetch data since: {}'.format(error), extra=self.extraLoggerMsg)
