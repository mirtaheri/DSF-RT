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
class SmartControl:

    def __init__(self):
        self.state = None
        self.method = None


class dummyControl:

    TARGET_TERMINAL  = 'bus_126468'
    
    def __init__(self, sim):
        self.sim_object = sim
        self.extraLoggerMsg = {'classname': self.__class__.__name__, 'controlelement': self.sim.controlElement}


    def BESS_Control_execution(self):
        for i in self.sim_object.ess_controllables:
            self.sim_object.grid_object.all_elements['storages'][i]

    def BESS_Control_execution(self):
        '''
        For sake of simplicity and test, no particular optimization logic has been adopted.
        The control is simply executed by using the voltage measurement out of the target metering device, then this it is tried to regulate this voltage with a simple role of the instruction woth the target physical device(Energy Router)
        The values as threshold to control Battery, have been obtained based on the test within specific hours, network and device. 
  
        '''
        lower_treshold = 190
            
        baseline = 230
        
        upper_treshold = 240
        
        min_voltage = np.min([self.monitor_object.TARGET_TERMINAL['R'][-1], self.monitor_object.TARGET_TERMINAL['S'][-1], self.monitor_object.TARGET_TERMINAL['T'][-1]])
        
        max_voltage = np.max([self.monitor_object.TARGET_TERMINAL['R'][-1], self.monitor_object.TARGET_TERMINAL['S'][-1], self.monitor_object.TARGET_TERMINAL['T'][-1]])

        # Evaluate current timestamp to fill in the ER control message
        tic = round(time())

        print("TRIGGER CONTROL: " + str(min_voltage) + " , " + str(max_voltage))

        if min_voltage < 220: #lower_treshold:
                
                if localbuffer[POS_SOC_RATE] > 0.1 and localbuffer[POS_SOC_RATE] <= 0.5:
                        
                        power_modulation = (baseline/min_voltage) *250 
                
                elif localbuffer[POS_SOC_RATE] > 0.5:
                        
                        power_modulation = (baseline/min_voltage) *500 
                
                else:
                        
                        power_modulation = 0
        
        elif min_voltage >= 220: #max_voltage > upper_treshold:
                
                if localbuffer[POS_SOC_RATE] >= 0.5 and localbuffer[POS_SOC_RATE] < 0.95:
                
                        power_modulation =  (max_voltage/baseline) *500 
                
                elif localbuffer[POS_SOC_RATE] < 0.5:      
                        
                        power_modulation = 1 * (max_voltage/230) *500 
                
                else: 
                        
                        power_modulation = 1 * (max_voltage/baseline) *500 
        
        else:
                
                power_modulation = 0          
        
        if abs(power_modulation) > 500:
                
                power_modulation = 500 * np.sign(power_modulation)
          
        elif abs(power_modulation) < 100:
                power_modulation = 100 * np.sign(power_modulation)
        
        self.sim.logger.info('max voltage is {} and min is {}  and power modulation is {} '.format(min_voltage, max_voltage, power_modulation), extra=self.extraLoggerMsg)

        #selected_control_msg = "[{\"v\": "+str(power_modulation)+", \"u\": \"W\", \"t\":"+str(tic)+",\"n\":\"PPvRef\"}]"
        #mqtt_local_pub.publish(selected_er_topic_pv, selected_control_msg) 

        selected_control_msg = "[{\"v\": "+str(power_modulation)+", \"u\": \"W\", \"t\":"+str(tic)+",\"n\":\"PBatRef\"}]"
        
        mqtt_local_pub.publish(selected_er_topic_ess, selected_control_msg) 




