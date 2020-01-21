#!/usr/bin/python
# Copyright 2019 Ligios Michele <michele.ligios@linksfoundation.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''
Created on Feb 12, 2019
@author: ligios
@version: 1.1
'''

###############################################################################################################
# Script description (DSF-HYBRID-REMOTE):
# -------------------------------------------------------------------------------------------------------------
# Main steps:
# 0. Read a configuration file
# 1. Start the direct MQTT subscription towards the ER sensor from its Broker
# 2. Put on the internal queue each MQTT message received
# 3. Wait for all the group of messages and align them
# 4. Perform the mean of the received values in the groups of messages
# 5. Hamid algotithm
# 6. Send all the results produced towards the ER sensor exploiting its Broker
# -------------------------------------------------------------------------------------------------------------
# Notes:
# * By default it store error messages (only) into a dedicated log file generated inside the same folder 
#   (named: error_dsf_hybrid.log)
# * In case you are interested in debugging more deeply and you want to enable the entire log prints:
#   (You have to run the python script by adding the debugprint or debugFullprint argument)
#   (In this case, it will be generated a dedicated file called controlflow_dsf_hybrid.log with all the log prints)
# -------------------------------------------------------------------------------------------------------------
# The most precise approach to manage the retrieval of every message about ER sensed values should involve:
# TOPIC_MASK
# This because we should verify if every sample has been received concerning the known group of values
# The current approach take advantage of the known S4G scenario where these values are extracted:
# 1. Very fastly (About 10 ms distance between each sample)
# 2. Overall Rate (About 2000 ms distance between each group of sample)
# So, if we receive all the known values in a time-window of 1000 ms, it means that we received everything
# So, if we do not receive all the known values in a time-window of 1000 ms, it means:
# A) We are not receiveing a particular field
# B) We are trying to group values belonging to different groups (required an alignment, autonomously performed)
# -------------------------------------------------------------------------------------------------------------
import os
import sys
import socket
import requests
import paho.mqtt.client as mqtt
import json

if (sys.version_info > (3, 0)):
	import queue  
else:
	import Queue  

import logging
import configparser
import time
from datetime import datetime
import threading

# -------------------------------------------------------------------------------------------------------------
# MQTT TOPICS USED BY ER-SB-Connector to transmit realtime data:
PREFIX    = "/ER/SMX/EnergyRouterInverter/"
PREFIX_PV = "/ER/SMX/EnergyRouterPV/" 
# -------------------------------------------------------------------------------------------------------------
# Data Messages (MQTT TOPICS):
# -------------------------------------------------------------------------------------------------------------
P_GRID    = "MMXN1.Watt.instMag.f"
# Measurement Unit value: W

Q_GRID    = "MMXN1.VolAmpr.instMag.f"
# Measurement Unit value: VAr

SOC_RATE  = "ZBAT1.VolChgRte.instMag.f"
# Measurement Unit value: %

SOC_V     = "ZBAT1.Vol.instMag.f"
# Measurement Unit value: W

P_PV      = "MMDC1.Watt.instMag.f"
# Measurement Unit value: W
# -------------------------------------------------------------------------------------------------------------
OVERALL_MIN_LINES       = 3 # Define the group of values related to the minimum amount of data to start the MEAN   
# -------------------------------------------------------------------------------------------------------------
OVERALL_NUM_DATA_TOPICS = 5
SAMPLING_RATE           = 2
# In case you're intrested in defining a time frame instead of the overall amount of grouped samples,
# Rely on TIME_RATE
TIME_RATE               = SAMPLING_RATE*OVERALL_MIN_LINES
MSECONDS                = 1000       # Expressed in ms
# DISTANCE                = 1*MSECONDS # 1 second of distance allowed
DISTANCE                = 5*MSECONDS # 5 second of distance allowed
# -------------------------------------------------------------------------------------------------------------
# Control COMMANDS (MQTT TOPICS):
# -------------------------------------------------------------------------------------------------------------
DERStart  = "DRCC1.DERStr.ctlNum" 
# Mapping Measurement value:
# 0 = OFF
# 1 = Standby (Grid ON)
# 2 = RPPT (Grid ON)
# 3 = MPPT (Grid ON)
# 4 = PV OFF (Grid ON) 
# 5 = Standby (Grid OFF)
# 6 = RPPT (Grid OFF)
# 7 = MPPT (Grid OFF)
DERStop   = "DRCC1.DERStop.ctlNum"

P_BATREF  = "ZBTC1.BatChaPwr.setMag.f"
# Measurement Unit value: W

P_PVREF   = "MMDC1.Watt.subMag.f"
# Measurement Unit value: W

Q_GRIDREF = "MMXN1.VolAmpr.subMag.f"
# Measurement Unit value: VAr

# Format of messages ( value, measurement_unit, timestamp, topic )
# message = [{"v": 0, "u": "", "t":0,"n":""}]

# -------------------------------------------------------------------------------------------------------------
logErrorName   = "error_dsf_hybrid.log"
logFileName    = "controlflow_dsf_hybrid.log"
# -------------------------------------------------------------------------------------------------------------
mqtt_broker = ""
mqtt_port   = ""
cntrl_mqtt_port   = ""
cntrl_mqtt_broker = ""
# -------------------------------------------------------------------------------------------------------------
REVERSE_MAP            = {}
OVERALL_DATA_TOPICS    = {}
# -------------------------------------------------------------------------------------------------------------
# Variables used to selective log some behavior
debugprint        = False
# It is possible to use the following flag to give more flexibility to the loggin process
debugprintVerbose = False
# -------------------------------------------------------------------------------------------------------------
MQTT_sub_disconnected = True
MQTT_pub_disconnected = True
# -------------------------------------------------------------------------------------------------------------
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
controlflowlogger   = ""
debuglogger         = ""
# -------------------------------------------------------------------------------------------------------------
# Internal Queue Initialization
if (sys.version_info > (3, 0)):
	msg_queue = queue.Queue()
	msg_Internalaqueue = queue.Queue()
else:
	msg_queue = Queue.Queue() 
	msg_Internalaqueue = Queue.Queue()
# -------------------------------------------------------------------------------------------------------------
# Commands to update the PATH OS variable with the current directory:
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
# -------------------------------------------------------------------------------------------------------------
def main():
	global debugprintVerbose
	global debugprint
	global logFileName
	global logErrorName
	global debuglogger
	global controlflowlogger
	# ------------------------------- #
	global OVERALL_DATA_TOPICS
	global OVERALL_NUM_DATA_TOPICS
	global REVERSE_MAP
	# ------------------------------- #
	global mqtt_port
	global mqtt_broker
	# ------------------------------- #
	global cntrl_mqtt_port
	global cntrl_mqtt_broker
	# ------------------------------- #
	global sensor_name
	global sensor_location
	global sensor_company
	# ------------------------------- #

	currtime = datetime.utcnow()
	today = currtime.strftime("%d/%m/%Y")

	# First file logger ( Main Errors )
	debuglogger = setup_logger('first_logger', logErrorName)
	debuglogger.info('Error Flow about DSF Hybrid Tool')
	debuglogger.info("[DSF-HYBRID] S4G DSF Hybrid Tool Started")
	debuglogger.info("[DSF-HYBRID] Argument List:" + str(sys.argv))
	debuglogger.info("[DSF-HYBRID] Starting on: " + today)

	if(len(sys.argv) != 2 and len(sys.argv) != 3):
		print("[DSF-HYBRID] Arguments Error")
		print("[DSF-HYBRID] Examples: ")
		print("[DSF-HYBRID] python S4G-DSF-HYBRID.py configHybrid.properties")
		print("[DSF-HYBRID] python S4G-DSF-HYBRID.py configHybrid.properties debugprint")
		print("[DSF-HYBRID] python S4G-DSF-HYBRID.py configHybrid.properties debugfullprint")
		print("[DSF-HYBRID] debugprint option will enable full control flow prints on log file")
		return False

	# Following options to enable control flow logging
	if(len(sys.argv) == 3):
		if(sys.argv[2] == "debugprint"):
			debugprint = True
		elif(sys.argv[2] == "debugfullprint"):
			debugprint        = True
			debugprintVerbose = True
		else:
			print("[DSF-HYBRID] Arguments Error")
			print("[DSF-HYBRID] Examples: ")
			print("[DSF-HYBRID] python S4G-DSF-HYBRID.py")
			print("[DSF-HYBRID] python S4G-DSF-HYBRID.py config.properties debugprint")
			return False

	thing_name = socket.gethostname()
	config_file = sys.argv[1]

	# Second file logger ( Control Flow )
	if(debugprint == True):
		controlflowlogger = setup_logger('second_logger', logFileName)
		controlflowlogger.info('Control Flow about DSF Hybrid Tool')
		controlflowlogger.info("[DSF-HYBRID] Device: " + str(thing_name))		
		controlflowlogger.info("[DSF-HYBRID] S4G DSF Hybrid Tool Started")
		controlflowlogger.info("[DSF-HYBRID] Argument List:" + str(sys.argv))
		controlflowlogger.info("[DSF-HYBRID] Starting on: " + today)

	sensor_name        = ""

	time.sleep(1)
	# --------------------------------------------------------------------------------------------------------- #
	# PHASE: Local Configuration
	# --------------------------------------------------------------------------------------------------------- #
	# Read configuration File (exit if integrity not satisfied)
	if(debugprint == True):
		controlflowlogger.info("[DSF-HYBRID][MAIN] Read OGC configuration File (exit if integrity not satisfied)")
	try:

		config = configparser.ConfigParser()
		config.read(config_file)
		config.sections()		
		# ------------------------------------------------ #
		sensor_name       = config['SENSOR']['TYPE']
		sensor_location   = config['SENSOR']['LOCATION']
		sensor_company    = config['SENSOR']['COMPANY']
		# ------------------------------------------------ #
		# Remote Address because source is UPB ER
		mqtt_broker = config['MQTT_DATABROKER']['ADDR']
		mqtt_port   = config['MQTT_DATABROKER']['PORT']
		# ------------------------------------------------ #
		# Localhost because destination is Hamid Simulator App
		cntrl_mqtt_broker = config['MQTT_CONTROLBROKER']['ADDR']
		cntrl_mqtt_port   = config['MQTT_CONTROLBROKER']['PORT']
		# ------------------------------------------------ #
	except Exception as e:
		debuglogger.debug("[DSF-HYBRID][OGC] Configuration file Exception! %s not configured" %e)
		return config

	if(debugprint == True):
		controlflowlogger.info("[---------------------------------------------------------]")
		controlflowlogger.info("[DSF-HYBRID][MAIN] Values just read from configuration file: ")
		controlflowlogger.info("[SENSOR-NAME]:     " + str(sensor_name) )
		controlflowlogger.info("[SENSOR-LOCATION]: " + str(sensor_location) )
		controlflowlogger.info("[SENSOR-COMPANY]:  " + str(sensor_company) )
		controlflowlogger.info("[MQTT-DATABROKER-ADDR]: " + str(mqtt_broker) )
		controlflowlogger.info("[MQTT-DATABROKER-PORT]: " + str(mqtt_port) )
		controlflowlogger.info("[MQTT-CONTROLBROKER-ADDR]: " + str(cntrl_mqtt_broker) )
		controlflowlogger.info("[MQTT-CONTROLBROKER-PORT]: " + str(cntrl_mqtt_port) )
		controlflowlogger.info("[---------------------------------------------------------]")

	# ----------------------------------------------------------------- #
	# By following the above process, we assume that:
	# - we could have multiple ER sensors spread on the S4G Network
	# - consequently we have to select the ER of interest!
	# - SCRIPT-INPUT  = ER_1
	# - SCRIPT PERFORM SOME ELABORATIONS
	# - SCRIPT-OUTPUT = ER_1
	# Theoretically we will have:
	#   - one ER on Bucharest (UPB) 
	#   - one on Skiive (LIBAL)
	# ----------------------------------------------------------------- #
	# Fixed data topics:	
	OVERALL_DATA_TOPICS[0] = PREFIX + P_GRID
	OVERALL_DATA_TOPICS[1] = PREFIX + Q_GRID
	OVERALL_DATA_TOPICS[2] = PREFIX + SOC_RATE
	OVERALL_DATA_TOPICS[3] = PREFIX + SOC_V
	OVERALL_DATA_TOPICS[4] = PREFIX_PV + P_PV
	# ----------------------------------------------------------------- #
	z = 0
	while(z < OVERALL_NUM_DATA_TOPICS):
		REVERSE_MAP[OVERALL_DATA_TOPICS[z]] = z
		z += 1	
		
	# ----------------------------------------------------------------- #
	if(debugprint == True):
		controlflowlogger.info("[DSF-HYBRID][MAIN] Other initializations...")

	# DSF-HYBRID ---------------> DATA ---> SENSOR
	# We want to forward in real-time results to the sensor of interest
	startRemotePublisher()

	# SENSOR ---------------> DATA ---> DSF-HYBRID
	# We want to intercept ALL the messages in real-time coming from the sensor of interest
	startRemoteSubscriber()

	if(debugprint == True):
		controlflowlogger.info("[DSF-HYBRID][MAIN] Starting on:" + str(currtime))
	# --------------------------------------------------------------------------------------------------------- #
	debuglogger.info("[DSF-HYBRID][MAIN] Starting on:" + str(currtime))

	# Start the thread in charge of performing the Hybrid Algorithm
	hybridThreadActive = True
	hybridthread = HybridThread()
	hybridthread.start()  
	# --------------------------------------------------------------------------------------------------------- #

	# --------------------------------------------------------------------------------------------------------- #
	try:
		mainLoop()
	except KeyboardInterrupt:
		debuglogger.info("[MAIN][END] Interrupt intercepted")
		if(hybridThreadActive == True):
			debuglogger.info("[MAIN][END] Ending Aggregator Thread")
			hybridthread.kill_received = True
			hybridthread.join(5.0)
			debuglogger.info("[MAIN][END] Ended Aggregator Thread")
	# --------------------------------------------------------------------------------------------------------- #


######################################################################################################################################
def performAVG(msgs):
	global debugprint
	global controlflowlogger
	global debuglogger
	global OVERALL_DATA_TOPICS
	global OVERALL_NUM_DATA_TOPICS

	# Initialize AVG
	# -------------------------------------------------------------------------------------- #
	avg = {}
	z = 0
	while(z < OVERALL_NUM_DATA_TOPICS):
		avg[str(z)] = 0
		z += 1

	# Sum (coherently) every value received
	# -------------------------------------------------------------------------------------- #
	y = 0
	while(y < OVERALL_MIN_LINES):
		if(debugprint == True):
			controlflowlogger.info("[DSF-HYBRID] *** CurrentLine["+str(y)+"]")
			controlflowlogger.info(msgs[str(y)])

		currentline = msgs[str(y)]
		if(debugprint == True):
			controlflowlogger.info("TYPE: " + str(type(currentline)))
			controlflowlogger.info("Content: " + str(currentline[y]))


		for (key, value) in currentline:
			avg[str(key)] += value
			if(debugprint == True):
				controlflowlogger.info("[DSF-HYBRID] Key: " + str(key))
				controlflowlogger.info("[DSF-HYBRID] Value: " + str(value))
				controlflowlogger.info("[DSF-HYBRID] Average: " + str(avg[str(key)]))
		y += 1

	# -------------------------------------------------------------------------------------- #
	z = 0
	while(z < OVERALL_NUM_DATA_TOPICS):
		avg[str(z)] /= OVERALL_MIN_LINES
		if(debugprint == True):
			controlflowlogger.info("[DSF-HYBRID] AVG["+str(z)+"]="+str(avg[str(z)]))		
		z += 1
	# -------------------------------------------------------------------------------------- #
#	return avg
	return json.dumps(avg)
	
def mainLoop():
	global debugprint
	global msg_queue
	global MQTT_sub_disconnected
	global MQTT_pub_disconnected
	global controlflowlogger
	global debuglogger
	global OVERALL_DATA_TOPICS
	global OVERALL_NUM_DATA_TOPICS
	global DISTANCE
	global OVERALL_MIN_LINES
	global msg_Internalaqueue

	# Starting values:
	lines        = 0
	firstTime    = 0
	numMessages  = 1

	firstMessage = True
	overall_msg  = {}
	mytuple      = []


	while True:
		time.sleep(0.01) # Refresh rate (Configured manually)
		# [Be aware that the resulting rate must be lot more than the sampling rate]
		# ---------------------------------------------------------------------------------------------------- #
		# Make sense here to avoid any iteration ONLY if the publisher (towards the ER is not able to reach it)
		# ---------------------------------------------------------------------------------------------------- #
		if MQTT_pub_disconnected == True:
			time.sleep(0.5) # Sleep more to reduce the load on the CPU
		else:
			try:
				if msg_queue.empty() == False:
					# Extract from the queue both the target topic and the real message:
					position, topic , msg_json1 = msg_queue.get()
					msg_queue.task_done()
					if(debugprint == True):
						controlflowlogger.info("[DSF-HYBRID][mainLoop] Received Message [Len: "+str(len(msg_json1))+"]")
						controlflowlogger.info("[DSF-HYBRID][mainLoop] Received Message [Len: "+str(type(msg_json1))+"]")
						controlflowlogger.info("[DSF-HYBRID][mainLoop] Received Message [Len: "+str(msg_json1)+"]")
						controlflowlogger.info("[DSF-HYBRID][mainLoop] Selected Topic:  [" + str(topic) + "]")
						controlflowlogger.info("[DSF-HYBRID][mainLoop] Internal position:  [" + str(position) + "]")

					# Store received message as JSON 
					tmp_msg_json = json.loads(msg_json1)
					controlflowlogger.info("[DSF-HYBRID][mainLoop] Loaded")

					# msg_json = dict(tmp_msg_json['e'][0])
					msg_json = dict(tmp_msg_json[0])
					controlflowlogger.info("[DSF-HYBRID][mainLoop] PArsed")
					# msg_json = dict(msg_json)

					if(debugprint == True):
						controlflowlogger.info("[DSF-HYBRID][mainLoop] Message[Type]: "+str(type(msg_json)) +" [Content]: "+str(msg_json))


					try:
						#######################################################################################
						# We need to identify the first value to start the alignment with timestamps!
						# Otherwise the risk is to aggregate messages related to different samples (in time)!
						if( firstMessage == True ):
							if(debugprint == True):
								controlflowlogger.info("[DSF-HYBRID][mainLoop] FirstMessage Found: " +str(msg_json['t']))
							# 1. First time will be surely higher than 0
							# 2. Next times it will guarantee that new group is forward in time respect the previous group
							if(msg_json["t"] < firstTime):
								# We do not have a recovery strategy in this scenario: just notify it in the log file!
								debuglogger.debug("[DSF-HYBRID] Error: Base Time higher than received one:["+str(firstTime)+" > "+str(msg_json["t"])+"]")

							firstMessage           = False
							firstTime              = msg_json["t"]

							if(debugprint == True):
								controlflowlogger.info("[DSF-HYBRID][mainLoop] New BaseTime:  [" + str(firstTime) + "]")

							numMessages            = 1
							lines                  = 0
							mytuple = []
							mytuple.append((position,msg_json["v"]))

							overall_msg = {}
						else:
							if(debugprint == True):
								controlflowlogger.info("[DSF-HYBRID][mainLoop] Next Message Found ")

							# ----------------------------------------------------------------------------
							# Next line starts (need to update baseTime)
							if(numMessages == 0):
								if(debugprint == True):
									controlflowlogger.info("[DSF-HYBRID][Updating BaseTime]From:  [" + str(firstTime) + "]")
									controlflowlogger.info("[DSF-HYBRID][Updating BaseTime]To:  [" + str(msg_json["t"]) + "]")
								firstTime = msg_json["t"]

							if(debugprint == True):
								controlflowlogger.info("[DSF-HYBRID] Overall Messages:  [" + str(numMessages) + "]")
								controlflowlogger.info("[DSF-HYBRID] Current Line:      [" + str(lines) + "]")
								controlflowlogger.info("[DSF-HYBRID] Current BaseTime:  [" + str(firstTime) + "]")
								controlflowlogger.info("[DSF-HYBRID] Current Message:   [" + str(msg_json) + "]")
								controlflowlogger.info("[DSF-HYBRID] Overall Messages:  [" + str(overall_msg) + "]")

							numMessages           += 1
							mytuple.append((position,msg_json["v"]))

							if(debugprint == True):
								controlflowlogger.info("[DSF-HYBRID][mainLoop] DISTANCE:  [" + str(abs(msg_json["t"] - firstTime)/1000) + "]")

							# ISSUE Found in current implementation (consider all the messages aligned):
							# Group of messages arrives with misaligned timestamps
							# [e.g. first extracted value seems always the last one received inside a defined group]

							# Did we reached the last group value!?
							# increment only if we are sure that distance between firsTime and currentTime
							# if(numMessages == OVERALL_NUM_DATA_TOPICS and (firstTime <= msg_json["t"]) and abs(msg_json["t"] - firstTime) < DISTANCE):
							if(numMessages == OVERALL_NUM_DATA_TOPICS and abs(msg_json["t"] - firstTime) < DISTANCE):
								if(debugprint == True):
									controlflowlogger.info("[DSF-HYBRID][mainLoop] Line: " + str(lines))
									controlflowlogger.info("[DSF-HYBRID][mainLoop] New group: "+str(numMessages)+">="+str(OVERALL_NUM_DATA_TOPICS))

								# Store messages on the Main Map
								overall_msg[str(lines)]=mytuple

								# Flush messages collected until now
								mytuple      = []
								# -------------------------------------- #
								# We received every sample about current 
								# Group of values
								# -------------------------------------- #
								numMessages  = 0 
								lines        += 1							

							# elif((firstTime > msg_json["t"]) or abs(msg_json["t"] - firstTime) >= DISTANCE):
							elif(abs(msg_json["t"] - firstTime) >= DISTANCE):
								if(msg_json["t"] < firstTime):
									# We do not have a recovery strategy in this scenario: just notify it in the log file!
									debuglogger.debug("[DSF-HYBRID][RunTime] Error: Base Time higher than received one:["+str(firstTime)+" > "+str(msg_json["t"])+"]")

								if(debugprint == True):
									controlflowlogger.info("[DSF-HYBRID] Required Messages alignment!")
									controlflowlogger.info("[DSF-HYBRID] Base Time > MSG_Time (?)["+str(firstTime)+" > "+str(msg_json["t"])+"]?")
									controlflowlogger.info("[DSF-HYBRID][TimeDelta]: "+str(abs(msg_json["t"] - firstTime)))
								# -------------------------------------- #
								# We received part but... 
								# seems next group of values
								# -------------------------------------- #
								# We need to reset the current counters!
								# -------------------------------------- #
								firstTime    = msg_json["t"]
								numMessages  = 1
								lines        = 0

								# Flush messages collected until now
								overall_msg  = {}
								mytuple      = []
								# Overwrite with last tuple
								mytuple.append((position,msg_json["v"]))

						# #################### #################### #################### #################### #
						# ------------------------------------ #
						# Evaluate if we can aggregate!
						# Here we mixed values from a group!
						# ------------------------------------ #
						if(lines >= OVERALL_MIN_LINES):
							if(debugprint == True):
								controlflowlogger.info("[DSF-HYBRID][mainLoop] Line: " + str(lines))
								controlflowlogger.info("[DSF-HYBRID][mainLoop] Start the Mean")
								controlflowlogger.info("[DSF-HYBRID] Messages content:  [" + str(overall_msg) + "]")
							
							# Concerning the amount of Topics we reached 
							# the amount to perform AVG
							# ------------------------------------ #
							internalmsg = performAVG(overall_msg)
							# ------------------------------------ #
							# Send Mean values to the Main algorithm
							msg_Internalaqueue.put(internalmsg)
							# ------------------------------------ #
							# Then reset eveything
							# ------------------------------------ #
							lines        = 0
							numMessages  = 0
							overall_msg = {}
							firstMessage = True
							firstTime    = 0

					except Exception as e:
						debuglogger.debug("[DSF-HYBRID][mainLoop] Something Failed: %s" %e)
						if(debugprint == True):
							controlflowlogger.info("[DSF-HYBRID][mainLoop] Something Failed: %s" %e)

			except Exception as e:
				debuglogger.debug("[DSF-HYBRID][mainLoop] QUEUE Failed: %s" %e)
				if(debugprint == True):
					controlflowlogger.info("[DSF-HYBRID][mainLoop] Something Failed: %s" %e)


######################################################################################################################################
# Control COMMANDS (MQTT TOPICS):
# -------------------------------------------------------------------------------------------------------------
# DERStart  = "DRCC1.DERStr.ctlNum" 
# Mapping Measurement Unit value:
# 0 = OFF
# 1 = Standby (Grid ON)
# 2 = RPPT (Grid ON)
# 3 = MPPT (Grid ON)
# 4 = PV OFF (Grid ON) 
# 5 = Standby (Grid OFF)
# 6 = RPPT (Grid OFF)
# 7 = MPPT (Grid OFF)
# DERStop   = "DRCC1.DERStop.ctlNum"
# P_BATREF  = "ZBTC1.BatChaPwr.setMag.f"
# Measurement Unit value: W
# P_PVREF   = "MMDC1.Watt.subMag.f"
# Measurement Unit value: W
# Q_GRIDREF = "MMXN1.VolAmpr.subMag.f"
# Measurement Unit value: VAr
# -------------------------------------------------------------------------------------------------------------
class HybridThread(threading.Thread):
	def __init__(self):
		global debuglogger
		super(HybridThread, self).__init__()
		# ---------------------------------------------------------- #
		debuglogger.info("[HYBRID-THREAD] INIT on: " + str(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')))
		# ---------------------------------------------------------- #
		self.kill_received   = False
		# ---------------------------------------------------------- #

	# ---------------------------------------------------------------------------------------------------------- #
	def run(self):
		global debuglogger
		global controlflowlogger
		global debugprint
		global msg_Internalaqueue

		# ---------------------------------------------------------- #
		debuglogger.info("[HYBRID-THREAD] RUN")
		topicvalue           = ""
		selected_control_msg = ""
		single_msg           = ""
		# ---------------------------------------------------------- #
		if(debugprint == True):
			controlflowlogger.info("[HYBRID-THREAD] Start Run-Time behavior")

		# ---------------------------------------------------------- #
		while(True):
			if(self.kill_received == True):
				debuglogger.info("[HYBRID-THREAD] Kill Received")
				time.sleep(1)
				return

			if msg_Internalaqueue.empty() == True:
				# No Messages found (we can afford to sleep a bit longer)
				if(debugprint == True):
					controlflowlogger.info("[HYBRID-THREAD] EMPTY QUEUE: Longer Sleep")

				time.sleep(0.5)
			else:
				# ---------------------------------------------------------------------------------- #
				# Extract from the queue the message:
				single_msg = msg_Internalaqueue.get()
				# ---------------------------------------------------------------------------------- #
				msg_Internalaqueue.task_done()
				# ---------------------------------------------------------------------------------- #
				if(debugprint == True):
					controlflowlogger.info("[HYBRID-THREAD] Message Received, Running Algorithm:")
					controlflowlogger.info("[HYBRID-THREAD] Content: " + str(single_msg))

				# Send towards Hamid Simulator
				mqtt_local_pub.publish("INTERNAL/ER", str(single_msg))
				continue

				# ---------------------------------------------------------------------------------- #
				# Test Agorithm here
				# ---------------------------------------------------------------------------------- #
				topicvalue = 5
				#topicvalue, selected_control_msg = hamidFunction(single_msg)
				
				if(topicvalue == 0):
					# START				
					selected_er_topic = PREFIX + DERStart
				elif(topicvalue == 1):
					# STOP				
					selected_er_topic = PREFIX + DERStop
				elif(topicvalue == 2):
					# BATTERY				
					selected_er_topic = PREFIX + P_BATREF
				elif(topicvalue == 3):
					# PV
					selected_er_topic = PREFIX + P_PVREF
				elif(topicvalue == 4):
					# GRID
					selected_er_topic = PREFIX + Q_GRIDREF
				else:
					debuglogger.info("[HYBRID-THREAD] Wrong topic value:   " + str(topicvalue))
					debuglogger.info("[HYBRID-THREAD] About [RAW] message: " + str(single_msg))
					# debuglogger.info("[HYBRID-THREAD] About [RAW] message: " + str(selected_control_msg))
					debuglogger.info("[HYBRID-THREAD] Message [TYPE]: " + str(type(single_msg)))

					# TEMPORARY TEST
					mqtt_local_pub.publish("INTERNAL/ER", str(single_msg))
					time.sleep(2)
					continue
				# ---------------------------------------------------------------------------------- #
				# Publish results towards the ER
				# ---------------------------------------------------------------------------------- #
				#if(debugprint == True):
				#	controlflowlogger.info("[HYBRID-THREAD] Publishing Message...")
#
#				mqtt_local_pub.publish(selected_er_topic, selected_control_msg)
#
#				if(debugprint == True):
#					controlflowlogger.info("[HYBRID-THREAD] Next Message...")



######################################################################################################################################
mqtt_remote_sub    = mqtt.Client()

def on_data_message(mqtt_remote_sub, obj, msg):
	global debugprint
	global msg_queue
	global devices_mapping
	global controlflowlogger
	global debuglogger
	global REVERSE_MAP

	payload = str(msg.payload)
	if(debugprint == True):
		controlflowlogger.info("[DSF-HYBRID][MQTT-GLOBAL] " + str(msg.topic) + " Payload [" + str(len(msg.payload)) + "] -> " + str(payload))
		controlflowlogger.info("[DSF-HYBRID][MQTT-GLOBAL] Forwarding towards mainLoop exploiting internal queue!")

	if(len(msg.payload) > 0):
		try:
			# ---------------------------------------------------------------------------------- #	
			# Put inside the queue both the target topic and the real message to forward correctly:
			msg_queue.put((REVERSE_MAP[msg.topic],msg.topic,msg.payload.decode()))

		except Exception as e:
			debuglogger.debug("[DSF-HYBRID][MQTT-GLOBAL] What: %s " %e)
	else:
		if(debugprint == True):
			controlflowlogger.info("[DSF-HYBRID][MQTT-GLOBAL] Empty Message? ")
		debuglogger.debug("[DSF-HYBRID][MQTT-GLOBAL]  Empty Message? ")
	
def on_data_connect(mqtt_remote_sub, userdata, flags, rc):
	global debugprint
	global OVERALL_DATA_TOPICS
	global OVERALL_NUM_DATA_TOPICS
	global controlflowlogger
	
	z = 0
	while(z < OVERALL_NUM_DATA_TOPICS):
		if(debugprint == True):
			controlflowlogger.info("[DSF-HYBRID][MQTT-GLOBAL] connected, now subscribing... [" + str(OVERALL_DATA_TOPICS[z]) + "]")
		mqtt_remote_sub.subscribe(OVERALL_DATA_TOPICS[z])
		z += 1	


def on_data_disconnect(client, userdata, rc):
	global debugprint
	global mqtt_remote_sub
	global mqtt_broker 
	global mqtt_port
	global MQTT_sub_disconnected
	global controlflowlogger
	global debuglogger

	MQTT_sub_disconnected = True
	while MQTT_sub_disconnected:
		time.sleep(10)
		if(debugprint == True):
			controlflowlogger.info("[DSF-HYBRID][MQTT-GLOBAL] Re-connecting...")
		try:
			if(int(mqtt_remote_sub.connect(mqtt_broker, int(mqtt_port), 60)) == 0):
				MQTT_sub_disconnected = False
		except Exception as e:
			debuglogger.debug("[DSF-HYBRID][MQTT-GLOBAL] Failure %s " %e)
			MQTT_sub_disconnected = True


def startRemoteSubscriber():
	global debugprint
	global mqtt_remote_sub
	global mqtt_broker 
	global mqtt_port
	global MQTT_sub_disconnected
	global controlflowlogger
	global debuglogger

	if(debugprint == True):
		controlflowlogger.info("[DSF-HYBRID][MQTT-GLOBAL] Starting GLOBAL SUBSCRIBER (mqtt-client)")
		controlflowlogger.info("[DSF-HYBRID][MQTT-GLOBAL] MQTT: " + str(mqtt_broker)+":"+str(mqtt_port))

	while MQTT_sub_disconnected:
		try:
			if(debugprint == True):
				controlflowlogger.info("[DSF-HYBRID][MQTT-GLOBAL] Building the GLOBAL SUBSCRIBER (mqtt-client)")

			mqtt_remote_sub.on_message    = on_data_message
			mqtt_remote_sub.on_connect    = on_data_connect
			mqtt_remote_sub.on_disconnect = on_data_disconnect

			if(debugprint == True):
				controlflowlogger.info("[DSF-HYBRID][MQTT-GLOBAL] Connecting...")

			if(int(mqtt_remote_sub.connect(mqtt_broker, int(mqtt_port), 60)) == 0):
				MQTT_sub_disconnected = False

		except Exception as e:
			debuglogger.debug("[DSF-HYBRID][MQTT-GLOBAL] Stopped %s" %e)
			MQTT_sub_disconnected = True
			time.sleep(1)

	mqtt_remote_sub.loop_start()     # This create a non-BLOCKING Thread!!


#####################################################################################################################################
## MQTT Global Publisher
# Following methods will be used to forward (sensors) data from the SMX to the DWH
# --------------------------------------------------->[DWH]
mqtt_local_pub = mqtt.Client()

def on_cntrl_disconnect(client, userdata, rc):
	global MQTT_pub_disconnected
	global cntrl_mqtt_broker
	global cntrl_mqtt_port

	MQTT_pub_disconnected = True

	while MQTT_pub_disconnected:
		time.sleep(10)
		if(debugprint == True):
			controlflowlogger.info("[MQTT-GLOBAL][CONTROL] Re-connecting...")
		try:
			if(int(mqtt_local_pub.connect(cntrl_mqtt_broker, cntrl_mqtt_port, 60)) == 0):
				MQTT_pub_disconnected = False
		except Exception as e:
			debuglogger.debug("[MQTT-GLOBAL][CONTROL] Failure %s" %e)
			MQTT_pub_disconnected = True


def startRemotePublisher():
	global debugprint
	global cntrl_mqtt_broker
	global cntrl_mqtt_port
	global mqtt_local_pub
	global MQTT_pub_disconnected

	while MQTT_pub_disconnected:
		try:
			if(debugprint == True):
				controlflowlogger.info("[MQTT-GLOBAL][CONTROL] Building the GLOBAL PUBLISHER (mqtt-client)")
				controlflowlogger.info("[DSF-HYBRID][MQTT-GLOBAL] MQTT: " + str(cntrl_mqtt_broker)+":"+str(cntrl_mqtt_port))

			mqtt_local_pub.on_disconnect = on_cntrl_disconnect

			if(debugprint == True):			
				controlflowlogger.info("[MQTT-GLOBAL][CONTROL] Connecting...")

			if(int(mqtt_local_pub.connect(cntrl_mqtt_broker, int(cntrl_mqtt_port), 60)) == 0):
				MQTT_pub_disconnected = False

		except Exception as e:
			debuglogger.debug("[MQTT-GLOBAL][CONTROL] Stopped %s " %e)
			MQTT_pub_disconnected = True
			time.sleep(1)

	mqtt_local_pub.loop_start()			

#####################################################################################################################################

def setup_logger(name, log_file, level=logging.DEBUG):
    """Function setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


#####################################################################################################################################
if __name__ == '__main__':
	main()
