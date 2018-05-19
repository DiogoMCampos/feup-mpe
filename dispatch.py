#!/bin/python3
""" dispatch.py - Dispatch the flights read from a file input as an argument according to ERD """

import sys
import random
from collections import deque

random.seed()

def get_first_available_time(term):
    """ return the conveyor that is available first """
    return term.index(min(terminal['conveyors']))

def get_baggage(plane_model):
    """ get the number of bags according to the airplane model """
    if plane_model == "Airbus" or plane_model == "Boeing":
        return 250 + random.random(-20, 20)

    if plane_model == "Cessna" or plane_model == "Embraer":
        return 100 + random.random(-10, 10)

    return 50 + random.random(-5, 5)

def read_flights(file):
    """ read the flights and place them in an array """

    flights_array = []

    # Read the header string from the file
    file.readline()

    for line in file:

        # each line has
        # id,date,estimated_arrival_time,scheduled_arrival_time,terminal,gate,baggage,plane
        line = line.strip().split(',')

        # terminal is not defined
        if line[4] == '-':
            continue

        read_flight = {}
        read_flight['code'] = line[0]
        read_flight['estimated_arrival_time'] = line[2]
        read_flight['scheduled_arrival_time'] = line[3]
        read_flight['terminal'] = line[4]
        read_flight['gate'] = line[5]
        read_flight['baggage'] = get_baggage(line[6])
        read_flight['plane'] = line[7]

        flights_array.append(read_flight)

    return flights_array


JOBS = []
TERMINALS = [{} for i in range(0, 9)]

# set up conveyors in terminal 1 to 5
for i in range(1, 6):
    TERMINALS[i]['conveyors'] = [0, 0, 0]

# set up the rest of the terminals
TERMINALS[7]['conveyors'] = [0, 0, 0]
TERMINALS[8]['conveyors'] = [0, 0]

for terminal in TERMINALS:
    terminal['flight_queue'] = deque([])

INPUT_FILE = open(sys.argv[1], "r")
FLIGHTS = read_flights(INPUT_FILE)

for flight in FLIGHTS:
    flight_terminal = int(flight['terminal'])
    TERMINALS[flight_terminal]['flight_queue'].append(flight)

for terminal in TERMINALS:
    print(terminal)
    for item in terminal['flight_queue']:
        print(item)

INPUT_FILE.close()
