#!/bin/python3
""" dispatch.py - Dispatch the flights read from a file input as an argument according to ERD """

import sys
import queue


def get_first_available_time(terminal):
    """ return the conveyor that is available first """
    return terminal.index(min(terminal))

def read_flights(file):
    """ read the flights and place them in an array """

    flights_array = []

    # Read the header string from the file
    file.readline()

    for line in file:

        # each line has
        # id,date,estimated_arrival_time,scheduled_arrival_time,terminal,gate,baggage,plane
        line = line.strip().split(',')
        flight = {}
        flight['code'] = line[0]
        flight['estimated_arrival_time'] = line[2]
        flight['scheduled_arrival_time'] = line[3]
        flight['terminal'] = line[4]
        flight['gate'] = line[5]
        flight['baggage'] = line[6]
        flight['plane'] = line[7]

        print(flight)


    return flights_array



TERMINALS = []

# set up conveyors in terminal 1 to 5
for i in range(1, 6):
    TERMINALS[i] = [0, 0, 0]

# set up the rest of the terminals
TERMINALS[7] = [0, 0, 0]
TERMINALS[8] = [0, 0]

for terminal in TERMINALS:
    terminal['flight_queue'] = queue.Queue()

INPUT_FILE = open(sys.argv[1], "r")
FLIGHTS = read_flights(INPUT_FILE)

for flight in FLIGHTS:
    TERMINALS[flight['terminal']]['flight_queue'].put(flight)

print(TERMINALS)

INPUT_FILE.close()
