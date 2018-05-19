#!/bin/python3
""" dispatch.py - Dispatch the flights read from a file input as an argument according to ERD """

import sys
import random
import datetime
from collections import deque

random.seed()

def get_first_available_time(term):
    """ return the conveyor that is available first """
    return term.index(min(terminal['conveyors']))

def get_baggage(plane_model):
    """ get the number of bags according to the airplane model """
    if plane_model == "Airbus" or plane_model == "Boeing":
        return 250 + random.randrange(-40, 40)

    if plane_model == "Cessna" or plane_model == "Embraer":
        return 100 + random.randrange(-20, 20)

    return 50 + random.randrange(-5, 5)

def parse_time(time):
    """ parse the time into an object """

    [hours_and_minutes, period] = time.split(' ')
    [hours, minutes] = hours_and_minutes.split(':')
    date = {}
    date['seconds'] = 0
    date['minutes'] = int(minutes)

    if period == 'am':
        if hours == '12':
            date['hours'] = 0
        else:
            date['hours'] = int(hours)

    else:
        if hours == '12':
            date['hours'] = 12
        else:
            date['hours'] = 12 + int(hours)

    return date



def read_flights(file):
    """ read the flights and place them in an array """

    flights_array = []

    # initialized with the maximum time in the future
    earliest_release_date = datetime.datetime.max

    # Read the header string from the file
    file.readline()

    for line in file:

        # each line has
        # id,date,estimated_arrival_time,scheduled_arrival_time,terminal,gate,baggage,plane
        line = line.strip().split(',')
        [year, month, day] = line[1].split('-')
        [year, month, day] = [int(year), int(month), int(day)]

        # terminal is not defined
        if line[4] == '-':
            continue

        read_flight = {}
        eat = parse_time(line[2])
        eat = datetime.datetime(year, month, day, eat['hours'], eat['minutes'], eat['seconds'])
        sat = parse_time(line[3])
        sat = datetime.datetime(year, month, day, sat['hours'], sat['minutes'], sat['seconds'])

        if earliest_release_date > eat:
            earliest_release_date = eat

        read_flight['code'] = line[0]
        read_flight['estimated_arrival_time'] = eat
        read_flight['scheduled_arrival_time'] = sat
        read_flight['terminal'] = line[4]
        read_flight['gate'] = line[5]
        read_flight['baggage'] = line[6]
        read_flight['plane'] = line[7]
        read_flight['size'] = get_baggage(line[7])


        flights_array.append(read_flight)

    return [flights_array, earliest_release_date]


JOBS = []
TERMINALS = [{} for i in range(0, 9)]


for terminal in TERMINALS:
    terminal['flight_queue'] = deque([])

INPUT_FILE = open(sys.argv[1], "r")
[FLIGHTS, EARLIEST_DATE] = read_flights(INPUT_FILE)

# set up conveyors to be available at the earliest release date possible
# set up conveyors in terminal 1 to 5
for i in range(1, 6):
    TERMINALS[i]['conveyors'] = [EARLIEST_DATE, EARLIEST_DATE, EARLIEST_DATE]

# set up the rest of the terminals
TERMINALS[7]['conveyors'] = [EARLIEST_DATE, EARLIEST_DATE, EARLIEST_DATE]
TERMINALS[8]['conveyors'] = [EARLIEST_DATE, EARLIEST_DATE]

for flight in FLIGHTS:
    flight_terminal = int(flight['terminal'])
    TERMINALS[flight_terminal]['flight_queue'].append(flight)

INPUT_FILE.close()
