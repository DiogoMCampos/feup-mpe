#!/bin/python3
""" dispatch.py - Dispatch the flights read from a file input as an argument according to ERD """

import sys
import random
import datetime
import functools
import heapq
import queue

random.seed()

QUEUE_SIZE = 10

@functools.total_ordering
class Flight:
    """ represents a flight read from the file """

    def __init__(self, plane_dict):
        pd_eat = plane_dict['estimated_arrival_time']
        pd_sat = plane_dict['scheduled_arrival_time']

        self.code = plane_dict['code']
        self.estimated_arrival_time = pd_eat if pd_eat else pd_sat
        self.terminal = plane_dict['terminal']
        self.gate = plane_dict['gate']
        self.baggage = plane_dict['baggage']
        self.plane = plane_dict['plane']
        self.size = plane_dict['size']

    def __str__(self):
        return self.__dict__

    def __lt__(self, other):
        return self.estimated_arrival_time < other.estimated_arrival_time

    def __eq__(self, other):
        return self.estimated_arrival_time == other.estimated_arrival_time


class Job:
    """ represents a job allocating a flight to a conveyor """

    def __init__(self, plane_dict):
        if plane_dict:
            self.empty = False
            self.code = plane_dict['code']
            self.terminal = plane_dict['terminal']
            self.release_date = plane_dict['release_date']
            self.size = plane_dict['size']
            self.conveyor = plane_dict['conveyor']
            self.start_time = plane_dict['start_time']
            self.flow_time = plane_dict['flow_time']
            self.end_time = self.start_time + self.size
        else:
            self.empty = True

    def print_job(self):
        if self.empty:
            print('empty job')
        else:
            print('code: {}'.format(self.code))
            print('size: {}'.format(self.size))
            print('release date: {}'.format(self.release_date))
            print('start time: {}'.format(self.start_time))
            print('terminal: {}'.format(self.terminal))
            print('conveyor: {}'.format(self.conveyor))
            print('flow time: {}'.format(self.flow_time))
            print('end time: {}'.format(self.end_time))

    def __lt__(self, other):
        return self.start_time < self.start_time


def get_available_date(conveyor):
    """ return earliest available date for specific conveyor """
    if not conveyor:
        return datetime.datetime.min
    else:
        return conveyor[-1].end_time


def add_job_to_conveyor(job, conveyor):
    conveyor.append(job)
    conveyor.sort()


def get_earliest_available_conveyor(term):
    """ return the conveyor that is available first """
    earliest_dates = [get_available_date(c) for c in term['conveyors']]
    return earliest_dates.index(min(earliest_dates))


def get_job_size(plane_model):
    """ get the duration of the job according to the airplane model """
    # around 250 people, ~30 minutes
    if plane_model == "Airbus" or plane_model == "Boeing":
        return datetime.timedelta(minutes=30 + random.randrange(-15, 15))

    # around 100 people, ~15 minutes
    if plane_model == "Cessna" or plane_model == "Embraer":
        return datetime.timedelta(minutes=15 + random.randrange(-10, 10))

    # other planes, <= 50 people, 5 minutes
    return datetime.timedelta(minutes=5 + random.randrange(-5, 5))


def parse_time(time):
    """ parse the time into an object """

    [hours_and_minutes, period] = time.split(' ')
    [hours, minutes] = hours_and_minutes.split(':')

    date = {}
    date['minutes'] = int(minutes)

    # Convert 12 hour to 24 hour format
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
    # variable used to store the absolute earliest release date
    # to be used to set up the conveyors
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
        eat = datetime.datetime(year, month, day, eat['hours'], eat['minutes'])
        sat = parse_time(line[3])
        sat = datetime.datetime(year, month, day, sat['hours'], sat['minutes'])

        if earliest_release_date > eat:
            earliest_release_date = eat

        read_flight['code'] = line[0]
        read_flight['estimated_arrival_time'] = eat
        read_flight['scheduled_arrival_time'] = sat
        read_flight['terminal'] = line[4]
        read_flight['gate'] = line[5]
        read_flight['baggage'] = line[6]
        read_flight['plane'] = line[7]
        read_flight['size'] = get_job_size(line[7])

        flights_array.append(Flight(read_flight))

    return [flights_array, earliest_release_date]


def is_best(job, comparable):
    """ returns true if job has lower flow time (is best) than comparable """
    return job['flow_time'] < comparable['flow_time']


def generate_new_neighbor_list(current_state):
    neighbor_list = []
    num_jobs = len(current_state)

    for i in range(0, num_jobs - 1):
        left_job = current_state[i]
        right_job = current_state[i + 1]
        left_job['conveyor'],  right_job['conveyor'] = right_job['conveyor'], left_job['conveyor']
        # TODO manage time, probably clreate class Conveyor
        # remove old start time and duration from conveyon
        # check start time and duration
        # if impossible, move to best possible start_time

    return neighbor_list


def get_best_state(state_list):
    best_state = None
    for state in state_list:
        if is_best(state, best_state):
            best_state = state

    return best_state


def generate_new_non_tabu_neighbor(current_state, tabu_list):
    """ get best non tabu neighbor given current state and tabu list """
    neighbor_list = generate_new_neighbor_list(current_state)
    non_tabu_neighbor_list = [neighbor for neighbor in neighbor_list
                              if neighbor not in tabu_list]

    new_state = get_best_state(non_tabu_neighbor_list)

    return new_state


def tabu_search(initial_state, max_iterations):
    """ run tabu_search on initial_state for a max of max_iterations """
    current_state = initial_state
    best_state = initial_state
    tabu_list = Queue.Queue(maxsize=QUEUE_SIZE)

    # TODO additional stopping criteria
    for i in range(max_iterations):
        candidate = generate_new_non_tabu_neighbor(current_state, tabu_list)

        if tabu_list.full():
            tabu_list.get()
        tabu_list.put(s)

        if is_best(candidate, best_state):
            best_state = candidate

        current_state = candidate

    return current_state


TERMINALS = [{} for i in range(0, 9)]

for terminal in TERMINALS:
    terminal['flight_queue'] = []

INPUT_FILE = open(sys.argv[1], "r")
[FLIGHTS, EARLIEST_DATE] = read_flights(INPUT_FILE)
INPUT_FILE.close()

# set up conveyors to be available at the earliest release date possible
# set up conveyors in terminal 1 to 5
for i in range(1, 6):
    TERMINALS[i]['conveyors'] = [list(), list(), list()]

# set up the rest of the terminals
TERMINALS[7]['conveyors'] = [list(), list(), list()]
TERMINALS[8]['conveyors'] = [list(), list()]

# place the flights in the appropriate terminal queues
for flight in FLIGHTS:
    flight_terminal = int(flight.terminal)
    TERMINALS[flight_terminal]['flight_queue'].append(flight)

# create the priority queue
for terminal in TERMINALS:
    heapq.heapify(terminal['flight_queue'])

# where the scheduling will be stored
JOBS = []

max_flow_time = datetime.timedelta(0)
mft_job = Job({})

mft_job.print_job()
print(''.join(['maximum flow time: ', str(max_flow_time)]))

# do the actual scheduling
for terminal in TERMINALS:

    # while the queue is not empty
    while terminal['flight_queue']:
        job = {}

        flight = heapq.heappop(terminal['flight_queue'])

        job['code'] = flight.code
        job['terminal'] = flight.terminal
        job['release_date'] = flight.estimated_arrival_time
        job['size'] = flight.size

        conveyor = get_earliest_available_conveyor(terminal)
        job['conveyor'] = conveyor

        # the latest between the time the conveyor is available and the time the flight arrives
        job['start_time'] = max(get_available_date(terminal['conveyors'][conveyor]),
                                flight.estimated_arrival_time)

        flow_time = job['start_time'] + flight.size - flight.estimated_arrival_time
        job['flow_time'] = flow_time

        job = Job(job)
        add_job_to_conveyor(job, terminal['conveyors'][conveyor])

        if flow_time > max_flow_time:
            max_flow_time = flow_time
            mft_job = job

        JOBS.append(job)


for job in JOBS:
    job.print_job()
    print()

print('Maximum flow time')
mft_job.print_job()
