#!/bin/python3
""" dispatch.py - Dispatch the flights read from a file input as an argument according to ERD """

import sys
import random
import datetime
import functools
import heapq
import json

random.seed()

TABU_LIST_SIZE = 10
TABU_SEARCH_ITERATIONS = 10
EARLIEST_DATE = datetime.datetime.min


class TabuList():
    def __init__(self, maxsize):
        self.maxsize = maxsize
        self.list = list()

    def full(self):
        return len(self.list) == self.maxsize

    def put(self, item):
        if self.full():
            self.list.remove(self.list[0])
        self.list.append(item)

    def __contains__(self, comparable):
        """ checks if comparable state exists in list """

        for state in self.list:
            if state == comparable:
                return True

        return False


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
            self.terminal = int(plane_dict['terminal'])
            self.release_date = plane_dict['release_date']
            self.size = plane_dict['size']
            self.conveyor = int(plane_dict['conveyor'])
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

    def update_start_time(self, start_time):
        self.start_time = start_time
        self.end_time = self.start_time + self.size
        self.flow_time = self.end_time - self.release_date

    def copy(self):
        return Job(self.__dict__)

    def toJson(self, filename):
        self_json = self.__dict__
        for key in self_json:
            self_json[key] = str(self_json[key])
        return json.dump(self.__dict__, open(filename, 'a'))

    def __lt__(self, other):
        return self.start_time < self.start_time

    def __eq__(self, other):
        return self.code == other.code and \
           self.size == other.size and \
           self.release_date == other.release_date and \
           self.start_time == other.start_time and \
           self.terminal == other.terminal and \
           self.conveyor == other.conveyor and \
           self.flow_time == other.flow_time and \
           self.end_time == other.end_time


class State:
    def __init__(self, terminals, jobs):
        self.terminals = []
        self.jobs = []

        for terminal in terminals:
            if 'conveyors' not in terminal:
                self.terminals.append({})
                continue

            terminal_copy = {'conveyors': []}
            for conveyor in terminal['conveyors']:
                conveyor_copy = []
                for job in conveyor:
                    conveyor_copy.append(job.copy())
                terminal_copy['conveyors'].append(conveyor_copy)
            self.terminals.append(terminal_copy)

        for job in jobs:
            self.jobs.append(job.copy())

    # spaghetti code please fix me
    # assuming terminals in state are always coherent with jobs
    def __eq__(self, comparable):
        if len(self.jobs) != len(comparable.jobs):
            return False

        for i in range(0, len(self.jobs)):
            j1 = self.jobs[i]
            j2 = comparable.jobs[i]

            if j1 != j2:
                return False

        return True


def get_available_date(conveyor):
    """ return earliest available date for specific conveyor """
    if not conveyor:
        return EARLIEST_DATE
    else:
        return conveyor[-1].end_time


def get_earliest_available_conveyor(term):
    """ return the conveyor that is available first """
    earliest_dates = [get_available_date(c) for c in term['conveyors']]
    return earliest_dates.index(min(earliest_dates))


def get_earliest_possible_date(job, conveyor):
    """ return earliest available date for specific job in a specific conveyor """
    job_min_end_date = job.release_date + job.size
    if job_min_end_date < conveyor[0].start_time:
        return job.release_date

    for index in range(0, len(conveyor) - 1):
        left_comparable = conveyor[index]
        if job.release_date >= left_comparable.end_time:
            return job.release_date

    return max(conveyor[-1].end_time, job.release_date)


def add_job_to_conveyor(job, conveyor):
    conveyor.append(job)
    conveyor.sort()
    for index in range(0, len(conveyor) - 1):
        left_job = conveyor[index]
        right_job = conveyor[index + 1]

        if right_job.start_time <= left_job.end_time:
            right_job.update_start_time(left_job.end_time)


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


def get_maximum_flow_time_job(state):
    maximum_flow_time = datetime.timedelta.min
    mft_job_index = 0

    for index, job in enumerate(state.jobs):
        if job.flow_time > maximum_flow_time:
            maximum_flow_time = job.flow_time
            mft_job_index = index

    return state.jobs[mft_job_index]


def is_better(state, comparable):
    """ returns true if state has maximum flow time lower than comparable """
    if comparable is None:
        return True

    return get_maximum_flow_time_job(state).flow_time < get_maximum_flow_time_job(comparable).flow_time


def generate_new_neighbor_list(current_state):
    neighbor_list = []
    num_jobs = len(current_state.jobs)
    for i in range(0, num_jobs - 1):
        state = State(current_state.terminals, current_state.jobs)
        jobs = state.jobs
        left_job = state.jobs[i]
        right_job = state.jobs[i + 1]

        if left_job.terminal != right_job.terminal:
            continue

        left_conveyor = state.terminals[left_job.terminal]['conveyors'][left_job.conveyor]
        right_conveyor = state.terminals[right_job.terminal]['conveyors'][right_job.conveyor]

        left_conveyor.remove(left_job)
        right_conveyor.remove(right_job)

        left_job.conveyor,  right_job.conveyor = right_job.conveyor, left_job.conveyor

        earliest_left_date = get_earliest_possible_date(left_job, right_conveyor)
        earliest_right_date = get_earliest_possible_date(right_job, left_conveyor)

        left_job.update_start_time(earliest_left_date)
        right_job.update_start_time(earliest_right_date)

        add_job_to_conveyor(left_job, right_conveyor)
        add_job_to_conveyor(right_job, left_conveyor)

        neighbor_list.append(state)

    return neighbor_list


def get_best_state(state_list):
    best_state = None
    for state in state_list:
        if is_better(state, best_state):
            best_state = state

    return best_state


def generate_new_non_tabu_neighbor(current_state, tabu_list):
    """ get best non tabu neighbor given current state and tabu list """
    neighbor_list = generate_new_neighbor_list(current_state)
    non_tabu_neighbor_list = [neighbor for neighbor in neighbor_list
                              if neighbor not in tabu_list]

    if not non_tabu_neighbor_list:
        return current_state
    else:
        return get_best_state(non_tabu_neighbor_list)


def tabu_search(initial_state, max_iterations):
    """ run tabu_search on initial_state for a max of max_iterations """
    current_state = State(initial_state.terminals, initial_state.jobs)
    best_state = State(initial_state.terminals, initial_state.jobs)
    tabu_list = TabuList(maxsize=TABU_LIST_SIZE)

    # TODO additional stopping criteria
    for i in range(max_iterations):
        candidate = generate_new_non_tabu_neighbor(current_state, tabu_list)
        mft_job = get_maximum_flow_time_job(candidate)
        print("Iteration {}, Maximum Flow Time: {}".format(i, mft_job.flow_time))
        print("MFT Job: ")
        mft_job.print_job()
        print()

        tabu_list.put(current_state)
        current_state = State(candidate.terminals, candidate.jobs)

        if is_better(candidate, best_state):
            best_state = State(candidate.terminals, candidate.jobs)

    return best_state


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

dispatch_state = State(TERMINALS, JOBS)

if len(sys.argv) > 2:
    TABU_SEARCH_ITERATIONS = sys.argv[2]

tabu_search_jobs = tabu_search(dispatch_state, int(TABU_SEARCH_ITERATIONS))

tabu_mft = get_maximum_flow_time_job(tabu_search_jobs)
print('Maximum Flow Time after dispatching rules: {}'.format(mft_job.flow_time))
print("MFT Job: ")
mft_job.print_job()
print()
print('MFT after Tabu Search: {}'.format(tabu_mft.flow_time))
print("MFT Job: ")
tabu_mft.print_job()
print()

with open("results_list.csv", "a") as myfile:
    myfile.write('{} {}\n'.format(mft_job.flow_time, tabu_mft.flow_time))
