import csv
import re
from urllib.request import build_opener
from bs4 import BeautifulSoup

OPENER = build_opener()
OPENER.addheaders = [('User-Agent', '')]


def get_flight_link(a_element):
    return 'https://www.airport-jfk.com' + a_element['href']


def parse_flight(link):
    flight_page = OPENER.open(link)
    this_soup = BeautifulSoup(flight_page, 'lxml')

    this_flight = {}

    this_flight['id'] = link.split('/')[-1]

    arrival_info = this_soup.find('div', attrs={'id': "flight_arr"})
    date = arrival_info.findAll(text=re.compile('Date: '))
    if date:
        this_flight['date'] = date[0].split(': ')[1].strip()

    eta = arrival_info.find('h2')
    this_flight['estimated_arrival_time'] = eta.text if eta else ''

    date = arrival_info.findAll(text=re.compile('Date: '))
    if date:
        this_flight['date'] = date[0].split(': ')[1].strip()

    sat = arrival_info.findAll(text=re.compile('Scheduled Arrival Time: '))
    if sat:
        this_flight['scheduled_arrival_time'] = sat[0].split(': ')[1].strip()

    terminal = arrival_info.findAll(text=re.compile('Terminal: '))
    if terminal:
        this_flight['terminal'] = terminal[0].split(': ')[1].strip()

    gate = arrival_info.findAll(text=re.compile('Gate: '))
    if gate:
        this_flight['gate'] = gate[0].split(': ')[1].strip()

    baggage = arrival_info.findAll(text=re.compile('Baggage: '))
    this_flight['baggage'] = baggage[0].split(': ')[1].strip() if baggage else ''

    other_info = this_soup.find('div', attrs={'id': "flight_other"})

    plane = other_info.findAll(text=re.compile('Plane: '))
    if plane:
        this_flight['plane'] = plane[0].split()[1].strip()

    duplicate_flights = other_info.findAll('a')
    duplicate_links = []

    for same in duplicate_flights:
        duplicate_links.append(get_flight_link(same))

    return this_flight, duplicate_links


ARRIVALS_URL = 'https://www.airport-jfk.com/arrivals.php'
ARRIVALS_PAGE = OPENER.open(ARRIVALS_URL)

SOUP = BeautifulSoup(ARRIVALS_PAGE, 'lxml')

FLIGHTS_HTML = SOUP.findAll('div', attrs={'id': "flight_detail"})
FLIGHTS = []
IGNORE_LIST = []

for flight in FLIGHTS_HTML:
    if flight:
        fhour_div = flight.find('div', attrs={'id': 'fhour'})
        if fhour_div:
            scrape_link = get_flight_link(fhour_div.a)
            if scrape_link not in IGNORE_LIST:
                parsed_flight, duplicates = parse_flight(scrape_link)
                IGNORE_LIST.extend(duplicates)
                FLIGHTS.append(parsed_flight)
                print(parsed_flight)

KEYS = ['id', 'date', 'estimated_arrival_time', 'scheduled_arrival_time',
        'terminal', 'gate', 'baggage', 'plane']

with open('data/flights_24_04_2018_06_12.csv', 'w') as output_file:
    DICT_WRITER = csv.DictWriter(output_file, KEYS)
    DICT_WRITER.writeheader()
    DICT_WRITER.writerows(FLIGHTS)
