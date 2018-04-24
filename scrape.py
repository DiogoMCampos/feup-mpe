from urllib.request import urlopen, build_opener
from bs4 import BeautifulSoup
import re
import csv

opener = build_opener()
opener.addheaders = [('User-Agent', 'Mozilla/5.0')]


def get_flight_link(a_element):
    return 'https://www.airport-jfk.com' + a_element['href']


def parse_flight(link):
    flightPage = opener.open(link)
    soup = BeautifulSoup(flightPage, 'lxml')

    flight = {}

    flight['id'] = link.split('/')[-1]

    arrival_info = soup.find('div', attrs={'id': "flight_arr"})
    date = arrival_info.findAll(text=re.compile('Date: '))
    if date:
        flight['date'] = date[0].split(': ')[1].strip()

    eta = arrival_info.find('h2')
    flight['estimated_arrival_time'] = eta.text if eta else ''

    sat = arrival_info.findAll(text=re.compile('Scheduled Arrival Time: '))
    if sat:
        flight['scheduled_arrival_time'] = sat[0].split(': ')[1].strip()

    terminal = arrival_info.findAll(text=re.compile('Terminal: '))
    if terminal:
        flight['terminal'] = terminal[0].split(': ')[1].strip()

    gate = arrival_info.findAll(text=re.compile('Gate: '))
    if gate:
        flight['gate'] = gate[0].split(': ')[1].strip()

    baggage = arrival_info.findAll(text=re.compile('Baggage: '))
    flight['baggage'] = baggage[0].split(': ')[1].strip() if baggage else ''

    other_info = soup.find('div', attrs={'id': "flight_other"})

    plane = other_info.findAll(text=re.compile('Plane: '))
    if plane:
        flight['plane'] = plane[0].split()[1].strip()

    duplicate_flights = other_info.findAll('a')
    duplicate_links = []

    for same in duplicate_flights:
        duplicate_links.append(get_flight_link(same))

    return flight, duplicate_links


arrivalsUrl = 'https://www.airport-jfk.com/arrivals.php?tp=6'
arrivalsPage = opener.open(arrivalsUrl)

soup = BeautifulSoup(arrivalsPage, 'lxml')

flights_html = soup.findAll('div', attrs={'id': "flight_detail"})
flights = []
ignore_list = []

for flight in flights_html:
    if flight:
        fhour_div = flight.find('div', attrs={'id': 'fhour'})
        if fhour_div:
            scrape_link = get_flight_link(fhour_div.a)
            if scrape_link not in ignore_list:
                parsed_flight, duplicates = parse_flight(scrape_link)
                ignore_list.extend(duplicates)
                flights.append(parsed_flight)
                print(parsed_flight)

keys = ['id', 'date', 'estimated_arrival_time', 'scheduled_arrival_time',
        'terminal', 'gate', 'baggage', 'plane']

with open('flights_24_04_2018_06_12.csv', 'w') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(flights)
