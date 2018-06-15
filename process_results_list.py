import csv
import sys
import datetime

results_list_file = sys.argv[1]
dest_file = sys.argv[2]

dispatch_list = []
tabu_list = []

with open(results_list_file, 'r') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=' ')
    for row in csvreader:
        dispatch_list.append(row[0])
        tabu_list.append(row[1])

dispatch_list.pop(0)
tabu_list.pop(0)

diff = [datetime.datetime.strptime(dispatch_list[i], '%H:%M:%S') -
        datetime.datetime.strptime(tabu_list[i], '%H:%M:%S')
        for i in range(len(dispatch_list))]

diff_string = [str(x) for x in diff]
diff_int = [int(x.seconds / 60) for x in diff]

with open(results_list_file, 'w') as csvfile:
    csvfile.write('dispatch_time tabu_time diff_mins\n')

    for index in range(len(diff_int)):
        csvfile.write('{} {} {}\n'.format(dispatch_list[index],
                                          tabu_list[index],
                                          diff_int[index]))

no_examples = len(diff_int)
no_zeros = diff_int.count(0)
no_improvements = no_examples - no_zeros
improvement_values = [x for x in diff_int if x != 0]
improvement_average = sum(improvement_values) / len(improvement_values)
improvement_percentage = no_improvements / no_examples

with open(dest_file, 'w') as destfile:
    destfile.write('Number of examples: {}\n'.format(no_examples))
    destfile.write('Number of improvements: {}\n'.format(no_improvements))
    destfile.write('Improvement Percentage: {}\n'.format(improvement_percentage))
    destfile.write('Improvement Average: {}\n'.format(improvement_average))
