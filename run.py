from subprocess import call
import sys

if len(sys.argv) == 2:
    no_iterations = sys.argv[1]
else:
    no_iterations = 10

with open("results_list.csv", "w") as myfile:
    myfile.write('dispatching_time tabu_time\n')

for i in range(100):
    call(["python", "dispatch.py", "data/test.csv",
          str(no_iterations)])
