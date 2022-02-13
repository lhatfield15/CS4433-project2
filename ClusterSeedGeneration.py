import random
import sys

if __name__ == '__main__':
    count = sys.argv[1];
    f = open(r"C:\Users\lhatf\Documents\College\CS4433\VM_shared_folder\cs4433_project1\cs4433-project2\seed_points.txt", "w")
    # f = open(r"C:\Users\Alex\IdeaProjects\CS4433_Project_2\data_points.txt", "a")
    for i in range(int(count)):
        x = random.randint(0,10000)
        y = random.randint(0,10000)
        point = str(x) + "," + str(y) + "\n"
        f.write(point)
    f.close()
