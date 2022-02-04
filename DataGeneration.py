import random

if __name__ == '__main__':
    num_points = 1000
    f = open(r"C:\Users\Alex\IdeaProjects\CS4433_Project_2\data_points.txt", "a")
    for i in range(num_points):
        x = random.randint(0,10000)
        y = random.randint(0,10000)
        point = str(x) + "," + str(y) + "\n"
        f.write(point)
    f.close()
