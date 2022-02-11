import random

if __name__ == '__main__':
    num_points = 200 #Actually half the number
    f = open(r"C:\Users\lhatf\Documents\College\CS4433\VM_shared_folder\cs4433_project1\cs4433-project2\data_points.txt", "a")
    # f = open(r"C:\Users\Alex\IdeaProjects\CS4433_Project_2\data_points.txt", "a")
    for i in range(num_points):
        x = random.randint(0,500)
        y = random.randint(0,500)
        point = str(x) + "," + str(y) + "\n"
        f.write(point)

    for i in range(num_points):
        x = random.randint(5000,5500)
        y = random.randint(5000,5500)
        point = str(x) + "," + str(y) + "\n"
        f.write(point)
    f.close()
