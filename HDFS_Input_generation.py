import random
file = open("myfile1.txt","w")
n = 100
for i in range(n):
    file.write('%d : ' % (i+1))
    num = random.randint(1,50)
    randomlist = random.sample(range(100, 5000), num)
    for listitem in randomlist:
        file.write('%s ' % listitem)
    file.write('\n')
    
