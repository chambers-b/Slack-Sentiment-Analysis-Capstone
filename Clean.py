import re
import json
import os

#Get directory structure
TopDir = os.getcwd()+ "\\UHD-MSDA Slack export Dec 3 2017 - Nov 12 2019"
ext = "-clean-predict" #New directory extension
SubDirs = os.listdir(TopDir)

SubDirs.remove("channels.json")#Exclude this file
SubDirs.remove("integration_logs.json")#Exclude this file
#SubDirs.remove("users.json")#Exclude this file

count = 0
msgs = 0

#Create directory
try:
    os.mkdir(TopDir + ext)
except OSError:
    print ("Creation of the directory %s failed" % (TopDir + ext))
else:
    print ("Successfully created the directory %s " % (TopDir + ext))
    
#print(SubDirs) #Print all sub directories

#Iterate through directories
for SubDir in SubDirs:
    file = os.fsencode(SubDir)
    
    #Iterate through files
    for file in os.listdir(TopDir+"\\"+SubDir):
        filename = os.fsdecode(file)
        f = open(TopDir +"\\"+SubDir+"\\"+filename, errors = 'ignore')
        data = f.read()
        reg = ("(,[^,]* \"user_profile\": {[^}]*})")

        #Count raw messages
        msg = re.findall("\"type\": \"message\"", data)
        for i in msg:
            msgs = msgs + 1
        
        cases = re.findall(reg, data)
        for i in cases:
            count = count + 1
        data = re.sub(reg, "", data)
        
        #f.write(data) #Optional for error detection
        #print(str(count) + " " + SubDir + " " + filename) #Print number cleanings per file #Optional for error detection

        f2 = open(TopDir + ext + "\\" + SubDir + " " + filename, "w")
        f2.write(data)
print("%s messages were cleaned" % count)
print("out of %s messages" % msgs)
f.close()
f2.close()
        
