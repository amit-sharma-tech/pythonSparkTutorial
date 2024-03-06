
def readFile():
    with open("in/airports.text","r") as f:
        readData = f.readline()
        #breakpoint()
        #print(readData)

        #for line in f:
            # if "United States" in line:
            #     with open("dummy.txt","a+") as wr:
            #         wr.write(line)
                    #print(line)
        #print(type(readData))
        #breakpoint()
        data = list(readData.split(","))
        # print(data)
        # breakpoint()
        data = filter(lambda li : li.split(" ")[3],data)
        breakpoint()   

if __name__ == "__main__":
    readFile()