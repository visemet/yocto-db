import time
import calendar

output = open("data-parsed.dta", "w")

with open("data.dta") as f:
    for line in f:
        parsed = (line[1:len(line)-3]).split(", ")
        datetime = parsed[0] + " 00:00:00"
        unix_time = calendar.timegm(time.strptime(datetime, "%Y-%m-%d %H:%M:%S"))
        
        output.write("{" + str(unix_time))
        for i in range(1, 23):
            output.write(", " + str(parsed[i]))
        output.write("}.\n")

output.close()
    
