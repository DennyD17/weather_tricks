**Tech Task**
<br>
Write a script that reads a copy of the file found on the remote URL and produce the outputs listed
below:
http://www.fifeweather.co.uk/cowdenbeath/200606.csv

Generate files for required outputs as follows:
1. A .txt file containing the answers to the following questions:
Using the “Outside Temperature” values:
a. What is the average time of hottest daily temperature (over month);
b. What time of the day is the most commonly occurring hottest time;
c. Which are the Top Ten hottest times on distinct days, preferably sorted by date order.
2. Using the ‘Hi Temperature’ values produce a “.txt” file containing all of the Dates and Times
where the “Hi Temperature” was within +/- 1 degree of 22.3 or the “Low Temperature” was
within +/- 0.2 degree higher or lower of 10.3 over the first 9 days of June
3. You want to forecast the “Outside Temperature” for the first 9 days of the next month.
Assume that:
a. The average temperature for each day of July is constant and equal to 25 degrees;
b. For the 1st of July, the pattern of the temperatures across the day with respect to the
average temperature on that day is similar to the one found on 1st of June, for the
2nd of July is similar to the average on the 2nd of June, etc.
Produce a “.txt” file with your forecast for July (from 1st July to 9th July) with the sample
values for each time for e.g. dd/mm/yyyy, Time, Outside Temperature.

**Local Development**
<br>
1. Clone repository to local machine
2. Install environment using ```poetry install```
3. Run script using ```python main.py``` 

Files with results  will be placed in ```output``` directory.   