nohup python3 -u weather_hko.py &
To execute weather_hko in the background and won't hang up even if the terminal is shutted down.

ps ax | grep weather_hko.py
To search for a process whose ID is forgotten.

kill -9 12345
To kill the process 12345.

Output of weather_hko.py will go to nohup.out due to nohup, must open .out file with default text editor.