"""
Produce stream data

Reads power_streaming_data.csv and repeatedly samples 5 rows, 
writing each sample as a csv file to the stream_folder.

The loop runs 20 iterations with a 10-second pause between each output.
"""

import time
import pandas as pd

# Read data
df = pd.read_csv("Final_Project/power_streaming_data.csv")

# 20 iterations
for i in range(20):
    sample = df.sample(5)
    sample.to_csv(f"Final_Project/stream_folder/batch_"+ str(i) + ".csv", index = False)
    time.sleep(2) # Pause for 10 seconds in between outputting of data sets
