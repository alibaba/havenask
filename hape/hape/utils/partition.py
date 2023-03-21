import math

def get_partition_intervals(partition_count, partid=None):
    MAX_RANGE = 65536
    partition_len =  int(math.floor(1.0 * MAX_RANGE / partition_count))
    intervals = []
    start, end = 0, partition_len - 1
    if partition_len * partition_count < MAX_RANGE:
        end += MAX_RANGE - partition_len * partition_count
    while True:
        intervals.append((start, end))
        start = end + 1 
        end = start + partition_len - 1 
        # print(start, end)
        if end >= MAX_RANGE - 1:
            break
    if start != MAX_RANGE:
        intervals.append((start, end))
    if partid != None:
        return intervals[partid]
    else:
        return intervals
