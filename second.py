import random
import math

def process_data(p_value1, p_value2=None):
    if isinstance(p_value1, str) and p_value1 != "":
        return p_value1.count("e")
    elif isinstance(p_value1, tuple):
        return p_value1[-1]
    elif isinstance(p_value1, int) and isinstance(p_value2, int):
        return random.randint(p_value1, p_value2 - 1)
    elif isinstance(p_value1, float):
        return math.ceil(p_value1)
    elif isinstance(p_value1, list):
        p_value1.insert(1, p_value2)
        return p_value1
    elif isinstance(p_value1, dict):
        p_value1["M"] = "Mysuru"
        return p_value1

# sample calls
# print(process_data("Apple"))
# print(process_data((10,20,30)))
# print(process_data(2,8))
# print(process_data(2.3))
# print(process_data([100,200,300],10))
# print(process_data({"I":"India","A":"USA"}))
