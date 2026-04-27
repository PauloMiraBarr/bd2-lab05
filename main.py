from heapfile import export_to_heap
from tpmms import external_sort

# PATHS
data_path = "data"
employee_path = "employee"
department_path = "department_employee"

# FORMATS
# id:5s, birth:10s, first:30s, last:30s, gender:1s, hire:10s
employee_format = "<5s10s12s12s1s10s"
# id:5s, departmentid:4s, from:10s, to:10s
department_format = "<5s4s10s10s"

# MEM SIZE
PAGE_SIZE = 4096


# CONSTRUCTION
"""
# employee table
export_to_heap(
    f"{employee_path}.csv",
    f"{data_path}/{employee_path}.bin",
    employee_format,
    PAGE_SIZE
)

# employee department table
export_to_heap(
    f"{department_path}.csv",
    f"{data_path}/{department_path}.bin",
    department_format,
    PAGE_SIZE
)
"""


# TPMMS QUERY
""" Descomentar para probar tpmms
KB = 1024
# BUFFER_SIZE_LIST = [256*KB]
BUFFER_SIZE_LIST = [64*KB, 128*KB, 256*KB]

for INDEX, BUFFER_SIZE in enumerate(BUFFER_SIZE_LIST):
    result = external_sort(
        heap_path=f"{data_path}/{employee_path}.bin",
        output_path=f"{data_path}/{employee_path}_sorted_{INDEX}.bin",
        page_size=PAGE_SIZE,
        buffer_size=BUFFER_SIZE,
        sort_key="hire_date"
    )
    print(f"====== TEST {INDEX} ======\n", result, "\n")
"""
