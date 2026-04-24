from heapfile import *

heap_path = "heap"

employee_path = "employee"
department_path = "department_employee"

# id:5s, birth:10s, first:30s, last:30s, gender:1s, hire:10s
employee_format = "<5s10s30s30s1s10s"
# id:5s, departmentid:4s, from:10s, to:10s
department_format = "<5s4s10s10s"

# employee table
export_to_heap(
    f"{employee_path}.csv",
    f"{heap_path}/{employee_path}.dat",
    employee_format,
    4096
)
# employee department table
export_to_heap(
    f"{department_path}.csv",
    f"{heap_path}/{department_path}.dat",
    department_format,
    4096
)


