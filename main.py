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

""" USO DEL EXTERNAL HASHING PARA HACER UN GROUP BY EN UN ARCHIVO QUE NO CABE EN RAM
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
from external_hashing import external_hash_group_by

print("\n" + "="*40)
print(" INICIANDO PRUEBA DE EXTERNAL HASHING")
print("="*40)

# 2. Definimos los parámetros del laboratorio
# Usaremos el archivo .dat que main.py acaba de crear en la carpeta heap
archivo_a_procesar = f"{heap_path}/{department_path}.dat"
tamano_pagina = 4096      # 4 KB por página
memoria_ram = 65536       # 64 KB de buffer total en RAM
resultados = external_hash_group_by(archivo_a_procesar, tamano_pagina, memoria_ram, "from_date")
print("\n--- MUESTRA DE RESULTADOS ---")
contador = 0
for fecha_bytes, cantidad in resultados.items():
    # Como los datos vienen en bytes desde el archivo binario, 
    # los decodificamos a texto normal para que se vean bonitos
    try:
        fecha_texto = fecha_bytes.decode('utf-8').strip('\x00')
    except:
        fecha_texto = str(fecha_bytes)
        
    print(f"Fecha: {fecha_texto} ---> Se contrataron {cantidad} empleados")
    
    contador += 1
    if contador >= 15: # Solo mostramos 15 para no saturar la pantalla
        break

"""
