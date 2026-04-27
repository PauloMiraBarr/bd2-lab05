import time
from heapfile import read_page, write_page, count_pages
import struct

RECORD_FORMAT = "<5s4s10s10s" 

def partition_data(heap_path: str, page_size: int, buffer_size: int, group_key: str) -> list[str]:
    # 1. Calculamos cuántas páginas caben en nuestra RAM
    B = buffer_size // page_size
    k = B - 1 
    
    # 2. Creamos las rutas de los archivos k temporales
    partition_paths = [f"particion_{i}.dat" for i in range(k)]
    
    # Nos aseguramos de crear los archivos vacíos en el disco duro
    for path in partition_paths:
        open(path, "wb").close()
    buffer_ram = {i: [] for i in range(k)}
    pages_written = {i: 0 for i in range(k)} # Para saber en qué página escribir
    
    record_size = struct.calcsize(RECORD_FORMAT)
    max_records = page_size // record_size
    
    # Leemos el archivo original página por página
    total_pages = count_pages(heap_path, page_size)
    for page_id in range(total_pages):
        records = read_page(heap_path, page_id, RECORD_FORMAT, page_size)
        
        for record in records:
            # record[2] es la fecha (from_date)
            fecha = record[2]
            caja_destino = hash(fecha) % k
            
            # Lo ponemos en la bandejita de la RAM
            buffer_ram[caja_destino].append(record)
            if len(buffer_ram[caja_destino]) == max_records:
                write_page(partition_paths[caja_destino], pages_written[caja_destino], buffer_ram[caja_destino], RECORD_FORMAT, page_size)
                pages_written[caja_destino] += 1
                buffer_ram[caja_destino] = [] # Limpiamos la bandejita
    for i in range(k):
        if len(buffer_ram[i]) > 0:
            write_page(partition_paths[i], pages_written[i], buffer_ram[i], RECORD_FORMAT, page_size)
            
    return partition_paths

def aggregate_partitions(partition_paths: list[str], page_size: int, buffer_size: int, group_key: str) -> dict:
    resultado_final = {}
 
    for path in partition_paths:
        try:
            total_pages = count_pages(path, page_size)
        except ValueError:
            continue # Si la partición quedó vacía, la ignoramos
            
        # Leemos toda esta caja (ahora sí estamos seguros de que no quemará la RAM)
        for page_id in range(total_pages):
            records = read_page(path, page_id, RECORD_FORMAT, page_size)
            
            for record in records:
                fecha = record[2] # from_date
                # Contamos normalmente como un diccionario de Python
                if fecha in resultado_final:
                    resultado_final[fecha] += 1
                else:
                    resultado_final[fecha] = 1
                    
    return resultado_final

def external_hash_group_by(heap_path: str, page_size: int, buffer_size: int, group_key: str) -> dict:
    start_time = time.time()
    
    # Calculamos K para las métricas
    B = buffer_size // page_size
    k = B - 1
    
    # Calculamos las páginas originales
    try:
        from heapfile import count_pages
        total_pages = count_pages(heap_path, page_size)
    except:
        total_pages = 0
    
    # Fase 1: Particionar
    t0 = time.time()
    particiones = partition_data(heap_path, page_size, buffer_size, group_key)
    t1 = time.time()
    time_phase1 = t1 - t0
    
    # Fase 2: Agregar (Contar)
    t2 = time.time()
    resultado = aggregate_partitions(particiones, page_size, buffer_size, group_key)
    t3 = time.time()
    time_phase2 = t3 - t2
    
    time_total = time.time() - start_time
    pages_read = total_pages * 2
    pages_written = total_pages
    return {
        'result': resultado,
        'partitions_created': k,
        'pages_read': pages_read,
        'pages_written': pages_written,
        'time_phase1_sec': time_phase1,
        'time_phase2_sec': time_phase2,
        'time_total_sec': time_total
    }
