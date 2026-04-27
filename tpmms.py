import time
import heapq

from heapfile import *


# employee format
RECORD_FORMAT = "<5s10s12s12s1s10s"
FIELD_INDEX = {
    "id": 0,
    "birth_date": 1,
    "first_name": 2,
    "last_name": 3,
    "gender": 4,
    "hire_date": 5,
}


def get_sort_index(sort_key: str) -> int:
    if sort_key not in FIELD_INDEX:
        raise ValueError(f"sort_key inválido: {sort_key}")
    return FIELD_INDEX[sort_key]


"""
Lee B páginas a la vez, las ordena en memoria por el sort_key,
las escribe como archivos temporales de run ordenado.
Retorna la lista de rutas de los runs generados.
"""
def generate_runs(
    heap_path: str,
    page_size: int,
    buffer_size: int,
    sort_key: str
) -> list[str]:

    B = buffer_size // page_size

    if B < 1:
        raise ValueError("buffer_size debe ser mayor que page_size")

    total_pages = count_pages(heap_path, page_size)
    sort_index = get_sort_index(sort_key)
    page_capacity = page_size // struct.calcsize(RECORD_FORMAT)

    run_paths = []
    run_id = 0

    for start_page in range(0, total_pages, B):
        all_records = []
        end_page = min(start_page + B, total_pages)

        # leer B páginas
        for page_id in range(start_page, end_page):
            records = read_page(
                heap_path,
                page_id,
                RECORD_FORMAT,
                page_size
            )
            all_records.extend(records)

        # ordenar en memoria
        all_records.sort(key=lambda r: r[sort_index])

        # guardar run temporal
        run_path = f"run_{run_id}.bin"
        run_id += 1

        if os.path.exists(run_path):
            os.remove(run_path)

        open(run_path, "wb").close()

        page_records = []
        page_id_write = 0

        for record in all_records:
            page_records.append(record)

            if len(page_records) == page_capacity:
                write_page(
                    run_path,
                    page_id_write,
                    page_records,
                    RECORD_FORMAT,
                    page_size
                )
                page_records = []
                page_id_write += 1

        if page_records:
            write_page(
                run_path,
                page_id_write,
                page_records,
                RECORD_FORMAT,
                page_size
            )

        run_paths.append(run_path)

    return run_paths



"""
Realiza un k-way merge de los runs usando un min-heap.
Escribe el resultado ordenado en output_path.
Usa heapq de Python para el min-heap.
"""

def merge_group(
    group_paths: list[str],
    output_path: str,
    page_size: int,
    sort_key: str
):
    sort_index = get_sort_index(sort_key)
    page_capacity = page_size // struct.calcsize(RECORD_FORMAT)

    if os.path.exists(output_path):
        os.remove(output_path)

    open(output_path, "wb").close()

    heap = []
    run_states = []

    # cargar primera página de cada run del grupo
    for run_idx, path in enumerate(group_paths):
        first_page = read_page(
            path,
            0,
            RECORD_FORMAT,
            page_size
        )

        if not first_page:
            continue

        state = {
            "path": path,
            "page_id": 0,
            "records": first_page,
            "record_index": 0,
        }

        run_states.append(state)

        first_record = first_page[0]
        heapq.heappush(
            heap,
            (first_record[sort_index], run_idx, first_record)
        )

    output_records = []
    output_page_id = 0

    while heap:
        _, run_idx, record = heapq.heappop(heap)
        output_records.append(record)

        if len(output_records) == page_capacity:
            write_page(
                output_path,
                output_page_id,
                output_records,
                RECORD_FORMAT,
                page_size
            )
            output_records = []
            output_page_id += 1

        state = run_states[run_idx]
        state["record_index"] += 1

        if state["record_index"] >= len(state["records"]):
            state["page_id"] += 1
            next_page = read_page(
                state["path"],
                state["page_id"],
                RECORD_FORMAT,
                page_size
            )

            if not next_page:
                continue

            state["records"] = next_page
            state["record_index"] = 0

        next_record = state["records"][state["record_index"]]
        heapq.heappush(
            heap,
            (next_record[sort_index], run_idx, next_record)
        )

    if output_records:
        write_page(
            output_path,
            output_page_id,
            output_records,
            RECORD_FORMAT,
            page_size
        )


def multiway_merge(
    run_paths: list[str],
    output_path: str,
    page_size: int,
    buffer_size: int,
    sort_key: str
) -> int:
    B = buffer_size // page_size

    if B < 2:
        raise ValueError(
            "Se necesitan al menos 2 buffers (1 input + 1 output)"
        )

    fan_in = B - 1
    current_runs = run_paths[:]
    pass_num = 0

    while len(current_runs) > 1:
        new_runs = []
        group_id = 0

        # dividir en grupos de tamaño B-1
        for i in range(0, len(current_runs), fan_in):
            group = current_runs[i:i + fan_in]
            temp_output = f"merge_pass_{pass_num}_group_{group_id}.bin"
            group_id += 1

            merge_group(
                group,
                temp_output,
                page_size,
                sort_key
            )

            new_runs.append(temp_output)

        # borrar runs viejos temporales
        for old_run in current_runs:
            if old_run.startswith("run_") or old_run.startswith("merge_pass_"):
                if os.path.exists(old_run):
                    os.remove(old_run)

        current_runs = new_runs
        pass_num += 1

    final_run = current_runs[0]

    if os.path.exists(output_path):
        os.remove(output_path)
        
    os.rename(final_run, output_path)
    
    return pass_num



"""
Ejecuta TPMMS completo y retorna métricas:
{
 'runs_generated': int,
 'pages_read': int,
 'pages_written': int,
 'time_phase1_sec': float,
 'time_phase2_sec': float,
 'time_total_sec': float
}
"""
def external_sort(
    heap_path: str,
    output_path: str,
    page_size: int,
    buffer_size: int,
    sort_key: str
) -> dict:

    total_pages = count_pages(heap_path, page_size)

    total_start = time.time()

    # Phase 1
    phase1_start = time.time()

    run_paths = generate_runs(
        heap_path,
        page_size,
        buffer_size,
        sort_key
    )

    phase1_end = time.time()

    run_pages = sum(
        count_pages(path, page_size)
        for path in run_paths
    )

    # Phase 2
    phase2_start = time.time()

    merge_passes = multiway_merge(
        run_paths,
        output_path,
        page_size,
        buffer_size,
        sort_key
    )

    phase2_end = time.time()
    total_end = time.time()

    output_pages = count_pages(output_path, page_size)

    pages_read = total_pages + (merge_passes * total_pages)
    pages_written = run_pages + (merge_passes * total_pages)

    return {
        "runs_generated": len(run_paths),
        "pages_read": pages_read,
        "pages_written": pages_written,
        "time_phase1_sec": round(phase1_end - phase1_start, 4),
        "time_phase2_sec": round(phase2_end - phase2_start, 4),
        "time_total_sec": round(total_end - total_start, 4),
    }
