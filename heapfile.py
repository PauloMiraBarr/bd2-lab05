import csv
import os
import struct


# Exporta un CSV a un heap file binario paginado
def export_to_heap(csv_path: str, heap_path: str, record_format: str, page_size: int):
    record_size = struct.calcsize(record_format)

    if page_size < record_size:
        raise ValueError("El page_size es demasiado pequeño para el tamaño del registro.")

    with open(csv_path, "r", newline="", encoding="utf-8") as csv_file, \
         open(heap_path, "wb") as heap_file:

        reader = csv.reader(csv_file, delimiter="\t")
        next(reader)

        # si tu CSV tiene header, descomenta esto:
        # next(reader)

        for row in reader:
            parsed_row = []

            for value in row:
                parsed_row.append(value.encode("utf-8"))

            packed = struct.pack(record_format, *parsed_row)
            heap_file.write(packed)

        # padding final para completar la última página
        current_size = heap_file.tell()
        remainder = current_size % page_size

        if remainder != 0:
            heap_file.write(b"\x00" * (page_size - remainder))


# Lee una página del heap file y retorna sus registros
def read_page(
    heap_path: str,
    page_id: int,
    record_format: str,
    page_size: int
) -> list[tuple]:

    records = []
    record_size = struct.calcsize(record_format)
    max_records = page_size // record_size

    with open(heap_path, "rb") as heap_file:
        offset = page_id * page_size
        heap_file.seek(offset)

        page_data = heap_file.read(page_size)

        if not page_data:
            return []

        for i in range(max_records):
            start = i * record_size
            end = start + record_size

            record_bytes = page_data[start:end]

            # evitar leer padding vacío
            if record_bytes == b"\x00" * record_size:
                continue

            unpacked = struct.unpack(record_format, record_bytes)

            # decodificar bytes -> string limpia
            clean_record = tuple(
                field.decode("utf-8").strip("\x00").strip()
                if isinstance(field, bytes)
                else field
                for field in unpacked
            )

            records.append(clean_record)

    return records


# Escribe una página completa en el heap file
def write_page(
    heap_path: str,
    page_id: int,
    records: list[tuple],
    record_format: str,
    page_size: int
):
    record_size = struct.calcsize(record_format)
    max_records = page_size // record_size

    if len(records) > max_records:
        raise ValueError("Demasiados registros para una sola página.")

    page_data = b""

    for record in records:
        encoded_record = []

        for value in record:
            if isinstance(value, str):
                encoded_record.append(value.encode("utf-8"))
            else:
                encoded_record.append(value)

        page_data += struct.pack(record_format, *encoded_record)

    # padding hasta completar la página
    if len(page_data) < page_size:
        page_data += b"\x00" * (page_size - len(page_data))

    with open(heap_path, "r+b") as heap_file:
        offset = page_id * page_size
        heap_file.seek(offset)
        heap_file.write(page_data)


# Retorna el número total de páginas del heap file
def count_pages(heap_path: str, page_size: int) -> int:
    if not os.path.exists(heap_path):
        raise FileNotFoundError(f"No existe el archivo: {heap_path}")

    file_size = os.path.getsize(heap_path)

    if file_size % page_size != 0:
        raise ValueError(
            "El heap file está corrupto o no está alineado a páginas."
        )

    return file_size // page_size
