import pymysql
import pandas as pd

# Conectar a MySQL con PyMySQL
print("Intentando conectar a MySQL...")
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="0919",
    database="murder_mystery",
    port=3306
)
cursor = conn.cursor()

print("¡Conexión exitosa a MySQL!")

# Limpieza de datos
csv_files = {
    'person': 'C:\\Users\\LENOVO\\Documentos\\ANALISIS DE DATOS\\POWER BI\\person.csv',
    'drivers_license': 'C:\\Users\\LENOVO\\Documentos\\ANALISIS DE DATOS\\POWER BI\\drivers_license.csv',
    'crime_scene_report': 'C:\\Users\\LENOVO\\Documentos\\ANALISIS DE DATOS\\POWER BI\\crime_scene_report.csv',
    'get_fit_now_member': 'C:\\Users\\LENOVO\\Documentos\\ANALISIS DE DATOS\\POWER BI\\get_fit_now_member.csv',
    'facebook_event_checkin': 'C:\\Users\\LENOVO\\Documentos\\ANALISIS DE DATOS\\POWER BI\\facebook_event_checkin.csv',
    }

for table, path in csv_files.items():
    print(f"Limpieza de {table}...")
    df = pd.read_csv(path, on_bad_lines='skip')


# Quitar datos nulos de las tablas y duplicados
    df.drop_duplicates(inplace=True)
    df.dropna(how='all', inplace=True)

 # Conversión de fechas si existen
    for col in df.columns:
        if "date" in col or "dob" in col:
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass

  # Eliminar columnas vacías si existen
    df.dropna(axis=1, how='all', inplace=True)

# Subir datos limpios a MySQL
    print(f"Ingresando {len(df)} filas en {table}...")
    columns = ','.join(df.columns)
placeholders = ','.join(['%s'] * len(df.columns))  

for _, row in df.iterrows():
    values = tuple(row.fillna(value=""))

    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    try:
        cursor.execute(sql, values)
    except pymysql.Error as err:
        print(f"Error en fila {row}: {err}")
print(f"Datos de {table} subidos exitosamente.")

conn.commit()
cursor.close()
conn.close()

print("Limpieza e inserción completadas.")
# Cerrar la conexión
  
