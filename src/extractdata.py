import zipfile
import argparse
import cdsapi
import xarray as xr
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import calendar
import sys
import netCDF4
print("🐍 Ejecutando con:", sys.executable)
print("📦 netCDF4 versión:", netCDF4.__version__)

CARPETA_TEMPORAL_FINAL = "datos_extraidos_finales"
CARPETA_COMPRIMIDO_FINAL = "datos_comprimidostemporales_finales"
os.makedirs(CARPETA_TEMPORAL_FINAL, exist_ok=True) # Crea la carpeta si no existe
os.makedirs(CARPETA_COMPRIMIDO_FINAL, exist_ok=True) # Crea la carpeta si no existe

LOCALIDADES = {
    "Murcia Capital": (37.987, -1.130), "Cartagena": (37.605, -0.986),
    "Lorca": (37.671, -1.696), "Molina de Segura": (38.053, -1.213),
    "Alcantarilla": (37.973, -1.211), "Torre-Pacheco": (37.744, -0.953),
    "Águilas": (37.406, -1.583), "Cieza": (38.241, -1.417),
    "Yecla": (38.614, -1.114), "San Javier": (37.805, -0.834),
    "Mazarrón": (37.599, -1.314), "Totana": (37.771, -1.500),
    "Jumilla": (38.477, -1.332), "Caravaca de la Cruz": (38.106, -1.861),
    "Archena": (38.118, -1.300)
}
AREA_MURCIA = [38.8, -2.2, 37.3, -0.6]
HORAS = ['00:00', '03:00', '06:00', '09:00', '12:00', '15:00', '18:00', '21:00']


def descargar_cams(anio, mes): 
    c = cdsapi.Client()
    last_day = calendar.monthrange(anio, mes)[1]
    fecha_inicio = f"{anio}-{str(mes).zfill(2)}-01"
    fecha_fin = f"{anio}-{str(mes).zfill(2)}-{last_day}"
    rango_fechas = f"{fecha_inicio}/{fecha_fin}"
    file_ultravioleta = os.path.join(CARPETA_TEMPORAL_FINAL, f"uv_{anio}_{str(mes).zfill(2)}.nc")

    # MANTENER: Lógica de control de archivos
    if os.path.exists(file_ultravioleta):
        print(f"⏩ [CAMS] {anio}-{mes:02d} ya existe. Saltando descarga.")
    else:
        print(f"📡 [CAMS] Descargando {anio}-{mes:02d} (Rango: {rango_fechas})...")
        c.retrieve('cams-global-atmospheric-composition-forecasts', {
                "variable": ["uv_biologically_effective_dose", "uv_biologically_effective_dose_clear_sky"],
                "date": rango_fechas,
                "time": ["00:00"],
                "leadtime_hour": ["0", "3", "6", "9", "12", "15", "18", "21"],
                "type": ["forecast"],
                "data_format": "netcdf_zip",
                "area": AREA_MURCIA
        }).download(file_ultravioleta)
        abrir_dataset_comprimido(file_ultravioleta)
    return file_ultravioleta

def descargar_era5(anio, mes): 
    c = cdsapi.Client(url='https://cds.climate.copernicus.eu/api', key='48ea6bed-9ba2-4b99-91c5-1f451351f093')
    last_day = calendar.monthrange(anio, mes)[1]
    lista_dias = [f"{d:02d}" for d in range(1, last_day + 1)]
    lista_horas = [f"{h:02d}:00" for h in range(24)]
    file_era5 = os.path.join(CARPETA_TEMPORAL_FINAL, f"era5_{anio}_{str(mes).zfill(2)}.nc")

    # MANTENER: Lógica de control de archivos
    if os.path.exists(file_era5):
        print(f"⏩ [ERA5] {anio}-{mes:02d} ya existe. Saltando descarga.")
    else:
        print(f"📡 [ERA5] Descargando {anio}-{mes:02d}...")
        c.retrieve('reanalysis-era5-land', {
            'variable': ['total_precipitation', '2m_temperature', '2m_dewpoint_temperature', 'surface_pressure', 'surface_solar_radiation_downwards', '10m_u_component_of_wind', '10m_v_component_of_wind'],
            'year': str(anio), 'month': str(mes).zfill(2), 'day': lista_dias, 'time': lista_horas,
            'area': AREA_MURCIA, 'data_format': 'netcdf_zip'
        }).download(file_era5)
        abrir_dataset_comprimido(file_era5)
    return file_era5

def descargar_eac4(anio, mes):
    c = cdsapi.Client()
    last_day = calendar.monthrange(anio, mes)[1]
    rango_fecha = f"{anio}-{str(mes).zfill(2)}-01/{anio}-{str(mes).zfill(2)}-{last_day}"
    file_gases = os.path.join(CARPETA_TEMPORAL_FINAL, f"eac4_gases_{anio}_{str(mes).zfill(2)}.nc")
    file_clima = os.path.join(CARPETA_TEMPORAL_FINAL, f"eac4_clima_{anio}_{str(mes).zfill(2)}.nc")

    # MANTENER: Lógica segmentada (Gases y Clima por separado)
    if os.path.exists(file_gases):
        print(f"⏩ [EAC4-Gases] {anio}-{mes:02d} ya existe. Saltando.")
    else:
        print(f"📡 [EAC4-Gases] Descargando {anio}-{mes:02d}...")
        c.retrieve('cams-global-reanalysis-eac4', {'format': 'netcdf', 'variable': ['carbon_monoxide', 'nitrogen_dioxide', 'ozone', 'sulphur_dioxide'], 'pressure_level': '1000', 'date': rango_fecha, 'time': HORAS, 'area': AREA_MURCIA}, file_gases)

    if os.path.exists(file_clima):
        print(f"⏩ [EAC4-Clima] {anio}-{mes:02d} ya existe. Saltando.")
    else:
        print(f"📡 [EAC4-Clima] Descargando {anio}-{mes:02d}...")
        c.retrieve('cams-global-reanalysis-eac4', {'format': 'netcdf', 'variable': ['particulate_matter_10um', 'particulate_matter_2.5um', 'uv_index_clear_sky', '2m_temperature', '2m_relative_humidity', '10m_u_component_of_wind', '10m_v_component_of_wind', 'surface_pressure', 'total_precipitation', 'surface_solar_radiation_downwards', 'total_cloud_cover', 'boundary_layer_height', 'total_aerosol_optical_depth_550nm'], 'date': rango_fecha, 'time': HORAS, 'area': AREA_MURCIA}, file_clima)

    return file_gases, file_clima

def abrir_dataset_comprimido(ruta_archivo):
    """
    Gestiona la descompresión, reubicación y apertura de ficheros .nc 
    recibidos en formato .zip desde la API de Copernicus.
    """
    # 1. Definir rutas de trabajo
    # Obtenemos la carpeta contenedora y el nombre del archivo sin extensión
    carpeta_origen = os.path.dirname(ruta_archivo)
    nombre_base = os.path.splitext(os.path.basename(ruta_archivo))[0]

    # Esta es la ruta donde queremos que finalmente resida nuestro archivo .nc extraído
    ruta_final_definitiva = os.path.join(CARPETA_TEMPORAL_FINAL, f"{nombre_base}.nc")

    # 2. Descompresión del fichero ZIP
    with zipfile.ZipFile(ruta_archivo, 'r') as zip_ref:
        # Definimos una carpeta temporal para volcar el contenido
        temp_extract_dir = os.path.join(CARPETA_COMPRIMIDO_FINAL, "temp_extract")
        zip_ref.extractall(temp_extract_dir)

        # 3. Localización inteligente del archivo .nc
        # Buscamos en todo el árbol de directorios extraído (por si viene dentro de carpetas internas)
        archivos_nc = [os.path.join(root, file) 
                       for root, dirs, files in os.walk(temp_extract_dir) 
                       for file in files if file.endswith('.nc')]

        if not archivos_nc: 
            raise FileNotFoundError("No se encontró ningún archivo con extensión .nc dentro del ZIP.")

        # 4. Consolidación del archivo extraído
        import shutil
        # Movemos el archivo encontrado a nuestra carpeta de trabajo definitiva
        shutil.move(archivos_nc[0], ruta_final_definitiva)

        # 5. Limpieza de archivos temporales
        # Eliminamos el directorio temporal creado para evitar basura en disco
        shutil.rmtree(temp_extract_dir)

    # 6. Retorno como Dataset de Xarray
    # Abrimos con engine 'netcdf4' que es el estándar para ficheros CAMS/ERA5
    return xr.open_dataset(ruta_final_definitiva, engine='netcdf4')


def extraer_datos_punto(ruta_nc, lat, lon):
    """
    Extrae series temporales de archivos NetCDF estándar (Reanálisis).
    Maneja dinámicamente las dimensiones y calcula las coordenadas más cercanas.
    """
    from netCDF4 import Dataset
    ds = Dataset(ruta_nc, 'r')

    # 1. Detección dinámica de coordenadas
    # Busca 'lat'/'latitude' y 'lon'/'longitude' independientemente del nombre del archivo
    lat_keys = [v for v in ds.variables if v.lower() in ['lat', 'latitude']]
    lon_keys = [v for v in ds.variables if v.lower() in ['lon', 'longitude']]
    lat_name, lon_name = lat_keys[0], lon_keys[0]

    # Calculamos la distancia mínima para encontrar el índice del punto geográfico más cercano
    lats, lons = ds.variables[lat_name][:], ds.variables[lon_name][:]
    lat_idx, lon_idx = (np.abs(lats - lat)).argmin(), (np.abs(lons - lon)).argmin()

    # 2. Detección de tiempo
    # Busca la variable que contenga 'time' y extrae la referencia base (units)
    time_name = next((v for v in ds.variables if 'time' in v.lower()), None)
    time_var = ds.variables[time_name]

    # Convertimos las unidades de tiempo a un objeto fecha base usando split('since')
    ref_date = pd.to_datetime(time_var.units.split('since')[-1].strip())
    factor = 24.0 if 'days' in time_var.units.lower() else (1/3600.0 if 'seconds' in time_var.units.lower() else 1.0)

    # 3. Detectar 'step' o 'leadtime' 
    # Identifica si el archivo es Forecast (tiene steps) o Reanálisis (paso directo)
    step_name = next((v for v in ds.variables if v in ['leadtime_hour', 'step', 'time1']), None)
    steps = ds.variables[step_name][:] if step_name else [0]

    all_data = []

    # Lógica de extracción adaptativa
    for i_t in range(time_var.shape[0]):
        base_h = float(time_var[i_t]) * factor
        for i_s, step in enumerate(steps):
            # El "tiempo real" es la suma del tiempo base (ref + base_h) + avance de predicción
            real_time = (ref_date + pd.Timedelta(hours=base_h + float(step))).round('h')
            row = {'time': real_time}

            for var_name in ds.variables:
                # Excluimos metadatos y coordenadas para extraer solo los valores físicos
                if var_name not in [lat_name, lon_name, time_name, step_name, 'level', 'expver', 'number']:
                    var_data = ds.variables[var_name]

                    # El sensor "inteligente" de dimensiones
                    # Si es (time, step, lat, lon) es un archivo Forecast
                    if var_data.ndim == 4: 
                        val = var_data[i_t, i_s, lat_idx, lon_idx]
                    # Si es (time, lat, lon) es un archivo de Reanálisis directo
                    elif var_data.ndim == 3:
                        if i_s == 0: val = var_data[i_t, lat_idx, lon_idx]
                        else: continue
                    else: continue

                    row[var_name] = float(val)
            all_data.append(row)

    ds.close()

    # Devolvemos un DataFrame limpio, eliminando duplicados si el merge generó solapes
    return pd.DataFrame(all_data).drop_duplicates(subset=['time'])

def extraer_datos_puntouv(ruta_nc, lat, lon):
    """
    Extrae series temporales de archivos CAMS Forecast (UV).
    Maneja la estructura 2D de 'valid_time' y las variables 'Geo2D' (uvbed, uvbedcs).
    """
    from netCDF4 import Dataset, num2date
    ds = Dataset(ruta_nc, 'r')

    # 1. Identificar coordenadas más cercanas
    # Usamos np.argmin sobre la diferencia absoluta para encontrar el pixel (punto) más próximo
    lat_arr = ds.variables['latitude'][:]
    lon_arr = ds.variables['longitude'][:]
    lat_idx = (np.abs(lat_arr - lat)).argmin()
    lon_idx = (np.abs(lon_arr - lon)).argmin()

    # 2. Acceso a las variables de tiempo y datos
    # 'valid_time' es una matriz 2D (ref_time, step) que mapea cada paso a una fecha exacta
    v_time = ds.variables['valid_time']
    # 'uvbed' y 'uvbedcs' contienen los datos físicos
    v_bed = ds.variables['uvbed']
    v_bedcs = ds.variables['uvbedcs']

    # 3. Conversión de tiempo 2D
    # num2date interpreta correctamente las unidades del NetCDF y el calendario ('standard')
    # Convierte el array multidimensional de tiempo a un array de objetos cftime
    times = num2date(v_time[:], units=v_time.units, calendar=getattr(v_time, 'calendar', 'standard'))

    all_data = []

    # 4. Iteración sobre las dimensiones temporales (i:forecast_reference_time, j:forecast_period)
    # v_time.shape[0] son los lanzamientos, v_time.shape[1] son los pasos horarios
    for i in range(v_time.shape[0]):
        for j in range(v_time.shape[1]):

            # 5. Extracción de datos físicos
            # Intentamos acceder asumiendo una estructura estándar de 4D (i, j, lat, lon)
            # Si el archivo está internamente aplanado, el 'try-except' maneja la excepción
            try:
                val = float(v_bed[j, i, lat_idx, lon_idx])
                val_cs = float(v_bedcs[j, i, lat_idx, lon_idx])
            except (IndexError, ValueError):
                # Fallback: acceso simplificado si la estructura no incluye dimensiones lat/lon explícitas
                val = float(v_bed[j, i]) 
                val_cs = float(v_bedcs[j, i])
            
            # 6. Conversión de tiempo científico a estándar (Timestamp)
            # str() convierte cftime -> ISO string, y pd.to_datetime lo hace objeto manipulable
            real_time = pd.to_datetime(str(times[i, j])).round('h')

            all_data.append({
                'time': real_time,
                'uv_uvbed': val,
                'uv_uvbedcs': val_cs
            })

    ds.close()

    # 8. Consolidación y eliminación de registros solapados
    # CAMS Forecast suele tener solapes entre lanzamientos, 'drop_duplicates' asegura hora única
    df = pd.DataFrame(all_data).drop_duplicates(subset=['time'])

    print(f"DEBUG: Registros extraídos para esta localidad: {len(df)}")
    return df


def procesar_unificar_unahora(anio, mes, file_gases, file_clima, file_era5, file_ultravioleta):
    """
    Unifica las fuentes de datos atmosféricos para un AÑO y MES específicos.
    Exporta el resultado en un archivo .parquet mensual.
    """
    # 1. Nombre del archivo resultante con mes (zfill asegura 01, 02...)
    output_fn = f"Murcia_Dataset_Completo_{anio}_{str(mes).zfill(2)}.parquet"
    print(f"🏗️ Procesando física atmosférica para {anio}-{str(mes).zfill(2)}...")

    data_total = []

    # Iteramos por localidad
    for nombre, (lat, lon) in LOCALIDADES.items():
        # Extracción
        df_e = extraer_datos_punto(file_era5, lat, lon).add_prefix('e5_').rename(columns={'e5_time': 'time'})
        df_g = extraer_datos_punto(file_gases, lat, lon).add_prefix('g_').rename(columns={'g_time': 'time'})
        df_c = extraer_datos_punto(file_clima, lat, lon).add_prefix('c_').rename(columns={'c_time': 'time'})

        # Extracción UV (ahora con el motor xarray robusto)
        df_uv = extraer_datos_puntouv(file_ultravioleta, lat, lon) 

        # Merge
        df = df_e.merge(df_g, on='time', how='outer')\
                 .merge(df_c, on='time', how='outer')\
                 .merge(df_uv, on='time', how='outer')

        # Conversiones
        df['Estacion'] = nombre
        df['PM10'] = df.get('c_pm10', 0) * 1e9
        df['PM2.5'] = df.get('c_pm2p5', 0) * 1e9
        df['NO2'] = df.get('g_no2', 0) * 1e9
        df['Ozono'] = df.get('g_go3', 0) * 1e9
        df['CO'] = df.get('g_co', 0) * 1e9
        df['SO2'] = df.get('g_so2', 0) * 1e9

        # Temp y Hum
        df['Temp'] = df.get('e5_t2m', 273.15) - 273.15
        T_c = df.get('e5_t2m', 273.15) - 273.15
        Td_c = df.get('e5_d2m', 273.15) - 273.15

        # Presión de vapor de saturación (usando Temp aire)
        es = 6.112 * np.exp((17.67 * T_c) / (T_c + 243.5))
        # Presión de vapor real (usando Temp punto de rocío)
        e = 6.112 * np.exp((17.67 * Td_c) / (Td_c + 243.5)) 
        # Cálculo final
        df['Hum'] = (e / es) * 100
        # Corrección física para eliminar casos residuales > 100%
        df['Hum'] = df['Hum'].clip(0, 100)

        # Otras vars
        df['Presion'] = df.get('e5_sp', 0) / 100
        df['Lluvia'] = df.get('e5_tp', 0) * 1000
        df['Radiacion'] = df.get('e5_ssrd', 0) / 10800
        df['Nubes'] = df.get('c_tcc', 0) * 100

        # Viento
        u = df.get('e5_u10', 0)
        v = df.get('e5_v10', 0)
        df['Viento_Vel'] = np.sqrt(u**2 + v**2) * 3.6
        df['Viento_Dir'] = (np.arctan2(u, v) * 180 / np.pi + 180) % 360

        # Mapeo UV (estandarizado desde extraer_datos_puntouv)
        df['UV_Real'] = df['uv_uvbed']
        df['UV_Cielo_Despejado'] = df['uv_uvbedcs']

        # Filtrado de columnas
        cols_finales = ['time', 'Estacion', 'PM10', 'PM2.5', 'NO2', 'Ozono', 'CO', 'SO2', 
                        'Temp', 'Hum', 'Viento_Vel', 'Viento_Dir', 'Presion', 'Lluvia', 
                        'Radiacion', 'Nubes', 'UV_Real', 'UV_Cielo_Despejado']
        df_limpio = df[[c for c in cols_finales if c in df.columns]].copy()

        data_total.append(df_limpio)
        print(f"✅ Estación {nombre} unificada.")

    # Guardado Final
    if data_total:
        df_final = pd.concat(data_total, ignore_index=True)
        df_final.to_parquet(output_fn, index=False, engine='pyarrow')
        print(f"\n🚀 ÉXITO. Archivo guardado: {output_fn}")
        return df_final
    return None
 

def procesar_unificar_treshoras(df_input, nombre_archivo_original):
    """
    Transforma un dataset horario a tri-horario (3H) mediante una media móvil centrada.
    Se asegura de que el resultado solo contenga las horas 0, 3, 6, 9, 12, 15, 18 y 21.
    """
    df = df_input.copy()

    # 1. Definición del nombre de salida basado en el archivo original
    # Ejemplo: 'datos.parquet' -> 'datos_3h.parquet'
    base, ext = os.path.splitext(nombre_archivo_original)
    output_fn = f"{base}_3h{ext}"

    # Asegurar formato temporal y orden cronológico por estación
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values(['Estacion', 'time'])

    # 2. Clasificación de variables según su comportamiento físico:
    # Las intensidades (media) vs los acumulados (suma)
    cols_mean = ['PM10', 'PM2.5', 'NO2', 'Ozono', 'CO', 'SO2', 'Temp', 'Hum', 
                 'Viento_Vel', 'Viento_Dir', 'Presion', 'Nubes', 'UV_Real', 'UV_Cielo_Despejado']
    cols_sum = ['Lluvia', 'Radiacion']

    # Filtramos columnas que están presentes en el DataFrame para evitar errores
    m = [c for c in cols_mean if c in df.columns]
    s = [c for c in cols_sum if c in df.columns]

    # 3. Algoritmo de agregación móvil (Rolling Window Centrada)
    # Utilizamos una ventana de 3 periodos (3 horas) centrada en el valor actual.
    def rolling_3h(g):
        # Guardamos el nombre de la estación que viene del groupby
        estacion_actual = g['Estacion'].iloc[0] 

        g_indexed = g.set_index('time')

        # Calculamos agregaciones
        res_mean = g_indexed[m].rolling(window=3, center=True, min_periods=1).mean()
        res_sum = g_indexed[s].rolling(window=3, center=True, min_periods=1).sum()

        # Concatenamos las columnas calculadas
        result = pd.concat([res_mean, res_sum], axis=1)

        # Añadimos la columna Estacion de nuevo al DataFrame resultante
        result['Estacion'] = estacion_actual

        return result

    # Aplicamos la función por cada estación de forma independiente
    print(f"🏗️ Calculando medias tri-horarias centradas...")
    df_rolling = df.groupby('Estacion', group_keys=False).apply(rolling_3h).reset_index()

    # 4. Filtrado: Seleccionamos solo las horas objetivo (0, 3, 6, 9, 12, 15, 18, 21)
    # El operador módulo (%) permite filtrar exactamente estas horas.
    df_3h = df_rolling[df_rolling['time'].dt.hour % 3 == 0].copy()

    # 5. Guardado final del archivo procesado
    # Se usa el motor 'pyarrow' para asegurar la integridad de los datos Parquet
    df_3h.to_parquet(output_fn, index=False, engine='pyarrow')

    print(f"\n🚀 ÉXITO. Archivo 3H centrado guardado: {output_fn}")
    return df_3h


def obtener_nombre_unico(ruta):
    """
    Genera un nombre de archivo único para evitar sobrescribir ficheros existentes.
    Si ya existe 'fichero.parquet', devuelve 'fichero(1).parquet', etc.
    """
    if not os.path.exists(ruta):
        return ruta
    base, extension = os.path.splitext(ruta)
    contador = 1
    while True:
        nueva_ruta = f"{base}({contador}){extension}"
        if not os.path.exists(nueva_ruta):
            return nueva_ruta
        contador += 1

def parse_args():
    parser = argparse.ArgumentParser(description="Pipeline atmosférico: Descarga + Procesamiento")
    parser.add_argument("--start", required=True, help="Año-Mes inicio (YYYY-MM)")
    parser.add_argument("--end", required=True, help="Año-Mes fin (YYYY-MM)")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    meses_rango = pd.date_range(start=args.start, end=args.end, freq='MS')

    # FASE 1: DESCARGA MASIVA
    print("🚀 FASE 1: Descargando todos los ficheros necesarios...")
    for fecha in meses_rango:
        anio, mes = fecha.year, fecha.month
        # 1. EAC4 (Con control de error específico para fechas recientes)
        try:
            descargar_eac4(anio, mes)
        except Exception as e:
            print(f"⚠️ EAC4 no disponible para {anio}-{mes:02d} (saltando).")

        # 2. ERA5
        try:
            descargar_era5(anio, mes)
        except Exception as e:
            print(f"❌ Error crítico en descarga ERA5 de {anio}-{mes:02d}: {e}")

        # 3. CAMS (UV)
        try:
            descargar_cams(anio, mes)
        except Exception as e:
            print(f"❌ Error crítico en descarga CAMS (UV) de {anio}-{mes:02d}: {e}")


    # FASE 2: PROCESAMIENTO E INTEGRACIÓN
    print("\n🚀 FASE 2: Procesando ficheros descargados...")

    lista_df_anuales_1h = []
    lista_df_anuales_3h = []

    for fecha in meses_rango:
        anio, mes = fecha.year, fecha.month
        # Rutas de los archivos (ajustadas a la convención mensual)
        f_uv = os.path.join(CARPETA_TEMPORAL_FINAL, f"uv_{anio}_{mes:02d}.nc")
        f_era5 = os.path.join(CARPETA_TEMPORAL_FINAL, f"era5_{anio}_{mes:02d}.nc")
        f_gases = os.path.join(CARPETA_TEMPORAL_FINAL, f"eac4_gases_{anio}_{mes:02d}.nc")
        f_clima = os.path.join(CARPETA_TEMPORAL_FINAL, f"eac4_clima_{anio}_{mes:02d}.nc")

        # Nombres de archivos parquet mensuales
        fn_1h = f"Murcia_{anio}_{mes:02d}_1h.parquet"

        try:
            # 1. Unificación horaria (1H)
            df_1h = procesar_unificar_unahora(anio, mes, f_gases, f_clima, f_era5, f_uv)
            df_1h.to_parquet(fn_1h, index=False)
            lista_df_anuales_1h.append(df_1h) # Guardamos para el consolidado final

            # 2. Transformación tri-horaria (3H)
            df_3h = procesar_unificar_treshoras(df_1h, fn_1h) # Esta función ya guarda el archivo _3h internamente
            lista_df_anuales_3h.append(df_3h)

        except Exception as e:
            print(f"❌ Error en procesamiento de {anio}-{mes:02d}: {e}")

    # FASE 3: CONSOLIDACIÓN FINAL (Generar el archivo que englobe a todos)
    print("\n📦 Generando ficheros consolidados finales...")
    if lista_df_anuales_1h:
        final_1h = pd.concat(lista_df_anuales_1h, ignore_index=True)
        final_1h.to_parquet("Murcia_Dataset_Completo_Global_1H.parquet", index=False)
        print("✅ Generado: Murcia_Dataset_Completo_Global_1H.parquet")

    if lista_df_anuales_3h:
        final_3h = pd.concat(lista_df_anuales_3h, ignore_index=True)
        final_3h.to_parquet("Murcia_Dataset_Completo_Global_3H.parquet", index=False)
        print("✅ Generado: Murcia_Dataset_Completo_Global_3H.parquet")
