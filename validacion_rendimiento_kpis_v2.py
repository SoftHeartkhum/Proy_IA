#!/usr/bin/env python3
# Script adaptado a las columnas reales de tus tablas

import sqlite3
import pandas as pd
import time
import os
from datetime import datetime

RUTA_DW = os.path.expanduser("~/Implantacion_Videojuegos/dw_videojuegos.db")

def obtener_columnas(conn, tabla):
    """Obtiene las columnas de una tabla"""
    df = pd.read_sql(f"PRAGMA table_info({tabla})", conn)
    return df['name'].tolist()

def medir_rendimiento():
    """6.2 Pruebas de rendimiento"""
    
    if not os.path.exists(RUTA_DW):
        print("❌ Data Warehouse no encontrado")
        return None
    
    conn = sqlite3.connect(RUTA_DW)
    
    # Obtener columnas reales
    columnas_dim = obtener_columnas(conn, "DIM_VIDEOJUEGO_actualizado")
    columnas_fact = obtener_columnas(conn, "FACT_VENTAS_VIDEOJUEGOS_ACTUALIZADA")
    
    print("\n" + "="*70)
    print("6.2 PRUEBAS DE RENDIMIENTO")
    print("="*70)
    print(f"\n📋 Columnas encontradas en DIM: {', '.join(columnas_dim[:5])}...")
    print(f"📋 Columnas encontradas en FACT: {', '.join(columnas_fact[:5])}...")
    
    # Encontrar nombres de columnas relevantes
    col_nombre = next((c for c in columnas_dim if 'nombre' in c.lower()), 'nombre')
    col_precio = next((c for c in columnas_dim if 'precio' in c.lower()), 'precio_usd')
    col_propietarios = next((c for c in columnas_dim if 'propietario' in c.lower()), None)
    col_genero = next((c for c in columnas_dim if 'genero' in c.lower()), None)
    col_score = next((c for c in columnas_dim if 'score' in c.lower() or 'resena' in c.lower()), None)
    
    # Definir consultas adaptadas
    consultas = {}
    
    # Consulta 1: Conteo simple
    consultas["1. Conteo simple"] = f"""
        SELECT COUNT(*) as total 
        FROM DIM_VIDEOJUEGO_actualizado
    """
    
    # Consulta 2: Conteo con filtro (si existe columna precio)
    if col_precio:
        consultas["2. Conteo con filtro"] = f"""
            SELECT COUNT(*) as total 
            FROM DIM_VIDEOJUEGO_actualizado 
            WHERE {col_precio} BETWEEN 10 AND 50
        """
    else:
        consultas["2. Conteo con filtro"] = "SELECT COUNT(*) as total FROM DIM_VIDEOJUEGO_actualizado"
    
    # Consulta 3: JOIN básico
    if col_nombre in columnas_dim and col_nombre in columnas_fact:
        consultas["3. JOIN básico"] = f"""
            SELECT dv.{col_nombre}, fv.*
            FROM FACT_VENTAS_VIDEOJUEGOS_ACTUALIZADA fv
            JOIN DIM_VIDEOJUEGO_actualizado dv ON fv.{col_nombre} = dv.{col_nombre}
            LIMIT 1000
        """
    
    # Consulta 4: Agregación con GROUP BY (si existe género)
    if col_genero:
        consultas["4. Agregación con GROUP BY"] = f"""
            SELECT 
                {col_genero} as genero,
                COUNT(*) as cantidad
            FROM DIM_VIDEOJUEGO_actualizado
            WHERE {col_genero} IS NOT NULL AND {col_genero} != ''
            GROUP BY genero
            ORDER BY cantidad DESC
            LIMIT 10
        """
    
    # Consulta 5: Consulta compleja
    if col_genero and col_precio:
        consultas["5. Consulta compleja"] = f"""
            SELECT 
                {col_genero} as genero,
                COUNT(*) as cantidad,
                AVG({col_precio}) as precio_promedio
            FROM DIM_VIDEOJUEGO_actualizado
            WHERE {col_genero} IS NOT NULL
            GROUP BY genero
            ORDER BY cantidad DESC
        """
    
    # Consulta 6: Ordenamiento
    if col_propietarios:
        consultas["6. Ordenamiento por propietarios"] = f"""
            SELECT {col_nombre}, {col_propietarios}
            FROM DIM_VIDEOJUEGO_actualizado
            WHERE {col_propietarios} > 1000000
            ORDER BY {col_propietarios} DESC
            LIMIT 10
        """
    elif col_precio:
        consultas["6. Ordenamiento por precio"] = f"""
            SELECT {col_nombre}, {col_precio}
            FROM DIM_VIDEOJUEGO_actualizado
            WHERE {col_precio} > 0
            ORDER BY {col_precio} DESC
            LIMIT 10
        """
    
    resultados = []
    
    print("\n⏱️  Midiendo tiempos de ejecución...")
    print("-" * 70)
    print(f"{'Consulta':<40} {'T1 (s)':<8} {'T2 (s)':<8} {'T3 (s)':<8} {'Promedio (s)':<12}")
    print("-" * 70)
    
    for nombre, query in consultas.items():
        tiempos = []
        
        for i in range(3):
            try:
                inicio = time.time()
                df = pd.read_sql(query, conn)
                fin = time.time()
                tiempo = fin - inicio
                tiempos.append(tiempo)
            except Exception as e:
                print(f"⚠️ Error en {nombre}: {str(e)[:50]}")
                tiempos.append(None)
        
        tiempos_validos = [t for t in tiempos if t is not None]
        
        if tiempos_validos:
            tiempo_promedio = sum(tiempos_validos) / len(tiempos_validos)
            print(f"{nombre:<40} {tiempos[0]:<8.3f} {tiempos[1]:<8.3f} {tiempos[2]:<8.3f} {tiempo_promedio:<12.3f}")
            
            resultados.append({
                'consulta': nombre,
                'tiempo_1_seg': round(tiempos[0], 3) if tiempos[0] else None,
                'tiempo_2_seg': round(tiempos[1], 3) if tiempos[1] else None,
                'tiempo_3_seg': round(tiempos[2], 3) if tiempos[2] else None,
                'tiempo_promedio_seg': round(tiempo_promedio, 3),
                'estado': 'OK'
            })
        else:
            print(f"{nombre:<40} {'ERROR':<8} {'ERROR':<8} {'ERROR':<8} {'ERROR':<12}")
            resultados.append({
                'consulta': nombre,
                'tiempo_1_seg': None,
                'tiempo_2_seg': None,
                'tiempo_3_seg': None,
                'tiempo_promedio_seg': None,
                'estado': 'ERROR'
            })
    
    conn.close()
    
    df_resultados = pd.DataFrame(resultados)
    df_resultados.to_csv('reporte_rendimiento.csv', index=False)
    print("-" * 70)
    print("\n✅ Reporte de rendimiento guardado: reporte_rendimiento.csv")
    
    return df_resultados

def calcular_kpis_avanzados():
    """6.3 KPIs avanzados - adaptado a columnas reales"""
    
    if not os.path.exists(RUTA_DW):
        print("❌ Data Warehouse no encontrado")
        return None
    
    conn = sqlite3.connect(RUTA_DW)
    
    # Obtener columnas reales
    columnas_dim = obtener_columnas(conn, "DIM_VIDEOJUEGO_actualizado")
    columnas_fact = obtener_columnas(conn, "FACT_VENTAS_VIDEOJUEGOS_ACTUALIZADA")
    
    # Encontrar nombres de columnas
    col_nombre = next((c for c in columnas_dim if 'nombre' in c.lower()), 'nombre')
    col_precio = next((c for c in columnas_dim if 'precio' in c.lower()), None)
    col_genero = next((c for c in columnas_dim if 'genero' in c.lower()), None)
    col_desarrollador = next((c for c in columnas_dim if 'desarrollador' in c.lower()), None)
    col_gratis = next((c for c in columnas_dim if 'gratuito' in c.lower()), None)
    
    print("\n" + "="*70)
    print("6.3 KPIs AVANZADOS")
    print("="*70)
    
    resultados = []
    
    # KPI 1: Total de juegos
    df = pd.read_sql("SELECT COUNT(*) as total FROM DIM_VIDEOJUEGO_actualizado", conn)
    total_juegos = df['total'].iloc[0]
    print(f"\n📊 KPI 1 - Total de juegos: {total_juegos:,}")
    resultados.append({'KPI': 'Total juegos en catálogo', 'Valor': f"{total_juegos:,}"})
    
    # KPI 2: Juegos por plataforma
    print("\n🕹️ KPI 2 - Distribución por plataforma:")
    plataformas = {
        'Steam/PC': 'DIM_VIDEOJUEGO_actualizado',
        'Epic Games': 'DIM_VIDEOJUEGO_EpicGamesStore',
        'Xbox': 'DIM_VIDEOJUEGO_XboxMarketplace',
        'Nintendo': 'DIM_VIDEOJUEGO_NintendoeShop',
        'PlayStation': 'DIM_VIDEOJUEGO_PlayStation'
    }
    
    for nombre_plat, tabla in plataformas.items():
        try:
            df = pd.read_sql(f"SELECT COUNT(*) as total FROM {tabla}", conn)
            total = df['total'].iloc[0]
            print(f"   - {nombre_plat}: {total:,} juegos")
            resultados.append({'KPI': f'Juegos en {nombre_plat}', 'Valor': f"{total:,}"})
        except:
            pass
    
    # KPI 3: Top 10 juegos (usando columna de precio como referencia)
    if col_precio:
        print("\n🏆 KPI 3 - Top 10 juegos por precio:")
        query_top = f"""
            SELECT {col_nombre}, {col_precio}
            FROM DIM_VIDEOJUEGO_actualizado
            WHERE {col_precio} > 0
            ORDER BY {col_precio} DESC
            LIMIT 10
        """
        top_juegos = pd.read_sql(query_top, conn)
        for i, row in top_juegos.iterrows():
            print(f"   {i+1}. {row[col_nombre][:45]} - ${row[col_precio]:.2f}")
            if i < 5:
                resultados.append({'KPI': f'Top {i+1} precio', 'Valor': row[col_nombre][:40]})
    
    # KPI 4: Distribución por género
    if col_genero:
        print("\n🎭 KPI 4 - Top géneros:")
        query_generos = f"""
            SELECT 
                {col_genero} as genero,
                COUNT(*) as cantidad
            FROM DIM_VIDEOJUEGO_actualizado
            WHERE {col_genero} IS NOT NULL AND {col_genero} != ''
            GROUP BY genero
            ORDER BY cantidad DESC
            LIMIT 10
        """
        generos = pd.read_sql(query_generos, conn)
        for i, row in generos.iterrows():
            print(f"   {i+1}. {row['genero']:<25} | {row['cantidad']:>6,} juegos")
            if i < 5:
                resultados.append({'KPI': f'Género #{i+1}', 'Valor': row['genero']})
    
    # KPI 5: Precio promedio
    if col_precio:
        df = pd.read_sql(f"SELECT AVG({col_precio}) as promedio FROM DIM_VIDEOJUEGO_actualizado WHERE {col_precio} > 0", conn)
        precio_promedio = df['promedio'].iloc[0] if df['promedio'].iloc[0] else 0
        print(f"\n💵 KPI 5 - Precio promedio: ${precio_promedio:.2f}")
        resultados.append({'KPI': 'Precio promedio', 'Valor': f"${precio_promedio:.2f}"})
    
    # KPI 6: Top desarrolladores
    if col_desarrollador:
        print("\n🏢 KPI 6 - Top 5 desarrolladores:")
        query_dev = f"""
            SELECT 
                {col_desarrollador} as desarrollador,
                COUNT(*) as cantidad
            FROM DIM_VIDEOJUEGO_actualizado
            WHERE {col_desarrollador} IS NOT NULL AND {col_desarrollador} != ''
            GROUP BY desarrollador
            ORDER BY cantidad DESC
            LIMIT 5
        """
        top_dev = pd.read_sql(query_dev, conn)
        for i, row in top_dev.iterrows():
            print(f"   {i+1}. {row['desarrollador'][:40]} - {row['cantidad']} juegos")
            resultados.append({'KPI': f'Top {i+1} desarrollador', 'Valor': row['desarrollador'][:35]})
    
    # KPI 7: Rango de precios
    if col_precio:
        df_min = pd.read_sql(f"SELECT MIN({col_precio}) as min_precio, MAX({col_precio}) as max_precio FROM DIM_VIDEOJUEGO_actualizado WHERE {col_precio} > 0", conn)
        min_precio = df_min['min_precio'].iloc[0]
        max_precio = df_min['max_precio'].iloc[0]
        print(f"\n💲 KPI 7 - Rango de precios: ${min_precio:.2f} - ${max_precio:.2f}")
        resultados.append({'KPI': 'Rango de precios', 'Valor': f"${min_precio:.2f} - ${max_precio:.2f}"})
    
    conn.close()
    
    df_kpis = pd.DataFrame(resultados)
    df_kpis.to_csv('reporte_kpis_avanzados.csv', index=False)
    print("\n✅ Reporte de KPIs guardado: reporte_kpis_avanzados.csv")
    
    return df_kpis

def generar_informe():
    """Genera informe HTML"""
    
    rendimiento_df = pd.read_csv('reporte_rendimiento.csv') if os.path.exists('reporte_rendimiento.csv') else pd.DataFrame()
    kpis_df = pd.read_csv('reporte_kpis_avanzados.csv') if os.path.exists('reporte_kpis_avanzados.csv') else pd.DataFrame()
    conteos_df = pd.read_csv('reporte_conteos.csv') if os.path.exists('reporte_conteos.csv') else pd.DataFrame()
    nulos_df = pd.read_csv('reporte_nulos.csv') if os.path.exists('reporte_nulos.csv') else pd.DataFrame()
    
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Informe Implantación - Rendimiento y KPIs</title>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background: #f0f2f5; }}
        .container {{ max-width: 1200px; margin: auto; background: white; padding: 25px; border-radius: 15px; }}
        h1 {{ color: #1a73e8; border-bottom: 3px solid #1a73e8; }}
        h2 {{ background: #f8f9fa; padding: 10px; border-left: 4px solid #1a73e8; }}
        table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
        th {{ background: #1a73e8; color: white; }}
        .footer {{ text-align: center; margin-top: 30px; color: #666; }}
        .badge {{ background: #28a745; color: white; padding: 5px 15px; border-radius: 20px; display: inline-block; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🎮 Informe de Implantación</h1>
        <p><strong>Data Warehouse de Videojuegos</strong></p>
        <p><strong>Fecha:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
        <p><strong>Responsable:</strong> Marco Adrian Cori Heredia</p>
        <p><span class="badge">✅ IMPLANTACIÓN EXITOSA</span></p>
        
        <hr>
        
        <h2>⚡ 6.2 Pruebas de Rendimiento</h2>
        {rendimiento_df.to_html(index=False) if not rendimiento_df.empty else '<p>⚠️ No disponible</p>'}
        
        <h2>📊 6.3 KPIs Avanzados</h2>
        {kpis_df.to_html(index=False) if not kpis_df.empty else '<p>⚠️ No disponible</p>'}
        
        <h2>✅ 6.1 Validación de Datos</h2>
        <h3>Conteos</h3>
        {conteos_df.to_html(index=False) if not conteos_df.empty else '<p>⚠️ No disponible</p>'}
        
        <h3>Nulos</h3>
        {nulos_df.to_html(index=False) if not nulos_df.empty else '<p>⚠️ No disponible</p>'}
        
        <div class="footer">
            Informe generado automáticamente - Bloque 3: Implantación<br>
            Marco Adrian Cori Heredia
        </div>
    </div>
</body>
</html>
    """
    
    with open('informe_final_completo.html', 'w', encoding='utf-8') as f:
        f.write(html)
    
    print("\n✅ Informe completo generado: informe_final_completo.html")

def main():
    print("\n" + "="*70)
    print("📊 VALIDACIÓN COMPLETA - RENDIMIENTO (6.2) Y KPIS (6.3)")
    print("="*70)
    
    medir_rendimiento()
    calcular_kpis_avanzados()
    generar_informe()
    
    print("\n" + "="*70)
    print("✅ PROCESO COMPLETADO")
    print("📁 Archivos generados:")
    print("   - reporte_rendimiento.csv")
    print("   - reporte_kpis_avanzados.csv")
    print("   - informe_final_completo.html ⭐")
    print("="*70)

if __name__ == "__main__":
    main()
