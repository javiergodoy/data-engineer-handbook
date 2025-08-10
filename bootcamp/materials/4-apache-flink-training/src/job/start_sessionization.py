from sessionization_job import sessionization_job

if __name__ == "__main__":
    """
    Script de inicio para el job de sessionización de Apache Flink
    
    Este script:
    1. Configura el entorno de Flink
    2. Inicia el job de sessionización
    3. Sessioniza eventos por IP y host con gap de 5 minutos
    """
    
    print("=== Iniciando Job de Sessionización de Eventos Web ===")
    print("Configuración:")
    print("- Gap de sesión: 5 minutos")
    print("- Agrupación por: IP address + host")
    print("- Métricas calculadas: duración, eventos por sesión, URLs únicas")
    print("===================================================")
    
    try:
        result = sessionization_job()
        print("Job iniciado exitosamente!")
        
        # El job continúa ejecutándose en streaming mode
        # Para ver los resultados, consulta la tabla 'user_sessions' en PostgreSQL
        
    except Exception as e:
        print(f"Error al iniciar el job: {str(e)}")
        exit(1)
