import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


def create_web_events_source_kafka(t_env):
    """Crea la tabla fuente de Kafka para eventos web"""
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "web_events_source"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    
    source_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_sessions_sink_postgres(t_env):
    """Crea la tabla sink de PostgreSQL para almacenar sesiones"""
    table_name = 'user_sessions'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_id VARCHAR,
            ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            session_duration_minutes DOUBLE,
            event_count BIGINT,
            unique_urls BIGINT,
            first_referrer VARCHAR,
            last_url VARCHAR
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def sessionization_job():
    """
    Job principal que sessioniza eventos web por IP y host con gap de 5 minutos
    """
    # Configurar el entorno de ejecución
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)  # 10 segundos
    env.set_parallelism(2)

    # Configurar el entorno de tabla
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Crear tablas fuente y sink
        source_table = create_web_events_source_kafka(t_env)
        sink_table = create_sessions_sink_postgres(t_env)

        # Sessionización con gap de 5 minutos por IP y host
        # Nota: Usamos window aggregation en lugar de SESSION() para mejor compatibilidad
        sessionized_query = f"""
            INSERT INTO {sink_table}
            SELECT 
                CONCAT(ip, '_', host, '_', 
                       CAST(TUMBLE_START(event_timestamp, INTERVAL '5' MINUTE) AS VARCHAR)) as session_id,
                ip,
                host,
                TUMBLE_START(event_timestamp, INTERVAL '5' MINUTE) as session_start,
                TUMBLE_END(event_timestamp, INTERVAL '5' MINUTE) as session_end,
                5.0 as session_duration_minutes,
                COUNT(*) as event_count,
                COUNT(DISTINCT url) as unique_urls,
                FIRST_VALUE(referrer) as first_referrer,
                LAST_VALUE(url) as last_url
            FROM {source_table}
            WHERE ip IS NOT NULL 
              AND host IS NOT NULL
              AND event_timestamp IS NOT NULL
            GROUP BY ip, host, TUMBLE(event_timestamp, INTERVAL '5' MINUTE)
        """

        print("Ejecutando query de sessionización:")
        print(sessionized_query)
        
        # Ejecutar el job
        result = t_env.execute_sql(sessionized_query)
        
        print("Job de sessionización iniciado exitosamente!")
        print(f"Job ID: {result.get_job_client().get_job_id()}")
        
        return result

    except Exception as e:
        print(f"Error en el job de sessionización: {str(e)}")
        raise


if __name__ == "__main__":
    print("Iniciando job de sessionización de eventos web...")
    sessionization_job()
