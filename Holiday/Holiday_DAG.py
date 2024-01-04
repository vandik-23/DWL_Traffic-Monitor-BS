import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


def start():
    logging.info('Starting the DAG daily holidayupdate')

# In the below function we clean the data and fetch all future dates from our datalake and update the future dated holidays in our datawarehouse tables

def transformation_holiday_data():
    
    # SQL statements defined:
    query_cleaning = "UPDATE holidays SET type_holiday = 'School' WHERE type_holiday = 'EndOfLessons';"

    copy_holidays_to_holiday_temp = "CREATE TABLE IF NOT EXISTS holidays_temp AS SELECT * FROM holidays;"

    create_table_holiday_simple = """
    create table if not exists holiday_simple(
        code_holiday SERIAL primary key, 
        id_holiday VARCHAR(1000) NOT null, 
        date_holiday DATE NOT NULL, 
        isocode_holiday VARCHAR(5),
        name_holiday VARCHAR(250),
        type_public BOOL,
        type_school BOOL,
        nationwide BOOL,
        FR_ZB_ST BOOL,
        CH_BS BOOL,
        CH_BL BOOL,
        DE_BW BOOL);"""

    add_unique_constraint = """
    ALTER TABLE holiday_simple 
    ADD CONSTRAINT uc_date_id UNIQUE (date_holiday, id_holiday);"""

    update_data_holiday_simple ="""
    INSERT INTO holiday_simple (date_holiday, id_holiday, isocode_holiday, name_holiday, type_public, type_school, nationwide, FR_ZB_ST, CH_BS, CH_BL, DE_BW)
    SELECT
        gs.date,
        CAST(ht.id_holiday AS VARCHAR),
        ht.isocode_holiday,
        ht.name_holiday,
        CASE WHEN ht.type_holiday = 'Public' THEN TRUE ELSE FALSE END AS type_public,
        CASE WHEN ht.type_holiday = 'School' THEN TRUE ELSE FALSE END AS type_school,
        ht.nationwide,
        CASE WHEN ht.isocode_holiday = 'FR' AND ht.subdivisions_holiday && ARRAY['FR-ZB-ST'] THEN true
            WHEN ht.nationwide = true AND ht.isocode_holiday = 'FR' THEN true ELSE FALSE END AS FR_ZB_ST,
        CASE WHEN ht.isocode_holiday = 'CH' AND ht.subdivisions_holiday && ARRAY['CH-BS'] THEN true
            WHEN ht.nationwide = true AND ht.isocode_holiday = 'CH' THEN true ELSE FALSE END AS CH_BS,
        CASE WHEN ht.isocode_holiday = 'CH' AND ht.subdivisions_holiday && ARRAY['CH-BL'] THEN true
            WHEN ht.nationwide = true AND ht.isocode_holiday = 'CH' THEN true ELSE FALSE END AS CH_BL,
        CASE WHEN ht.isocode_holiday = 'DE' AND ht.subdivisions_holiday && ARRAY['DE_BW'] THEN true
            WHEN ht.nationwide = true AND ht.isocode_holiday = 'DE' THEN true ELSE FALSE END AS DE_BW
    FROM
        holidays_temp ht
    JOIN LATERAL (
        SELECT generate_series(start_date, end_date, interval '1 day')::date AS date
    ) gs ON true
    WHERE
        ht.subdivisions_holiday && ARRAY['FR-ZB-ST', 'CH-BS', 'CH-BL', 'DE-BW']
        OR ht.nationwide = true
    ON CONFLICT (date_holiday, id_holiday) DO UPDATE
        SET
            isocode_holiday = EXCLUDED.isocode_holiday,
            name_holiday = EXCLUDED.name_holiday,
            type_public = EXCLUDED.type_public,
            type_school = EXCLUDED.type_school,
            nationwide = EXCLUDED.nationwide,
            FR_ZB_ST = EXCLUDED.FR_ZB_ST,
            CH_BS = EXCLUDED.CH_BS,
            CH_BL = EXCLUDED.CH_BL,
            DE_BW = EXCLUDED.DE_BW;"""

    create_table_holiday_simple_agg = """
    CREATE TABLE IF NOT EXISTS holiday_simple_agg (
    date_holiday DATE PRIMARY KEY,
    name_holiday VARCHAR(250),
    type_public BOOL,
    type_school BOOL,
    nationwide BOOL,
    FR_ZB_ST BOOL,
    CH_BS BOOL,
    CH_BL BOOL,
    DE_BW BOOL);"""
    
    update_data_holiday_simple_agg ="""
    INSERT INTO holiday_simple_agg (date_holiday, name_holiday, type_public, type_school, nationwide, FR_ZB_ST, CH_BS, CH_BL, DE_BW)
    SELECT
        date_holiday,
        MIN(name_holiday) AS name_holiday,
        bool_or(type_public) AS type_public,
        bool_or(type_school) AS type_school,
        bool_or(nationwide) AS nationwide,
        bool_or(FR_ZB_ST) AS FR_ZB_ST,
        bool_or(CH_BS) AS CH_BS,
        bool_or(CH_BL) AS CH_BL,
        bool_or(DE_BW) AS DE_BW
    FROM holiday_simple hs 
    GROUP BY date_holiday
    ON CONFLICT (date_holiday) DO NOTHING;"""

    selectall_holiday_simple_agg = "SELECT * FROM holiday_simple_agg;"
    empty_table_holiday_simple_agg = "TRUNCATE TABLE holiday_simple_agg;"

    remove_foreignkey = """
    DO $$ 
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM information_schema.table_constraints
            WHERE constraint_name = 'fk_datetime_holiday'
            AND table_name = 'datetime'
            AND constraint_type = 'FOREIGN KEY'
        ) THEN
            -- Drop the foreign key constraint
            EXECUTE 'ALTER TABLE datetime DROP CONSTRAINT fk_datetime_holiday;';
        END IF;
    END $$;"""

    # Daily update future data:
    selectall_holiday_simple_agg_future = "SELECT * FROM holiday_simple_agg WHERE date_holiday > CURRENT_DATE;"
    empty_table_holiday_simple_agg_future = "DELETE FROM holiday_simple_agg WHERE date_holiday > CURRENT_DATE;"

    # generate missing dates function:

    generate_missing_dates = """
    CREATE OR REPLACE FUNCTION insert_missing_dates_with_defaults()
    RETURNS VOID AS
    $$
    BEGIN
        INSERT INTO holiday_simple_agg (date_holiday, name_holiday, type_public, type_school, nationwide, FR_ZB_ST, CH_BS, CH_BL, DE_BW)
        SELECT
            generate_series::DATE,
            'no holiday' AS name_holiday,
            FALSE AS type_public,
            FALSE AS type_school,
            FALSE AS nationwide,
            FALSE AS FR_ZB_ST,
            FALSE AS CH_BS,
            FALSE AS CH_BL,
            FALSE AS DE_BW
        FROM generate_series('2020-12-19'::DATE, CURRENT_DATE+2, '1 day'::INTERVAL) AS generate_series
        WHERE NOT EXISTS (
            SELECT 1
            FROM holiday_simple_agg
            WHERE date_holiday = generate_series::DATE
        );
    END;
    $$
    LANGUAGE plpgsql;"""

    generate_missing_dates_select = "SELECT insert_missing_dates_with_defaults()"

    add_foreignkey = """
    DO $$ 
    BEGIN 
        -- Check if the constraint exists
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.table_constraints
            WHERE constraint_name = 'fk_datetime_holiday'
        ) THEN
            -- Add the foreign key constraint
            EXECUTE 'ALTER TABLE datetime
                    ADD CONSTRAINT fk_datetime_holiday
                    FOREIGN KEY (date)
                    REFERENCES holiday_simple_agg(date_holiday)';
        END IF;
    END $$;"""



# connecting to datalake2 fetch data:
    try:
        pg_hook = PostgresHook(postgres_conn_id="rds_datalake_holiday", schema="datalake2")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query_cleaning)
        connection.commit()
        logging.info("Records cleaned successfully!")
        cursor.execute(copy_holidays_to_holiday_temp)
        logging.info('holidays_temp table created')
        cursor.execute(create_table_holiday_simple)
        logging.info('holiday_simple table created')
        connection.commit()
        try:
            cursor.execute(add_unique_constraint)
            logging.info('unique constraint added')
        except:
            connection.rollback()
            logging.info('unique constraint existed')
        cursor.execute(update_data_holiday_simple)
        logging.info('holiday_simple table updated')
        cursor.execute(create_table_holiday_simple_agg)
        logging.info('holiday_simple_agg table created')
        cursor.execute(update_data_holiday_simple_agg)
        logging.info('holiday_simple_agg table updated')
        # Commit the changes
        connection.commit()
        logging.info('holiday data cleaned and commited')

    # Executed at the first initial run -> than changed to daily update - change only future data
        #cursor.execute(selectall_holiday_simple_agg)

    # Executed in daily updates
        cursor.execute(selectall_holiday_simple_agg_future)

        records = cursor.fetchall()
        for record in records:
            logging.info(record)
    except Exception as e:
        logging.info(f"Error getting records: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#Connecting and uploading all data to the datawarehouse:
    try:
        pg_hook = PostgresHook(postgres_conn_id="rds_datawarehouse", schema="datawarehouse1")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        connection.set_session(autocommit=True)
        cursor.execute(create_table_holiday_simple_agg)
        logging.info('holiday_simple_agg table created')
        cursor.execute(remove_foreignkey)
        logging.info('foreign key fk_datetime_holiday removed')

    # Executed at the first initial run -> than changed to daily update - change only future data
        #cursor.execute(empty_table_holiday_simple_agg)

    # Executed in daily updates
        cursor.execute(empty_table_holiday_simple_agg_future)

         # Execute an INSERT query to insert data into the destination table
        count=0
        for record in records:
            cursor.execute(f'INSERT INTO holiday_simple_agg (date_holiday, name_holiday, type_public, type_school, nationwide, FR_ZB_ST, CH_BS, CH_BL, DE_BW) VALUES (%s, %s, %s, %s,%s, %s,%s, %s, %s)', record)
            count = count+1
            logging.info('updated record '+str(count))
        logging.info('holiday_simple_agg table uploaded to dw')
        cursor.execute(generate_missing_dates)
        cursor.execute(generate_missing_dates_select)
        logging.info('holiday_simple_agg table missing dates added')
        cursor.execute(add_foreignkey)
        logging.info('foreign key fk_datetime_holiday added')
        
        # Commit the changes
        connection.commit()
        logging.info('commited and upload finished')
    except Exception as e:
        logging.info(f"Error upodating records: {str(e)}")


# Define the start date and schedule interval
schedule_interval = datetime.timedelta(days=1)
start_date = datetime.datetime(2023, 12, 12, 0, 30)-schedule_interval
#schedule_interval = None  we only run this dag once at a manual triger to upload historical data



# Create the DAG
dag = DAG(
    'session1.Holiday_DAG',
    schedule_interval=schedule_interval,
    start_date=start_date,
    #catchup=True
)

start_task = PythonOperator(
   task_id="start_task",
   python_callable=start,
   dag=dag
)

transformation_holiday_data_task = PythonOperator(
    task_id="transformation_holiday_data",
    python_callable=transformation_holiday_data,
    dag=dag
)

start_task >> transformation_holiday_data_task

