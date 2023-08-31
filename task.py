import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from memory_profiler import profile
import argparse


def create_sql_table(db_name: str, table_name: str):
    """
    Задание 1. Создает таблицу.
    :param db_name: Название БД.
    :param table_name: Название таблицы.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY,
        timestamp TIMESTAMP,
        player_id INTEGER,
        event_id INTEGER,
        error_id TEXT,
        json_server JSON,
        json_client JSON
    );
    '''
    cursor.execute(create_table_query)

    conn.commit()
    conn.close()


def get_dataframe_for_date(csv_file: str, date_csv: datetime, time_column_name: str = 'timestamp') -> pd.DataFrame:
    """
    Возвращает данные из датафрейма для заданной даты.
    :param csv_file: Название файла.
    :param date_csv: Дата для csv файла.
    :param time_column_name: Название столбца содержащих даты.
    :return: Датафрейм для заданной даты.
    """
    csv_data = pd.read_csv(csv_file, sep=',')
    start_timestamp = date_csv.timestamp()
    end_timestamp = (date_csv + timedelta(days=1)).timestamp()

    return csv_data.loc[(start_timestamp <= csv_data[time_column_name]) & (csv_data[time_column_name] < end_timestamp)]


def join_frames(client_df: pd.DataFrame, server_df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Объединяет данные из двух датафреймов по столбцу.
    :param client_df: Название csv файла клиентов.
    :param server_df: Название csv файла серверов.
    :param column_name: Название столбца.
    :return: Объединенный датафрейм по столбцу.
    """
    return pd.merge(client_df, server_df, on=column_name, how='inner', suffixes=('_client', '_server'))


def exclude_cheaters(df_to_exclude: pd.DataFrame, db_name: str, cheaters_table: str, exclude_column: str,
                     time_column_df: str, time_column_table: str) -> pd.DataFrame:
    """
    Исключает данные из датафрейма, которые содержатся в таблице БД по столбцу exclude_column, если
    у exclude_column time_column_table - это предыдущие сутки или раньше относительно time_column_df.
    :param df_to_exclude: Датафрейм для исключения данных.
    :param db_name: Название базы данных.
    :param cheaters_table: Название таблицы базы данных.
    :param exclude_column: Название столбца по которому ведется исключение.
    :param time_column_df: Название столбца дат в датафрейме.
    :param time_column_table: Название столбца дат в таблице БД.
    :return: Датафрейм с исключенными данными.
    """
    conn = sqlite3.connect(db_name)
    cheaters_df = pd.read_sql_query(f'SELECT * FROM {cheaters_table}', conn)

    # Преобразование ban_time к timestamp
    cheaters_df[time_column_table] = pd.to_datetime(cheaters_df[time_column_table])
    cheaters_df[time_column_table] = cheaters_df[time_column_table].apply(lambda x: x.timestamp())

    # Поиск записей которые есть и в объединенном датафрейме, и в таблице БД по exclude_column.
    cheaters_to_exclude = df_to_exclude.merge(cheaters_df, on=exclude_column, how='inner')
    cheaters_to_exclude = cheaters_to_exclude[
        (cheaters_to_exclude[time_column_df] < cheaters_to_exclude[time_column_table])
    ]

    cheaters_to_exclude = cheaters_to_exclude.drop(time_column_table, axis=1)

    # Удаление из датафрейма записей найденных выше.
    result_df = df_to_exclude.merge(cheaters_to_exclude, how='left', indicator=True)
    result_df = result_df[result_df['_merge'] == 'left_only']
    result_df = result_df.drop('_merge', axis=1)

    return result_df


@profile
def manipulation_with_data(client_csv: str, server_csv: str, join_column: str, date: datetime, db_name: str,
                           cheaters_table: str, exclude_column: str, time_column_df: str, time_column_table: str,
                           new_table: str):
    """
    Задание 2. Выгрузка за определенную дату данных. Объединение их по столбцу.
    Исключение по условию в столбце. Запись в таблицу БД.
    :param client_csv: Название файла клиентских данных.
    :param server_csv: Название файла серверных данных.
    :param join_column: Имя колонки для объединения данных.
    :param date: Дата для которой происходит выгрузка.
    :param db_name: Название БД.
    :param cheaters_table: Название таблицы в БД для которой происходит сравнение.
    :param exclude_column: Имя колонки для которой происходит исключение.
    :param time_column_df: Имя колонки даты в датафрейме.
    :param time_column_table: Имя колонки даты в таблице БД.
    :param new_table: Имя таблицы в которую будут записаны данные.
    """
    excluded = exclude_cheaters(
        join_frames(get_dataframe_for_date(client_csv, date), get_dataframe_for_date(server_csv, date), join_column),
        db_name, cheaters_table, exclude_column, time_column_df, time_column_table)

    df_sql = excluded.rename(columns={'description_client': 'json_client', 'description_server': 'json_server',
                                      'timestamp_server': 'timestamp'})
    df_sql = df_sql.drop('timestamp_client', axis=1)

    conn = sqlite3.connect(db_name)
    df_sql.to_sql(new_table, conn, index=False, if_exists='replace')
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Тестовое задание')
    parser.add_argument("--client_csv", default="client.csv", type=str, help='Название csv файла клиентов.')
    parser.add_argument("--server_csv", default="server.csv", type=str, help='Название csv файла сервера.')
    parser.add_argument("--join_column", default="error_id", type=str,
                        help='Название колонки по которой будет происходить объединение.')

    parser.add_argument("--day", required=True, type=int, help='День даты для выгрузки.')
    parser.add_argument("--month", required=True, type=int, help='Месяц даты для выгрузки.')
    parser.add_argument("--year", required=True, type=int, help='Год даты для выгрузки.')

    parser.add_argument("--db_name", default="cheaters.db", type=str, help='Имя БД для подключения.')
    parser.add_argument("--cheaters_table", default="cheaters", type=str, help='Имя таблицы для исключения из выборки.')
    parser.add_argument("--exclude_column", default="player_id", type=str,
                        help='Название колонки по которой будет проиcходить исключение.')

    parser.add_argument("--time_column_df", default="timestamp_server", type=str,
                        help='Имя колонки даты в датафрейме по которой будет происходить сравнение.')
    parser.add_argument("--time_column_table", default="ban_time", type=str,
                        help='Имя колонки даты в таблице БД по которой будет происходить сравнение.')

    parser.add_argument("--new_table", default="task_table", type=str,
                        help='Имя таблицы в которую будут записаны данные.')

    args = parser.parse_args()
    create_sql_table(args.db_name, args.new_table)

    d = datetime(day=args.day, month=args.month, year=args.year)
    manipulation_with_data(args.client_csv, args.server_csv, args.join_column, d, args.db_name, args.cheaters_table,
                           args.exclude_column, args.time_column_df, args.time_column_table, args.new_table)
