import csv
from cassandra.cluster import Cluster



def drop_tables(session):
    """
    Drop tables
    :return:
    None
    """

    try:
        session.execute("DROP TABLE IF EXISTS artist_by_title_by_lenght")
        session.execute("DROP TABLE IF EXISTS artist_by_song_by_user")
        session.execute("DROP TABLE IF EXISTS username_by_song")
        print("All tables droped.")
    except Exception as e:
        print(e)


def create_tables(session):
    """
    Create tables
    :return:
    None
    """

    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS artist_by_title_by_lenght (
                sessionId INT,
                itemInSession INT,
                artist TEXT,
                song TEXT,
                length FLOAT,
                PRIMARY KEY (sessionId, itemInSession));
        """)
        session.execute("""
                CREATE TABLE IF NOT EXISTS artist_by_song_by_user (
                    userId INT,
                    sessionId INT,
                    itemInSession INT,
                    artist TEXT,
                    song TEXT,
                    firstName TEXT,
                    lastName TEXT,
                    PRIMARY KEY ((userId, sessionId), itemInSession));
            """)
        session.execute("""
                CREATE TABLE IF NOT EXISTS username_by_song (
                    song TEXT,
                    firstName TEXT,
                    lastName TEXT,
                    userId INT,
                    PRIMARY KEY (song, firstName, lastName, userId));
            """)
        print("All tables created.")
    except Exception as e:
        print(e)


def insert_table(session):
    """
    Insert records to each table selected bellow.
    :return:
    None
    """

    file = 'event_datafile_new.csv'

    with open(file, encoding='utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)  # skip header
        for line in csvreader:
            artist_by_title_by_lenght = "INSERT INTO artist_by_title_by_lenght (sessionId, itemInSession, artist, song, length)"
            artist_by_title_by_lenght = artist_by_title_by_lenght + " VALUES (%s, %s, %s, %s, %s);"
            session.execute(artist_by_title_by_lenght, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))

            artist_by_song_by_user = "INSERT INTO artist_by_song_by_user (userId, sessionId, itemInSession, artist, song, firstName, lastName)"
            artist_by_song_by_user = artist_by_song_by_user + " VALUES (%s, %s, %s, %s, %s, %s, %s);"
            session.execute(artist_by_song_by_user, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))

            username_by_song = "INSERT INTO username_by_song (song, firstName, lastName, userId)"
            username_by_song = username_by_song + " VALUES (%s, %s, %s, %s);"
            session.execute(username_by_song, (line[9], line[1], line[4], int(line[10])))

        print("All rows inserted.")


def select_table(session):
    """
    Select table rows to check if data has been inserted into each table.
    :return:
    None
    """

    # Select artist_by_title_by_lenght's table.
    artist_title = session.execute("""
            SELECT
                artist, song, length
            FROM
                artist_by_title_by_lenght
            WHERE
                sessionId = 338 AND itemInSession = 4
            ALLOW FILTERING;
        """)
    print("---------artist_by_title_by_lenght---------")
    for row in artist_title:
        print(row.artist, "|", row.song, "|", row.length)

    # Select artist_by_song_by_user's table.
    artist_song = session.execute("""
            SELECT
                artist, song, firstname, lastname
            FROM
                artist_by_song_by_user
            WHERE
                userId=10 AND sessionId=182
            ORDER BY
                itemInSession;
            """)
    print("---------artist_by_song_by_user---------")
    for row in artist_song:
        print(row.artist, "|", row.song, "|", row.firstname, "|", row.lastname)

    # Select username_by_song's table.
    username_song = session.execute("""
            SELECT
                firstname, lastname
            FROM
                username_by_song
            WHERE
                song='All Hands Against His Own';
            """)
    print("---------username_by_song---------")
    for row in username_song:
        print(row.firstname, "|", row.lastname)


def main():
    """
    - Set connection to Cassandra DB. Create a Cluster. Create and set a Keyspace.
    - Drop existing tables
    - Create tables
    - Insert records to eache created table.
    - Select records from eache table created to verify if they are inserted correctly.

    :return:
    None
    """

    cluster = Cluster()
    # To establish connection and begin executing queries, need a session
    session = cluster.connect()
    # Create a Keyspace
    session.execute("""
                CREATE KEYSPACE IF NOT EXISTS udacity 
                WITH REPLICATION = 
                { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
                """)
    session.set_keyspace('udacity')
    session.default_fetch_size = 10000000

    drop_tables(session)
    create_tables(session)
    insert_table(session)
    select_table(session)
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
