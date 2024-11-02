from cassandra.cluster import Cluster
from dateutil import parser

def connect_cassandra():
    # Connect to Cassandra running in Docker
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('streaming_service')
    return session

def insert_rating(session, log):
    parts = log.strip().split(',')
    rating_time = parser.parse(parts[0])
    user_id = int(parts[1])
    
    # Extract movie title and rating
    path = parts[2]
    movie_info = path.split('/rate/')[1].replace('+', ' ')
    movie_id, rating = movie_info.rsplit('=', 1)
    rating = int(rating)
    
    # Insert into Cassandra
    session.execute("""
        INSERT INTO movie_ratings (user_id, movie_id, rating, rating_time)
        VALUES (%s, %s, %s, %s)
    """, (user_id, movie_id, rating, rating_time))

def load_ratings_from_file(filename, session):
    with open(filename, 'r') as file:
        for line in file:
            insert_rating(session, line)

def query_movie_ratings(session, user_id):
    rows = session.execute("""
        SELECT * FROM movie_ratings WHERE user_id=%s
    """, (user_id,))
    for row in rows:
        print(f"User {row.user_id} rated '{row.movie_name}' with {row.rating} stars at {row.rating_time}")

if __name__ == "__main__":
    session = connect_cassandra()
    # Load and insert ratings from the file
    load_ratings_from_file('ratings.txt', session)
