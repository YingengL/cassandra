from cassandra.cluster import Cluster

def connect_cassandra():
    # Connect to Cassandra running in Docker
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('streaming_service')
    return session

def get_total_ratings_count(session):
    # Query to get the total count of ratings
    query = "SELECT COUNT(*) FROM movie_ratings"
    result = session.execute(query)
    return result.one()[0]

def get_first_few_ratings(session, limit=20):
    # Query to get the first few rows
    query = f"SELECT * FROM movie_ratings LIMIT {limit}"
    results = session.execute(query)
    return list(results)

if __name__ == "__main__":
    session = connect_cassandra()
    
    # Get total ratings count
    total_count = get_total_ratings_count(session)
    print(f"Total Ratings Count: {total_count}")
    
    # Get the first few rows of data
    first_few_ratings = get_first_few_ratings(session)
    print("First Few Ratings:")
    for row in first_few_ratings:
        print(row)
