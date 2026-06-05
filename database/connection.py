import os
import logging
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# We use a connection pool instead of opening a new database connection
# every time we need to write an event. Opening a connection is expensive
# it involves a network handshake, authentication, and memory allocation
# on the PostgreSQL server. At 10,000 events per minute, doing that for
# every single event would add significant latency and overwhelm PostgreSQL.
#
# A connection pool keeps a set of connections open and ready.
# When we need to write, we borrow a connection from the pool,
# use it, and return it. Fast, efficient, and safe under high load.
#
# min: 2 connections always open and ready even during quiet periods
# max: 10 connections maximum so we never overwhelm PostgreSQL

_pool = None


def get_pool():
    """
    Returns the connection pool, creating it if it does not exist yet.
    This pattern is called lazy initialisation. We do not create the pool
    when the module loads. We create it the first time something asks for it.
    That way if the database is not ready yet we do not crash on startup.
    """
    global _pool

    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            dbname=os.getenv("POSTGRES_DB"),
        )
        logger.info("PostgreSQL connection pool created")

    return _pool


def get_connection():
    """
    Borrows a connection from the pool.
    The caller is responsible for returning it using return_connection()
    after they are done. If they do not return it the pool runs out
    of connections and the system stops being able to write to the database.
    """
    return get_pool().getconn()


def return_connection(conn):
    """
    Returns a borrowed connection back to the pool so others can use it.
    Always call this in a finally block so it runs even if an exception occurs.
    """
    get_pool().putconn(conn)


def close_pool():
    """
    Shuts down the entire pool and closes all connections cleanly.
    Called when the application is shutting down.
    """
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
        logger.info("PostgreSQL connection pool closed")