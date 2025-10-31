import logging
import json
from airflow.hooks.base import BaseHook 

def upsert_player_data(player_json):
    import sqlite3
    import pandas as pd

# Fetch the connection object
    database_conn_id = 'analytics_database'
    connection = BaseHook.get_connection(database_conn_id)
    
    sqlite_db_path = connection.schema
    
    logging.info(f"DEBUG: player_json type: {type(player_json)}")
    logging.info(f"DEBUG: player_json length: {len(str(player_json)) if player_json else 'None'}")
    logging.info(f"DEBUG: sqlite_db_path: {sqlite_db_path}")

    if player_json:

        player_data = json.loads(player_json)
        logging.info(f"DEBUG: Parsed {len(player_data)} players from JSON")
        
        # Use a context manager for the SQLite connection
        with sqlite3.connect(sqlite_db_path) as conn:
            cursor = conn.cursor()
            
            logging.info(f"DEBUG: Connected to database, starting inserts")
            
            # Insert each player record into the 'player' table
            insert_count = 0
            for player in player_data:
                try:
                    cursor.execute("""
                        INSERT INTO player (
                            player_id, gsis_id, first_name, last_name, 
                            position, last_changed_date
                        ) 
                        VALUES (?, ?, ?, ?, ?, ?) 
                        ON CONFLICT(player_id) DO UPDATE
                        SET
                            gsis_id = excluded.gsis_id,
                            first_name = excluded.first_name,
                            last_name = excluded.last_name,
                            position = excluded.position,
                            last_changed_date = excluded.last_changed_date
                    """, (
                        player['player_id'], player['gsis_id'], 
                        player['first_name'], 
                        player['last_name'], 
                        player['position'], 
                        player['last_changed_date']
                    ))
                    insert_count += 1
                    if insert_count % 100 == 0:
                        logging.info(f"DEBUG: Inserted {insert_count} players so far")
                except Exception as e:
                    logging.error(
                        f"Failed to insert player {player.get('player_id', 'unknown')}: {e}")
                    logging.error(f"Player data: {player}")
                    raise
            
            conn.commit()
            logging.info(f"DEBUG: Successfully inserted {insert_count} players total")
                    
    else:
        logging.warning("No player data found.")
        raise ValueError(
            "No player data found. Task failed due to missing data.")
