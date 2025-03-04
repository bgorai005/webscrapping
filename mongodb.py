
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import pandas as pd

def store_data_in_mongodb(df: pd.DataFrame) -> None:
    """
    Summary:
        # Inserts the scraped data into the MongoDB database.
        # This function creates two collections in the database if not already present.
            - gnews_metadata
            - gnews_images
        # First it inserts the metadata (with unique constraint on headlines) into gnews_metadata collection.
        # Then it inserts the image info into gnews_images collection.
    Args:
        df (pd.DataFrame): The dataframe containing the scraped data.
            - df.columns = ['image_data',
                            'headlines',
                            'image_url',
                            'image_height',
                            'image_width',
                            'article_url',
                            'scrap_timestamp',
                            'published_time']
    Returns: None
    """
    
    try:
        # Connect to MongoDB
        client = MongoClient("mongodb://localhost:27017/")  # Default host and port
        db = client["news_database"]  # Database name
        metadata_collection = db["gnews_metadata"]  # Collection for metadata
        images_collection = db["gnews_images"]      # Collection for images

        print("\n\nConnected to the MongoDB database successfully. \n")

        # Ensure unique index on 'headlines' in gnews_metadata
        metadata_collection.create_index("headlines", unique=True)

        print("\nInserting data into MongoDB. \n")

        # Iterate through each row in the pandas dataframe df
        for _, row in df.iterrows():
            try:
                # Insert metadata into gnews_metadata collection
                metadata_document = {
                    "headlines": row["headlines"],
                    "article_url": row["article_url"],
                    "scrap_timestamp": row["scrap_timestamp"],
                    "published_time": row["published_time"]
                }
                metadata_collection.insert_one(metadata_document)
            except DuplicateKeyError:
                # Handle duplicate headline error
                print(f"Skipping duplicate data: {row['headlines']}")
            else:
                # If metadata insertion is successful, insert corresponding image info
                try:
                    image_document = {
                        "image_data": row["image_data"],
                        "image_url": row["image_url"],
                        "image_height": row["image_height"],
                        "image_width": row["image_width"]
                    }
                    images_collection.insert_one(image_document)
                except Exception as e:
                    # Handle errors during image insertion
                    print(f"INSERT ERROR in gnews_images: {e}")
                else:
                    print(f"Inserted data for headline: {row['headlines']}")

    # Error handling
    except Exception as e:
        print("Connection failed:", str(e))

    # Close the connection at the end
    finally:
        if "client" in locals() and client:
            client.close()
            print("\n\nInserted data. MongoDB connection closed.\n")
    return None    
