from sqlalchemy import create_engine
import pandas as pd
import re
import os
def data_cleaning(filepath: str) -> pd.DataFrame:

    """
    Input: Filepath to data. Function assumes columns exist in data.

    Transforms duration_ms column from milliseconds to minutes:seconds.
    Transforms artists column into string object.
    Removes columns not necessary for analysis questions.
    
    Output: Cleaned dataset, fufilling required parameters.
    """
    # Read in dataset from filepath to DataFrame object
    playlist = pd.read_csv(filepath)

    # Fix duration column
    # Create a series containing duration in minutes
    min = ((playlist['duration_ms'] / (1000 * 60))%60).astype(int)
    
    #Create a series containing duration in seconds
    sec = ((playlist['duration_ms'] / 1000)%60).astype(int)

    # Set index positions for the for loop
    index_pos_val_1 = 0
    index_pos_val_2 = 0

    # Loop through both series to add zeros to beginning of numbers which are only 1-digit long
    for val1, val2 in zip(min, sec):
        if len(str(val1)) == 1:
            val1 = '0' + str(val1)
            min[index_pos_val_1] = val1
            index_pos_val_1 +=1
        else:
            index_pos_val_1 +=1

        if len(str(val2)) == 1:
            val2 = '0' + str(val2)
            sec[index_pos_val_2] = val2
            index_pos_val_2 +=1
        else:
            index_pos_val_2 +=1
    
    # Save new duration column to DataFrame object
    playlist.insert(3, 'duration', min.astype(str).str.cat(sec.astype(str), sep = ":"))

    #Fix artists column
    """
    I chose to fix the artists column as we want clean data. A string of lists isn't an ideal way to
    handle this data. If we transform the values to be strings without random values in them, then separate
    multiple artists with a \, this will make the column more aesthetic.
    """
    # Apply Lambda function to artists column, keeping only letters.
    playlist['artists'] = playlist['artists'].apply(lambda x: re.sub(r"[^a-zA-Z]", " ", x).strip())

    # Replace double spaces in-between two artists with a back-slash
    playlist['artists'] = playlist['artists'].str.replace("    ", " / ")

    # Drop existing duration_ms column    # Drop unneccesary columns from DataFrame
    playlist.drop(columns= ['available_markets', 'episode', 'is_local', 'href', 'id', 'duration_ms', 'disc_number',
                  'external_ids', 'external_urls', 'uri', 'track', 'type', 'track_number'], inplace=True)

    return playlist

def extract_to_sql():
    folder_dir = os.getcwd()
    playlist_1, playlist_2 = (data_cleaning(f'{folder_dir}\playlist1.csv'),
                          data_cleaning(f'{folder_dir}\playlist2.csv'))

    merged_playlist = pd.merge(playlist_1, playlist_2, how='outer')     
    merged_playlist = merged_playlist[merged_playlist.duplicated() == False]
    merged_playlist = merged_playlist[merged_playlist['preview_url'].isna() == False]

    eng = create_engine('sqlite:///playlirst.db', echo = False)
    merged_playlist.to_sql('Playlist_Data', con = eng, if_exists='append')
    
if __name__ == '__main__':
    sql = extract_to_sql()