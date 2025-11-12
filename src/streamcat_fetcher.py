import requests
import pandas as pd
from typing import List, Optional

def _get_single_comid_data(comid: int) -> Optional[pd.DataFrame]:
    base_url = 'https://api.epa.gov/StreamCat/streams/waters_streamcat'
    headers = {
        'accept': 'application/json',
        'comid': str(comid)
    }

    try:
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()
        data = response.json()

        if not data.get('items'):
            return None

        metrics_collection = data['items'][0]
        combined_data = {}
        for metric_list in metrics_collection.values():
            if metric_list:
                combined_data.update(metric_list[0])
        
        if not combined_data:
            return None

        return pd.DataFrame([combined_data])

    except requests.exceptions.RequestException as e:
        print(f"API request failed for COMID {comid}: {e}")
        return None
    except (KeyError, IndexError, TypeError) as e:
        print(f"Failed to process JSON for COMID {comid}: {e}")
        return None

def get_streamcat_data_by_comids(comids: List[int]) -> Optional[pd.DataFrame]:
    """
    Retrieves StreamCat data for a list of COMIDs and concatenates them into a single DataFrame.
    """
    all_dataframes = []
    
    for comid in comids:
        print(f"Fetching data for COMID: {comid}...")
        single_comid_df = _get_single_comid_data(comid)
        
        if single_comid_df is not None:
            all_dataframes.append(single_comid_df)
        else:
            print(f"No data returned for COMID: {comid}")

    if not all_dataframes:
        print("Could not retrieve data for any of the provided COMIDs.")
        return None
    final_df = pd.concat(all_dataframes, ignore_index=True)
    
    return final_df