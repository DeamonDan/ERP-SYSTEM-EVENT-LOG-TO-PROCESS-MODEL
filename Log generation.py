import pandas as pd

# Load the CSV files (replace '/path/to/' with the actual file paths)
services_df = pd.read_csv('/path/to/services.csv') 
engineers_df = pd.read_csv('/path/to/engineers.csv') 
utilizes_df = pd.read_csv('/path/to/utilizes.csv')
technicians_df = pd.read_csv('/path/to/technicians.csv') 
techjobs_df = pd.read_csv('/path/to/techjobs.csv') 
requisitions_df = pd.read_csv('/path/to/requisitions.csv') 
items_df = pd.read_csv('/path/to/items.csv') 
demands_df = pd.read_csv('/path/to/demands.csv') 

def create_initial_event_log(requisitions_df, services_df, engineers_df,
                             utilizes_df, technicians_df, techjobs_df, items_df, demands_df):
    event_log = []
    for _, req in requisitions_df.iterrows():
        # Extracting service and engineer details
        service_details = services_df.loc[services_df['serviceID'] == req['serviceID']].iloc[0]
        engineer_details = engineers_df.loc[engineers_df['engineerID'] == service_details['engineerID']].iloc[0]

        # Extracting utilized items details
        utilized_items = utilizes_df[utilizes_df['requisitionID'] == req['requisitionID']]
        item_ids = utilized_items['itemID'].dropna().unique()
        item_names = items_df[items_df['itemID'].isin(item_ids)]['itemName'].tolist()
        item_names_str = ', '.join(item_names)

        # Extracting demand date
        demand_date = demands_df[demands_df['requisitionID'] == req['requisitionID']]['dateDemand']
        demand_date = demand_date.iloc[0] if not demand_date.empty else ''

        # Creating an event log entry for each requisition
        event_log_entry = {
            "CaseID": req['requisitionID'],
            "Activities": [
                f"{service_details['serviceName']} (Request received)",
                f"{engineer_details['engineerName']} (Assigned)",
                "Technicians (Visits the Place)",
                f"{item_names_str} (Indent the item)",
                "Revisit the location",
                f"Status: {req['status']}"
            ],
            "Timestamps": [
                req['dateRequisition'],
                req['dateTakeup'],
                utilized_items['dateUtilize'].iloc[0] if not utilized_items.empty else '',
                demand_date,
                req['dateResolve'],
                req['dateResolve']
            ]
        }
        event_log.append(event_log_entry)
    return event_log

# Create the initial event log
initial_event_log = create_initial_event_log(requisitions_df, services_df, engineers_df, utilizes_df, technicians_df, techjobs_df, items_df, demands_df)

def reformat_event_log_for_export(event_log):
    reformatted_log = []
    for entry in event_log:
        case_id = entry['CaseID']
        for activity, timestamp in zip(entry['Activities'], entry['Timestamps']):
            reformatted_log.append({
                'CaseID': case_id,
                'Activity': activity,
                'Timestamp': timestamp
            })
    return reformatted_log

# Reformatting the event log
reformatted_event_log = reformat_event_log_for_export(initial_event_log)

# Convert the reformatted event log to a DataFrame
reformatted_event_log_df = pd.DataFrame(reformatted_event_log)


