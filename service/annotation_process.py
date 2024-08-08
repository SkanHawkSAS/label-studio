
import pandas as pd
import logging
import numpy as np
import re
import pytz

huso_horario_utc_menos_5 = pytz.timezone('America/Bogota') 


pattern = r"/operator=([^/]+)/deviceId=([^/]+)"

from datetime import datetime, timedelta

    
def get_value(item:dict, type:str,from_name:str,multiple: bool=False):    
            
    logging.info(f"comments found {item['id']}")
    id = item["id"]
    value = item["value"]
    if multiple:
        if type == "textarea":
            comments_value = ",".join("text")
        else:
            comments_value = ",".join(value.get(type))
        
    else:
        comments_value = value.get(type)
    
    dict_result = {
            "id":id,
            from_name: comments_value,
        }
    
    return dict_result



def list_to_dataframe(list_of_dict: list):
     if len(list_of_dict)>0:
                             
        df_column = pd.DataFrame(list_of_dict)
        df = pd.merge(df, df_column, on='id', how='left')
        df = df.replace({np.nan: ''})
        
        return df_column
    
    
    
def process_result(df_result:pd, metadata:dict):
                results = df_result.copy()
               
               
                wrench  = []
                pipes = []
                values = []
                assembly_name = []
                number_list = []
                
                objetive = []
                comments = []
                rating = []
                dh_tool_family = []
                
                if results != []:
                    for item in results:
                        print(f"{item} \n")
                        
                        if item["type"]=="timeserieslabels":
                            
                            logging.info(f"timeserieslabels found {item['id']}")
                            id = item["id"]
                            value = item["value"]
                            start = value.get("start")
                            end = value.get("end")
                            print(value)
                            timeserieslabels = value.get("timeserieslabels")
                        
                            
                            if "meta" in item:
                                meta = item.get("meta", {}).get("text")[0]
                                
                            else:
                                meta = None
                            
                            values.append(
                                {
                                    "id":id,
                                    "start_date": start,
                                    "end_date": end,
                                    "activity": timeserieslabels[0],
                                    "meta": meta
                                }
                            )
                            
                        elif item["type"] == "choices" and item["from_name"]=="assembly":
                            item_value = get_value(item,"choices","assembly",True) 
                            assembly_name.append(item_value)                           
                            
                            
                        elif item["type"] == "number" and item["from_name"]=="assembly_number":
                            item_value = get_value(item,"number","assembly_number")
                            number_list.append(item_value)
                            
                        elif item["type"] == "labels" and item["from_name"]=="tuberia":
                            item_value = get_value(item,"labels","tuberia",True)
                            pipes.append(item_value)
                            
                           
                        elif item["type"] == "choices" and item["from_name"]=="wrench":
                            item_value = get_value(item,"choices","wrench",True)
                            wrench.append(item_value)
                                        
                            
                        elif item["type"] == "textarea" and item["from_name"]=="objetivo":
                            item_value = get_value(item,"textarea","objetivo",True)
                            objetive.append(item_value)                                        
                       
                            
                        elif item["type"] == "textarea" and item["from_name"]=="comments":
                            item_value = get_value(item,"textarea","comments",True)
                            comments.append(item_value)                              
                        
                                
                        elif item["type"] == "rating" and item["from_name"]=="rating":
                            
                            item_value = get_value(item,"rating","rating")
                            rating.append(item_value)
                            
                        elif item["type"] == "textarea" and item["from_name"]=="dh_tool_family":
                            item_value = get_value(item,"textarea","dh_tool_family",True)
                            comments.append(item_value)                              
                        
                        
                            

                    # DataFrame con los datos
                    logging.info(values)
                    df = pd.DataFrame(values)
                    
                    
                    # Crea un DataFrame con el esquema especificado (sin agregar datos)
                    logging.info("assembly")
                    if len(assembly_name)>0:
                        
                        df_assembly_name = pd.DataFrame(assembly_name)
                        df = pd.merge(df, df_assembly_name[['id', 'assembly']], on='id', how='left')
                        df.replace({np.nan: ''}, inplace=True)
                    else:
                        df["assembly"]= ''
                        
                        
                    logging.info("number_list")    
                    if len(number_list)>0:
                        df_number_list = pd.DataFrame(number_list)
                        df = pd.merge(df, df_number_list[['id', 'assembly_number']], on='id', how='left')
                        df.replace({np.nan: ''}, inplace=True)
                    else:
                        df["assembly_number"]= ""
                    logging.info("assembly") 
                    
                    
                    df['assembly'] = df['assembly'].astype(str) +" "+ df['assembly_number'].astype(str)    
                    
                    
                    logging.info("pipes")    
                    if len(pipes)>0:
                        df_pipes = pd.DataFrame(pipes)
                        df = pd.merge(df, df_pipes[['id', 'tuberia']], on='id', how='left')
                        df.replace({np.nan: ''}, inplace=True)
                    else:
                        df["tuberia"]= ''
                    
                    
                    logging.info("wrench")    
                    if len(wrench)>0:
                        df_wrench = pd.DataFrame(wrench)
                        df = pd.merge(df, df_wrench[['id', 'wrench']], on='id', how='left')
                        df.replace({np.nan: ''}, inplace=True)
                    else:
                        df["wrench"] = ''
                    
                    logging.info("objetivo")   
                    if len(objetive)>0:
                        df_objetive = pd.DataFrame(objetive)
                        df = pd.merge(df, df_objetive[['id', 'objetivo']], on='id', how='left')
                        df.replace({np.nan: ''}, inplace=True)
                    else:
                        df["objetivo"]= ''
                        
                    logging.info("comments")     
                    if len(comments)>0:
                        df_comments = pd.DataFrame(comments)
                        df = pd.merge(df, df_comments[['id', 'comments']], on='id', how='left')
                        df.replace({np.nan: ''}, inplace=True)
                    else:
                        df["comments"] = ''
                        
                        
                        
                    logging.info("dh_tool_family")
                        
                    if len(rating)>0:
                        df_rating = pd.DataFrame(rating)
                        df = pd.merge(df, df_rating[['id', 'dh_tool_family']], on='id', how='left')
                        df.replace({np.nan: ''}, inplace=True)
                    else:
                        df["dh_tool_family"]= '' 
                        
                    
                        
                    df["completed_by"] = metadata["completed_by"]        
                    df["created_at"] = metadata["created_at"]
                    df["updated_at"] = metadata["updated_at"]
                    df["device_id"] = metadata["device_id"]
                    df["operator"] = metadata["operator"]
                    df["well"] = metadata["well"]
                    df["id_intervention"]= metadata["id_intervention"]
                                
                    
                    
                    
                    df = df[["id","device_id","operator", "well","id_intervention", "start_date", "end_date",\
                            "activity","wrench","tuberia","assembly","objetivo","rating",\
                                "comments","created_at","updated_at","completed_by","dh_tool_family"]]
                    logging.info("Dataframe")
                    
                    return df 
    
def get_metadata(payload:dict):
    
    
    timeseries_url = payload.get("task", {}).get("data", {}).get("timeseriesUrl")
    parts_url = timeseries_url.split("/")
    match = re.search(pattern, timeseries_url)
    if match:
        operator = match.group(1)
        device_id = match.group(2)
    
    well = parts_url[-1].split("_")[1]
    id_intervention = parts_url[-1].split("_")[0] 

    # Aplica la conversión de zona horaria
    first_name = payload.get("annotation",{}).get("first_name","")
    last_name = payload.get("annotation",{}).get("last_name","")
    completed_by = first_name+" "+ last_name
    created_at_utc = payload.get("annotation",{}).get("created_at")
    updated_at_utc = payload.get("annotation",{}).get("updated_at")
    created_at_datetime_utc = datetime.strptime(created_at_utc, "%Y-%m-%dT%H:%M:%S.%fZ")   
    updated_at_datetime_utc = datetime.strptime(updated_at_utc, "%Y-%m-%dT%H:%M:%S.%fZ")
    
    created_at = created_at_datetime_utc.replace(tzinfo=pytz.UTC).astimezone(huso_horario_utc_menos_5).strftime("%Y-%m-%d %H:%M:%S")
    updated_at = updated_at_datetime_utc.replace(tzinfo=pytz.UTC).astimezone(huso_horario_utc_menos_5).strftime("%Y-%m-%d %H:%M:%S")
    
      
    
    metadata = {
        "completed_by":completed_by,        
        "created_at" : created_at,
        "updated_at" : updated_at,
        "device_id": device_id,
        "operator":operator,
        "well":well,
        "id_intervention":id_intervention
        
     
    }
    
    
    
    return metadata
    