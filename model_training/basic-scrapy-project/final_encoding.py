import pandas as pd
import numpy as np
import openpyxl

def encode_company(df_company):
    df_n = pd.DataFrame()
    df_encoded = pd.DataFrame()
    for profile in df_company["profile_name"].unique():
        df = df_company[df_company["profile_name"]==profile]
        df_n["profile_name"] = df["profile_name"].unique()
        df_n["number_of_jobs"] = len(df['job_level']),
        df_n["first_job_level"] = df[df['start_recency_months']==df['start_recency_months'].max()]['job_level'].max(),
        df_n["first_job_recency_months"] = df['start_recency_months'].max(),
        df_n["first_job_duration_months"] = df[df['start_recency_months']==df['start_recency_months'].max()]['duration'].max(),
        df_n["first_company_size"] = df[df['start_recency_months']==df['start_recency_months'].max()]['company_size'].max(),
        df_n["first_company_age_years"] = df[df['start_recency_months']==df['start_recency_months'].max()]['company_age_years'].max(),
        df_n["first_company_apparel"] = df[df['start_recency_months']==df['start_recency_months'].max()]['apparel_industry'].max(),
        df_n["first_company_sri_lankan"] = df[df['start_recency_months']==df['start_recency_months'].max()]['headquaters'].max(),
        df_n["last_job_level"] = df[df['start_recency_months']==df['start_recency_months'].min()]['job_level'].max(),
        df_n["last_job_recency_months"] = df['start_recency_months'].min(),
        df_n["last_job_duration_months"] = df[df['start_recency_months']==df['start_recency_months'].min()]['duration'].min(),
        df_n["last_company_size"] = df[df['start_recency_months']==df['start_recency_months'].min()]['company_size'].min(),
        df_n["last_company_age_years"] = df[df['start_recency_months']==df['start_recency_months'].min()]['company_age_years'].min(),
        df_n["last_company_apparel"] = df[df['start_recency_months']==df['start_recency_months'].min()]['apparel_industry'].min(),
        df_n["last_company_sri_lankan"] = df[df['start_recency_months']==df['start_recency_months'].min()]['headquaters'].min(),
        df_n["minimum_job_duration_months"] = df['duration'].min(),
        df_n["maximum_job_duration_months"] = df['duration'].max(),
        df_n["average_job_duration_months"] = df['duration'].mean(),
        df_n["total_job_duration_months"] = df['duration'].sum()
        df_n["number of turnovers"] = df[df['company_change']>-1]["company_change"].sum()
        df_n["number of companies"] = df['cum_no_of_companies'].max()
        df_n["number of levelups"] = len(df[df['level_up']>0]["level_up"])
        df_n["total levelups"] = df[df['level_up']>-1]["level_up"].sum()
        df_n["average years for levelup"] = ((df_n['total_job_duration_months']-df_n['last_job_duration_months'])/12)/(df_n[df_n["total levelups"]!=0]["total levelups"])
        df_n["total LUs within company"] = df[df['company_change']==0]["level_up"].sum()
        df_n["total LUs outside company"] = df[df['company_change']==1]["level_up"].sum()
        df_n["number of lateral movements"] = len(df[df['lateral_movements']>0]["lateral_movements"])
        df_n["duration ratio in sri lanka"] = (df[df['headquaters']==1]["duration"].sum())/df_n["total_job_duration_months"]
        df_n["duration ratio in apparel"] = (df[df['apparel_industry']==1]["duration"].sum())/df_n["total_job_duration_months"]
        df_encoded = pd.concat([df_encoded, df_n], ignore_index = True)

    return df_encoded

def encode_school(df_scl):
    df_ed = pd.DataFrame()
    df_encoded_scl = pd.DataFrame()
    for profile in df_scl["profile_name"].unique():
        df = df_scl[df_scl["profile_name"]==profile]
        df_ed["profile_name"] = df["profile_name"].unique()
        df_ed["number_of_Qualifications"] = len(df['course_level']),
        df_ed["first_qual_level"] = df[df['start_recency_months']==df['start_recency_months'].max()].iloc[0]['course_level'],
        df_ed["first_qual_recency_months"] = df[df['start_recency_months']!=24276]['start_recency_months'].max(),
        df_ed["first_qual_duration_months"] = df[df['start_recency_months']==df['start_recency_months'].max()].iloc[0]['duration'],
        df_ed["first_institute_size"] = df[df['start_recency_months']==df['start_recency_months'].max()].iloc[0]['school_size'],
        df_ed["first_institute_age_years"] = df[df['start_recency_months']==df['start_recency_months'].max()].iloc[0]['school_age_years'],
        df_ed["first_institute_sri_lankan"] = df[df['start_recency_months']==df['start_recency_months'].max()].iloc[0]['headquaters'],
        df_ed["last_qual_level"] = df[df['start_recency_months']==df['start_recency_months'].min()].iloc[0]['course_level'],
        df_ed["last_qual_recency_months"] = df['start_recency_months'].min(),
        df_ed["last_qual_duration_months"] = df[df['start_recency_months']==df['start_recency_months'].min()].iloc[0]['duration'],
        df_ed["last_institute_size"] = df[df['start_recency_months']==df['start_recency_months'].min()].iloc[0]['school_size'],
        df_ed["last_institute_age_years"] = df[df['start_recency_months']==df['start_recency_months'].min()].iloc[0]['school_age_years'],
        df_ed["last_institute_sri_lankan"] = df[df['start_recency_months']==df['start_recency_months'].min()].iloc[0]['headquaters'],
        df_ed["minimum_qual_duration_months"] = df['duration'].min(),
        df_ed["maximum_qual_duration_months"] = df['duration'].max(),
        df_ed["average_qual_duration_months"] = df['duration'].mean(),
        df_ed["total_qual_duration_months"] = df['duration'].sum()
        df_ed["number_of_institutes"] = df['cum_no_of_schools'].max()
        df_ed["duration ratio in sri lanka"] = df['course_level'].max() - df['course_level'].min()
        df_ed["duration ratio in sri lanka"] = (df[df['headquaters']==1]["duration"].sum())/df_ed["total_qual_duration_months"]
        df_encoded_scl = pd.concat([df_encoded_scl, df_ed], ignore_index = True)

    return df_encoded_scl

def create_final_data(input_file_company, input_file_scl):
    df_company = pd.read_csv(input_file_company)
    df_scl = pd.read_csv(input_file_scl)

    df_company = encode_company(df_company)
    df_scl  = encode_school(df_scl)

    df_final = pd.merge(df_company, df_scl, on = ['profile_name'], how = 'outer')
    
    return df_final

input_file_company = "encoded_data\\experience_encoded.csv"
input_file_scl = "encoded_data\\education_encoded.csv"

df_final = create_final_data(input_file_company, input_file_scl)

df_final.to_csv(r'encoded_data\final_dataset.csv', index=False)

print("\nData retrieved and encoded.\nEncoded dataset is store in encoded_data folder.\n")
