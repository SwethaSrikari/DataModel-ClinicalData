import math
import argparse
import pandas as pd


def filters(df, gender='all', min_age=18, max_age=72, ClinicID=5066):
	"""
	:params df(DataFrame): Pandas dataframe to filter
	:params gender (str): Gender of the user; 'Male' or 'Female' or 'all'
	:params min_age (int): Minimum age of the user to be queried
	:params max_age (int): Maximum age of the user to be queried
	:params ClinicID (int): ClinicID of the clinic that the users go to

	Filters the data (df) based on Gender, Age and Clinic
	"""

	# Gender filter
	if gender == 'all':
	    df = df
	elif gender == 'Male':
	    df = df[df.Gender == 'Male']
	elif gender == 'Female':
	    df = df[df.Gender == 'Female']
	    
	# Age filter
	# Set min_age and max_age same if you want users of a specific age
	# Change min_age and max_age to get users between a certain age
	df = df[(df.Age >= min_age) & (df.Age <= max_age)]

	# Clinic filter
	df = df[df.ClinicID == ClinicID]

	return df

def data_pipeline(path_to_data, cohort='week', gender='all',
                  min_age=18, max_age=72, ClinicID=5066):
	"""
	:params path_to_data (str): Path to data
	:params cohort (str): To get info monthly, weekly and based on clinic
	:params gender (str): Gender of the user; 'Male' or 'Female' or 'all'
	:params min_age (int): Minimum age of the user to be queried
	:params max_age (int): Maximum age of the user to be queried
	:params ClinicID (int): ClinicID of the clinic that the users go to

	Loads dataset, merges them, cleans them, calculates required metrics,
	and returns a dataframe with the 'weigh_in_rate', 'patient_starting_weight',
	'treatment_starting_weight', 'treatment_TBWL', 'patient_TBWL' metrics for each user cohort wise
	"""

	# Load datasets
	users = pd.read_csv(f'{path_to_data}/users.csv')
	weights = pd.read_csv(f'{path_to_data}/weights.csv')
	treatments = pd.read_csv(f'{path_to_data}/treatments.csv')


	# Left join dataframes: Users <> Weights, Treatments
	uw = pd.merge(users, weights,
				  left_on='UID', right_on='MasterUserID',
				  how='left')
	merged_df = pd.merge(uw, treatments,
						 left_on='UID', right_on='MasterUserID',
						 how='left')


	# Drop 2 duplicate UserID columns as the values are same in the 2 columns and UID column
	merged_df = merged_df.drop(columns=['MasterUserID_x', 'MasterUserID_y'])


	# Rename columns for better understanding
	columns_to_rename = {'CreatedDate_x': 'UIDCreatedDate',
						 'IsActive_x': 'User_IsActive',
						 'CreatedDate_y': 'Weights_CreatedDate',
						 'UpdatedDate': 'Weights_UpdatedDate',
						 'IsActive_y': 'Weights_IsActive',
						 'IsDelete': 'Weights_IsDelete',
						 'StartDate': 'Treatment_StartDate',
	                     }
	merged_df = merged_df.rename(columns=columns_to_rename)


	# Change datatypes of date columns from object to date
	columns_to_change = ['Birthday', 'UIDCreatedDate', 'Weights_CreatedDate',
						 'Weights_UpdatedDate', 'Treatment_StartDate']

	merged_df[columns_to_change] = merged_df[columns_to_change].apply(pd.to_datetime)


	# Sort the data by 'UID', 'UIDCreatedDate', 'TreatmentTypeID', 'Treatment_StartDate',
	# 'Weights_CreatedDate', 'Weights_UpdatedDate'
	merged_df = merged_df.sort_values(by=['UID',
										  'UIDCreatedDate',
										  'TreatmentTypeID',
										  'Treatment_StartDate',
										  'Weights_CreatedDate',
										  'Weights_UpdatedDate'])


	# The difference between the treatment start date and the weights created/updated 
	# can be used to calculate the month/week from the start of the treatment
	# Starting from week/month 1 i.e., first week of treatment instead of zeroth week

	# Calculate the number of months since treatment start date
	merged_df['month'] = (merged_df['Weights_CreatedDate'] - merged_df['Treatment_StartDate']) / pd.Timedelta(days=30.44)
	merged_df['month'] = merged_df['month'].apply(math.floor) + 1

	# Calculate the number of weeks since treatment start date
	merged_df['week'] = (merged_df['Weights_CreatedDate'] - merged_df['Treatment_StartDate']).dt.days // 7 + 1


	# weigh-in rate - number of times a user has weighed themselves during each week/month of treatment

	# Why treatment start date? - Ex- The user might have been in Trt 1 first, switched to Trt 2 and back to 1

	merged_df['weigh_in_rate'] = merged_df.groupby(['UID', 
													'TreatmentTypeID',
													'Treatment_StartDate',
													cohort])['Weights_UpdatedDate'].transform('count')


	# Patient starting weight
	merged_df['patient_starting_weight'] = merged_df.groupby(['UID'])['Weight'].transform('first')


	# Treatment starting weight
	merged_df['treatment_starting_weight'] = merged_df.groupby(['UID',
																'TreatmentTypeID',
																'Treatment_StartDate'])['Weight'].transform('first')


	# Treatment total body weight loss

	# The differene between the patient weight at the start of the treatment
	# and the patient weight at the end of the treatment

	# Treatment ending weight
	tew = merged_df.groupby(['UID',
							 'TreatmentTypeID',
							 'Treatment_StartDate'])['Weight'].transform('last')

	merged_df['treatment_TBWL'] = tew - merged_df['treatment_starting_weight']


	# Patient total body weight loss

	# The difference between patient weight at the start of the cohort (week/month)
	# and patient weight at the start of the next cohort (week/month)

	merged_df['wgt_diff'] = merged_df.groupby(['UID',
											   'TreatmentTypeID',
											   'Treatment_StartDate',
											   cohort])['Weight'].transform('first').diff(-1)

	# Replace with same cohort_wise weight loss for the respective user cohorts
	# negative value indicates weight lost, +ve indicates weight gained
	merged_df['patient_TBWL'] = - merged_df.groupby(['UID',
												   'TreatmentTypeID',
												   'Treatment_StartDate',
												   cohort])['wgt_diff'].transform('max')
	# Drop wgt_diff column
	merged_df = merged_df.drop(columns=['wgt_diff'])


	# Filter
	merged_df = filters(merged_df, gender, min_age, max_age, ClinicID)


	# print(merged_df[['Weight', 'week', 'weigh_in_rate',
    #                 'patient_starting_weight',
    #                 'treatment_starting_weight',
    #                 'treatment_TBWL',
    #                 'patient_TBWL']]) # Check
	return merged_df.drop_duplicates()



if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--path_to_data', type=str,
	 					 default='Data/', help='Dataset directory')
	parser.add_argument('--cohort', type=str,
						default='week', help='Cohort type',
						choices=['week', 'month', 'ClinicID'])
	parser.add_argument('--gender', type=str,
						default='all', help='Gender of the user',
						choices=['all', 'Male', 'Female'])
	parser.add_argument('--min_age', type=int,
						default=18, help='Minimum age of the\
						user to be queried')
	parser.add_argument('--max_age', type=int,
						default=72, help='Maximum age of the\
						user to be queried')
	parser.add_argument('--ClinicID', type=int,
						default=5066, help='ClinicID of the clinic that the users go to')
	args = parser.parse_args()

	data_pipeline(args.path_to_data, args.cohort, args.gender, args.min_age,
				  args.max_age, args.ClinicID)