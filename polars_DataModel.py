import argparse
import polars as pl


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
        df = df.filter(pl.col('Gender') == 'Male')
    elif gender == 'Female':
        df = df.filter(pl.col('Gender') == 'Female')

    # Age filter
    # Set min_age and max_age same if you want users of a specific age
    # Change min_age and max_age to get users between a certain age
    df = df.filter((pl.col('Age') >= min_age) & (pl.col('Age') <= max_age))

    # Clinic filter
    df = df.filter(pl.col('ClinicID') == ClinicID)

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
    and returns a dataframe with the 'weigh_in_rate',
    'patient_starting_weight', 'treatment_starting_weight', 'treatment_TBWL',
    'patient_TBWL' metrics for each user cohort wise
    """

    # Load datasets
    users = pl.read_csv(f'{path_to_data}/users.csv', try_parse_dates=True)
    weights = pl.read_csv(f'{path_to_data}/weights.csv', try_parse_dates=True)
    treatments = pl.read_csv(f'{path_to_data}/treatments.csv',
                             try_parse_dates=True)

    # Left join dataframes: Users <> Weights, Treatments
    uw = users.join(weights, left_on='UID',
                    right_on='MasterUserID',
                    how='left')
    df = uw.join(treatments, left_on='UID', right_on='MasterUserID',
                 how='left')

    # Rename columns for better understanding
    columns_to_rename = {'CreatedDate': 'UIDCreatedDate',
                         'IsActive': 'User_IsActive',
                         'CreatedDate_right': 'Wts_CreatedDate',
                         'UpdatedDate': 'Wts_UpdatedDate',
                         'IsActive_right': 'Wts_IsActive',
                         'IsDelete': 'Wts_IsDelete',
                         'StartDate': 'Tmt_StartDate',
                         }
    df = df.rename(columns_to_rename)

    # Sort the data by 'UID', 'UIDCreatedDate', 'TreatmentTypeID',
    # 'Treatment_StartDate', 'Weights_CreatedDate', 'Weights_UpdatedDate'
    df = df.sort(by=['UID',
                     'UIDCreatedDate',
                     'TreatmentTypeID',
                     'Tmt_StartDate',
                     'Wts_CreatedDate',
                     'Wts_UpdatedDate'])

    # The difference between the treatment start date and
    # the weights created/updated can be used to calculate the month/week
    # from the start of the treatment
    df = df.with_columns(days_in_month=pl.lit(30.417))
    df = df.with_columns(days_in_week=pl.lit(7))

    datediff = pl.col('Wts_CreatedDate') - pl.col('Tmt_StartDate')
    df = df.with_columns(datediff.alias('datediff'))

    # Calculate the number of months since treatment start date
    div = pl.col('datediff') / pl.duration(days="days_in_month")
    df = df.with_columns((div.floor()).alias('month'))

    # Calculate the number of weeks since treatment start date
    div = pl.col('datediff') / pl.duration(days="days_in_week")
    df = df.with_columns((div.floor()).alias('week'))

    # Cast float to int
    df = df.with_columns(pl.col('month').cast(pl.Int32),
                         pl.col('week').cast(pl.Int32))

    # weigh-in rate - number of times a user has weighed themselves
    # during each week/month of treatment

    # Why treatment start date? -
    # Ex- The user might have been in Trt 1 first,
    # switched to Trt 2 and back to 1

    df = df.with_columns(pl.col('Wts_UpdatedDate').count().
                         over(['UID', 'TreatmentTypeID', 'Tmt_StartDate',
                               cohort]).alias('WIR'))

    # Patient starting weight
    df = df.with_columns(pl.col('Weight').first().over('UID').alias('PSW'))

    # Treatment starting weight
    df = df.with_columns(pl.col('Weight').first().
                         over(['UID', 'TreatmentTypeID',
                               'Tmt_StartDate']).alias('TSW'))

    # Treatment total body weight loss

    # The differene between the patient weight at the start of the treatment
    # and the patient weight at the end of the treatment

    # Treatment ending weight
    df = df.with_columns(pl.col('Weight').last().
                         over(['UID', 'TreatmentTypeID',
                               'Tmt_StartDate']).alias('TEW'))

    # Difference between treatment ending weight and starting weight
    df = df.with_columns((pl.col('TEW') - pl.col('TSW')).alias('tmt_TBWL'))

    # Patient total body weight loss

    # The difference between patient weight at the start of the cohort
    # and patient weight at the start of the next cohort (week/month)

    # Find the weight at the start of eahc week and
    # calculate the consecutive row differences
    df = df.with_columns(pl.col('Weight').first().
                         over(['UID', 'TreatmentTypeID', 'Tmt_StartDate',
                               cohort]).alias('wgt_diff').diff(-1))

    # Weight lost each week is the highest wgt_diff each week
    df = df.with_columns(pl.col('wgt_diff').max().
                         over(['UID', 'TreatmentTypeID', 'Tmt_StartDate',
                               cohort]).alias('patient_TBWL'))

    # Drop wgt_diff column
    df = df.drop(columns=['wgt_diff', 'days_in_month',
                          'days_in_week', 'datediff'])

    # Filter
    df = filters(df, gender, min_age, max_age, ClinicID)

    print(df[['Weight', 'week', 'WIR',
              'PSW',
              'TSW',
              'tmt_TBWL',
              'patient_TBWL']])  # Check

    return df


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
                        default=5066, help='ClinicID of the\
                        clinic that the users go to')
    args = parser.parse_args()

    data_pipeline(args.path_to_data, args.cohort, args.gender, args.min_age,
                  args.max_age, args.ClinicID)
