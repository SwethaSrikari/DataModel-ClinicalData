# Analytics Data Model

The goal of this problem is to develop an analytics data model that one can use to quickly query and track a patient‘s weekly/monthly/clinic-wise weight loss. This model should also
be useful for filtering the data by age, gender and clinic. These models are crucial to track the patient’s weight loss progress throughout the treatment to understand (a) how the treatment is working, and (b) to tailor an effective treatment plan for the patients.

The data pipeline includes a series of processes and workflows that extract, transform, and load (ETL) data from various sources, perform necessary operations on it, and finally store it in a structured format for analysis. In this context, the data pipeline is designed to handle weight-related data from a clinical setting.

The data pipeline/analytics data model - 
* Preprocesses the data by
   * Cross-reference and merge the users, treatments and weights datasets
   * Deduplicate data
   * Meaningfully renaming the columns
  * Setting the correct date format
* Computes the attributes necessary to engineer the metrics
  * Patient’s weight at the start and end of treatment to calculate the treatment's
total body weight loss
  * Month and week columns from the start of the treatment plan
* Establishes the metrics - weigh-in rate, patient starting weight, treatment starting
weight, patient total body weight loss, and treatment total body weight loss based on
the cohort (week/month/clinic).
* Filters the data by
  * Age - one can query patients of a certain age group
  * Gender - query based on gender (Male/Female) or returns all the entries
  * ClinicID - based on the clinic the patient goes to
* Returns a data frame (table) with the following details about each patient
  * Patient’s User ID, Name, LastName, Gender, Age, Clinic ID, cohort
(week/month/clinic), weigh-in rate, patient’s starting weight, treatment starting
weight, patient total body weight loss (TBWL), treatment total body weight
loss (TBWL)

**To run the code on terminal**

```
python analytics_data_model.py --path_to_data PATH_TO_DATA --cohort 'week' --gender 'all' --min_age 18 --max_age 72
```
