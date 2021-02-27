#!/usr/bin/env python
# coding: utf-8

# In[227]:


import pandas as pd 
import datetime
raw_data =pd.read_csv(r"C:\Users\jsru2\Desktop\SRE class assignment\acs2017_census_tract_data.csv\acs2017_census_tract_data.csv")


# In[228]:


acs_df = raw_data[["TractId", "State", "County","TotalPop", "Poverty", "IncomePerCap"]]
acs_df.head()


# In[229]:


acs_df['poverty_pop'] = (acs_df['TotalPop']*acs_df['Poverty'])/100
acs_df['tot_income'] = acs_df['TotalPop']*acs_df['IncomePerCap']
acs_df.head(4)


# In[230]:


acs_df.drop(columns=['Poverty','IncomePerCap','TractId' ],inplace=True)
acs_df.head(4)


# In[231]:


acs_grp_df = acs_df.groupby(['State','County']).sum()
acs_grp_df.head()


# In[232]:


acs_grp_df['Poverty']= (acs_grp_df['poverty_pop']/acs_grp_df['TotalPop'])*100
acs_grp_df['IncomePerCap']= acs_grp_df['tot_income']/acs_grp_df['TotalPop']
acs_grp_df.drop(columns=['poverty_pop','tot_income'],inplace=True)

acs_grp_df.head(4)


# In[233]:


result = acs_grp_df.loc[[('Oregon', 'Washington County'),('Virginia','Loudoun County'),('Kentucky','Harlan County'),('Oregon','Malheur County')]]
result


# In[235]:


covid_data =pd.read_csv(r"C:\Users\jsru2\Desktop\SRE class assignment\COVID_county_data.csv\COVID_county_data.csv")


# In[236]:


covid_data['date']= pd.to_datetime(covid_data['date'])
covid_data['county']=covid_data['county']+' County'
covid_data.dtypes
covid_data.head(4)


# In[237]:


# TotalCases - total number of COVID cases for this county as of February 20, 2021
# TotalDeaths - total number of COVID deaths for this county as of February 20, 2021
#Find the sum from between start data and February 20, 2021
covid_tot_data=covid_data.groupby(['state','county']).max()
covid_tot_data=covid_tot_data.drop(columns=['fips'])
covid_tot_data.rename(columns={"cases": "tot_cases", "deaths": "tot_deaths"},inplace=True)
covid_tot_data.head()


# In[238]:


# Dec2020Deaths - number of COVID deaths recorded in this county in December of 2020
# Dec2020Cases - number of COVID cases recorded in this county in December of 2020
covid_dec_data=covid_data[(covid_data['date'] > '2020-11-30') & (covid_data['date'] < '2021-01-01')]
covid_dec_data=covid_dec_data.groupby(['state','county']).max()
covid_dec_data=covid_dec_data.drop(columns=['fips'])
covid_dec_data.rename(columns={"cases": "dec_cases", "deaths": "dec_deaths"},inplace=True)
covid_dec_data.head(4)


# In[241]:


covid_final_data = covid_dec_data.merge(covid_tot_data,left_index=True, right_index=True)
covid_final_data.index.rename(['State', 'County'], inplace =True)
covid_final_data.head()


# In[242]:


covid_result = covid_final_data.loc[[('Oregon', 'Washington County'),('Virginia','Loudoun County'),('Kentucky','Harlan County'),('Oregon','Malheur County')]]
covid_result


# In[201]:


final_data = covid_result.merge(result,left_index=True, right_index=True)
final_data


# In[217]:


country_result= covid_final_data.merge(acs_grp_df,left_index=True, right_index=True)
country_result['totCasesPer100k']=(country_result['tot_cases']*100000)/country_result['TotalPop']
country_result['totDeathsPer100k']=(country_result['tot_deaths']*100000)/country_result['TotalPop']
country_result['decCasesPer100k']=(country_result['dec_cases']*100000)/country_result['TotalPop']
country_result['decDeathsPer100k']=(country_result['dec_deaths']*100000)/country_result['TotalPop']
country_result


# In[244]:


oregon_result = country_result.loc[[('Oregon')]]
oregon_result.head(4)


# In[245]:


# COVID total cases vs. % population in poverty
print(oregon_result['totCasesPer100k'].corr(oregon_result['Poverty']))
# COVID total deaths vs. % population in poverty
print(oregon_result['totDeathsPer100k'].corr(oregon_result['Poverty']))
# COVID total cases vs. Per Capita Income level
print(oregon_result['totCasesPer100k'].corr(oregon_result['IncomePerCap']))
# COVID total deaths vs. Per Capita Income level
print(oregon_result['totDeathsPer100k'].corr(oregon_result['IncomePerCap']))
# COVID cases during December 2020 vs. % population in poverty
print(oregon_result['decCasesPer100k'].corr(oregon_result['Poverty']))
# COVID deaths during December 2020 vs. % population in poverty
print(oregon_result['decDeathsPer100k'].corr(oregon_result['Poverty']))
# COVID cases during December 2020 vs. Per Capita Income level
print(oregon_result['decCasesPer100k'].corr(oregon_result['IncomePerCap']))
# COVID cases during December 2020 vs. Per Capita Income level
print(oregon_result['decCasesPer100k'].corr(oregon_result['IncomePerCap']))


# In[246]:


#Across all of the counties in the entire USA

# COVID total cases vs. % population in poverty
print(country_result['totCasesPer100k'].corr(country_result['Poverty']))
# COVID total deaths vs. % population in poverty
print(country_result['totDeathsPer100k'].corr(country_result['Poverty']))
# COVID total cases vs. Per Capita Income level
print(country_result['totCasesPer100k'].corr(country_result['IncomePerCap']))
# COVID total deaths vs. Per Capita Income level
print(country_result['totDeathsPer100k'].corr(country_result['IncomePerCap']))
# COVID cases during December 2020 vs. % population in poverty
print(country_result['decCasesPer100k'].corr(country_result['Poverty']))
# COVID deaths during December 2020 vs. % population in poverty
print(country_result['decDeathsPer100k'].corr(country_result['Poverty']))
# COVID cases during December 2020 vs. Per Capita Income level
print(country_result['decCasesPer100k'].corr(country_result['IncomePerCap']))
# COVID cases during December 2020 vs. Per Capita Income level
print(country_result['decCasesPer100k'].corr(country_result['IncomePerCap']))


# In[247]:


oregon_result.plot.scatter('totDeathsPer100k', 'IncomePerCap')


# In[ ]:




