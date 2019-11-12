import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.style as style
style.available
style.use('seaborn-poster') #sets the size of the charts
style.use('ggplot')
import seaborn as sns

state= pd.read_csv('../data/clean_data/top_5.csv')
state['percent']=(state['Enrollees']/state['population']*100).round(2)
state=state.sort_values(by='Enrollees', ascending = False)

ax = sns.barplot(y= "Enrollees", x = "State", data = state, palette=("Blues_d"))
sns.set_context("poster")
plt.savefig('Ebnrollees')