# from library.functions import clean_and_update, readdata
from library.functions_withsessionid import clean_and_update, readdata
df = readdata()

clean_and_update(df)