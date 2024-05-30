# Jared Submission

## QA issues
The only issues I felt like I needed to fix were the added comma in the symbol and the 0 value for volume.

There were no obvious date issues and the trades by inactive users were filtered out by filtering all inactive users anyway.

I didn't include useres that didn't have any trades.

I also decided to interpret "Return a row for every combination of dt_report/login/server/symbol every day in June, July, August and September 2020. Your method should work even if there is nodata on a particular day in this period within the data." 
as returning a row for each day in those months and not for every combination of login server and symbol possible in that time due to the infra not being able to handle the record size.

In real life I would clarify this requirement and achieve it by taking distinct values and cross joining them into each other creating a shape and populating metrics from there.

Overall I enjoyed the case study.
