# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.



import dask.dataframe as dd

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('Starting NYC taxi Calc')

    df_tripdata_2019 = dd.read_csv("/Users/vishaldutt/downloads/nyfiles/2019/*_tripdata_2019-*.csv",low_memory=False,assume_missing=True)
    Total_sales_2019=df_tripdata_2019.total_amount.sum().compute()
    print("Total Sales 2019 :",Total_sales_2019.astype('int64', copy=False))


    df_yellow_tripdata = dd.read_csv("/Users/vishaldutt/downloads/nyfiles/2019/yellow_tripdata_2019-*.csv",low_memory=False,assume_missing=True)
    #df_yellow_tripdata_new=df_yellow_tripdata.rename(columns={"tpep_dropoff_datetime":"dropoff_datetime","tpep_pickup_datetime":"pickup_datetime"})
    zone_wise_expensive_trip_2019_yellow=df_yellow_tripdata.groupby(['PULocationID','DOLocationID']).total_amount.max().compute()
    print("Yellow  taxi Payment Zone Wise Most expensive Trip Amount 2019:",zone_wise_expensive_trip_2019_yellow.astype('int64', copy=False).head())

    df_yellow_tripdata['pickupdatetime']=dd.to_datetime(df_yellow_tripdata.tpep_pickup_datetime)
    df_yellow_tripdata['dayofweek']=df_yellow_tripdata['pickupdatetime'].dt.day_name()
    #print(df_yellow_tripdata.head())

   # df_yellow_tripdata_new.compute().to_csv("/Users/vishaldutt/downloads/nyfiles/final/yellow_tripdata_2019.csv", index=False)
    amount_payment_type_yellow=df_yellow_tripdata.groupby('payment_type').total_amount.sum().compute()
    print("Yellow taxi Payment TYpe total Amount 2019:",amount_payment_type_yellow.astype('int64', copy=False).head() )

    Avg_total_by_day_of_week_yellow=df_yellow_tripdata.groupby('dayofweek').total_amount.mean().compute()
    print("Yellow taxi AVG total Amount by Day of week 2019:",Avg_total_by_day_of_week_yellow.astype('int64', copy=False).head() )

    df_green_tripdata = dd.read_csv("/Users/vishaldutt/downloads/nyfiles/2019/green_tripdata_2019-*.csv",low_memory=False,assume_missing=True)

    zone_wise_expensive_trip_2019_green=df_green_tripdata.groupby(['PULocationID','DOLocationID']).total_amount.max().compute()
    print("Green taxi Payment Zone Wise Most expensive Trip Amount 2019:",zone_wise_expensive_trip_2019_green.astype('int64', copy=False).head() )

    df_green_tripdata['pickupdatetime']=dd.to_datetime(df_green_tripdata.lpep_pickup_datetime)
    df_green_tripdata['dayofweek']=df_green_tripdata['pickupdatetime'].dt.day_name()
    #print(df_green_tripdata.head())
    amount_payment_type_green=df_green_tripdata.groupby('payment_type').total_amount.sum().compute()
    print("Green taxi Payment TYpe total Amount 2019:",amount_payment_type_green.astype('int64', copy=False).head() )
    Avg_total_by_day_of_week_green=df_yellow_tripdata.groupby('dayofweek').total_amount.mean().compute()
    print("Yellow taxi AVG total Amount by Day of week 2019:",Avg_total_by_day_of_week_green.astype('int64', copy=False).head() )

    df_tripdata_2020 = dd.read_csv("/Users/vishaldutt/downloads/nyfiles/2020/*_tripdata_*-*.csv",low_memory=False,assume_missing=True)
    Total_sales_2020=df_tripdata_2020.total_amount.sum().compute()
    print("Total Sales Pandemic :",Total_sales_2020.astype('int64', copy=False))




    df_yellow_tripdata_2020 = dd.read_csv("/Users/vishaldutt/downloads/nyfiles/2020/yellow_tripdata_*-*.csv",low_memory=False,assume_missing=True)

    df_yellow_tripdata_2020['pickupdatetime']=dd.to_datetime(df_yellow_tripdata_2020.tpep_pickup_datetime)
    df_yellow_tripdata_2020['dayofweek']=df_yellow_tripdata_2020['pickupdatetime'].dt.day_name()

    zone_wise_expensive_trip_pandemic=df_yellow_tripdata_2020.groupby(['PULocationID','DOLocationID']).total_amount.max().compute()
    print("Yellow taxi Payment Zone Wise Most expensive Trip Amount Pandemic:",zone_wise_expensive_trip_pandemic.astype('int64', copy=False).head() )

    amount_payment_type_yellow_pandemic=df_yellow_tripdata_2020.groupby('payment_type').total_amount.sum().compute()
    print("Yellow taxi Payment TYpe total Amount Pandemic:",amount_payment_type_yellow_pandemic.astype('int64', copy=False).head() )

    Avg_total_by_day_of_week_yellow_pandemic=df_yellow_tripdata_2020.groupby('dayofweek').total_amount.mean().compute()
    print("Yellow taxi AVG total Amount by Day of week Pandemic:",Avg_total_by_day_of_week_yellow_pandemic.astype('int64', copy=False).head() )

    df_green_tripdata_2020 = dd.read_csv("/Users/vishaldutt/downloads/nyfiles/2020/green_tripdata_*-*.csv",low_memory=False,assume_missing=True)

    df_green_tripdata_2020['pickupdatetime']=dd.to_datetime(df_green_tripdata_2020.lpep_pickup_datetime)
    df_green_tripdata_2020['dayofweek']=df_green_tripdata_2020['pickupdatetime'].dt.day_name()

    zone_wise_expensive_trip_pandemic_green=df_green_tripdata_2020.groupby(['PULocationID','DOLocationID']).total_amount.max().compute()
    print("Green taxi Payment Zone Wise Most expensive Trip Amount Pandemic:",zone_wise_expensive_trip_pandemic_green.astype('int64', copy=False).head() )
    amount_payment_type_green_pandemic=df_green_tripdata_2020.groupby('payment_type').total_amount.sum().compute()
    print("Green taxi Payment TYpe total Amount Pandemic:",amount_payment_type_green_pandemic.astype('int64', copy=False).head() )
    Avg_total_by_day_of_week_green_Pandemic=df_yellow_tripdata.groupby('dayofweek').total_amount.mean().compute()
    print("Yellow taxi AVG total Amount by Day of week:",Avg_total_by_day_of_week_green_Pandemic.astype('int64', copy=False).head() )

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

