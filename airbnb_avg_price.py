try:
    from pyspark import SparkContext, SparkConf
    import operator
except Exception as e:
    print(e)

def get_avg_price():
    conf = SparkConf().setAppName('average price count')
    conf = conf.setMaster('spark://master:7077')
    sc = SparkContext(conf=conf)

    # read csv file as rdd
    airbnb_txt=sc.textFile("/tmp/MontrealAirBnB.csv", minPartitions=2)
    
    #remove header from rdd
    tagsheader = airbnb_txt.first() 
    header = sc.parallelize([tagsheader])
    airbnb_rdd = airbnb_txt.subtract(header)
    
    airbnb_row = airbnb_rdd.map(lambda x: x.split(','))
    
    # calculate count by neighbourhood
    neighbourhood_lst = airbnb_row.map(lambda x: (x[0], 1))
    n_count = neighbourhood_lst.reduceByKey(lambda x,y: x+y)
    
    # calculate sum(price) per neighbourhood
    price_lst = airbnb_row.map(lambda x: (x[0], int(x[2])))
    price_lst_sum = price_lst.reduceByKey(operator.add)

    #inner join total per neighbourhood and average by neighbourhood lists
    neighbourhood_price_lst = n_count.join(price_lst_sum)
    
    #calculate average price per neighbourhood
    avg_price_per_neighbourhood = neighbourhood_price_lst.map(lambda x: (x[0], (x[1][1]/x[1][0])))
    
    # output results    
    avg_price_per_neighbourhood.saveAsTextFile("/tmp/avg_price_rdd")

    sc.stop()

if __name__ == "__main__":
    get_avg_price()
