#Kmeans.py

import sys
import math
import random
from pyspark import SparkContext

def closestPoint(point, centerpoints):
    shortestDistanceIndex = 0
    currentIndex = 0
    shortestDistance = float("inf")

    for cp in centerpoints:
        distance = math.sqrt(math.pow((point[0] - cp[0]), 2) + math.pow((point[1] - cp[1]), 2))
        if (distance < shortestDistance):
            shortestDistance = distance
            shortestDistanceIndex = currentIndex
        currentIndex = currentIndex + 1

    return shortestDistanceIndex

def addPoints(p1, p2):
    newlat = p1[0] + p2[0]
    newlong = p1[1] + p2[1]

    newPoint = (newlat, newlong)

    return newPoint

def euclideanDistance(p1, p2):
    return math.sqrt(math.pow((p1[0] - p2[0]), 2) + math.pow((p1[1] - p2[1]), 2))

def greatCircleDistance(p1, p2):
    R = 3959.0 # miles
    lat1 = math.radians(p1[0])
    lat2 = math.radians(p2[0])
    delta_lat = math.radians(p2[0] - p1[0])
    delta_long = math.radians(p2[1] - p1[1])

    a = math.sin(delta_lat / 2) * math.sin(delta_lat / 2) + math.cos(lat1) * math.cos(lat2) * math.sin(delta_long / 2) * math.sin(delta_long / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    d = R * c

    return d


if __name__ == "__main__":
    if len(sys.argv) < 6:
        print >> sys.stderr, "Usage: Kmeans.py <file> k euclidean *or* greatCircle centroidsOutputfile.txt clustersOutputfile.txt"
        exit(-1)


    sc = SparkContext();
   
    text = sc.textFile(sys.argv[1])

    # Use the below commands for files where each line is a lat, long pair separated by commas. 
    split = text.map(lambda line: line.split(','))
    data = split.map(lambda line: (float(line[0]), float(line[1])))
    data.persist()

    # intialize k, convergeDist, and the centroids list
    k = int(sys.argv[2])
    convergeDist = 0.1
    centroids = []


    # intialize k centroids for the clusters	

    for i in range(k):
        lat = random.uniform(-90, 90)
        lon = random.uniform(-180, 180)
        pair = (lat, lon)
        centroids.append(pair)

    meanChange = 1

    # beginning of K-means algorithm
    while meanChange >= 0.1:		
        oldCentroids = centroids[:]
        
        # create an rdd composed of each point along with the centroid it belongs to using the closesPoint function
        pointCentroidPair = data.map(lambda line: (closestPoint(line, centroids), line))

        # for each centroid:
        for i in range(k):
            # create an rdd of only points that belong to the kth cluster
            clusterPoints = pointCentroidPair.filter(lambda line: line[0] == i)
            clusterPoints.persist()
            totalPoints = float(clusterPoints.count())
            # sum all the points in the cluster
            clusterSum = clusterPoints.reduceByKey(lambda c1, c2: addPoints(c1, c2))

            # if the kth cluster contains points:
            if clusterSum.count() != 0:
            # create a new centroid by taking the average of all the points in the cluster
                newCentroid = clusterSum.map(lambda pointSum: ((pointSum[1][0] / totalPoints), (pointSum[1][1] / totalPoints)))	
                # update the centroids array with the new centroid
                centroids[i] = newCentroid.take(1)[0]	

        # calculate the distance between all the old cluster centroids and new cluster centroids
        centerDistances = []
        for i in range(k):
            if sys.argv[3] == "euclidean":
                centerDistances.append(euclideanDistance(oldCentroids[i], centroids[i]))
            if sys.argv[3] == "greatCircle":
                centerDistances.append(greatCircleDistance(oldCentroids[i], centroids[i]))
        # calculate the average change in centroid
        cDlength = len(centerDistances)
        sumcDs = sum(centerDistances)
        meanChange = sumcDs / cDlength

    centroidsRDD = sc.parallelize(centroids)
    centroidsCleanRDD = centroidsRDD.map(lambda (lat,lon): str(lat) + ',' + str(lon))
    centroidsFile = centroidsCleanRDD.saveAsTextFile(sys.argv[4])

    # write the final centroids to an output file
    f = open(sys.argv[4], "w+")
    if f.mode == "w+":
        for x in centroids:
            f.write(str(x)+"\n")
    f.close()

    pointCentroidCleanRDD = pointCentroidPair.map(lambda (index, (lat,lon)): str(index) + ',' + str(lat) + ',' + str(lon))
    clusterFile = pointCentroidCleanRDD.saveAsTextFile(sys.argv[5])

    # write the final clusters to an output file
    f = open(sys.argv[5], "w+")
    if f.mode == "w+":
        for x in pointCentroidPair.collect():
            f.write(str(x)+"\n")
    f.close() 

    sc.stop()
