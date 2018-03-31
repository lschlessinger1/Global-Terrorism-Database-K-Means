# Global Terrorism Database Clustering

## Introduction
The repository contains a visualization of the the [Global Terrorism Database][1] (GTD) that is hosted on Kaggle and is provided by the START Consortium. It contains more than 170,000 terrorist attacks from all over the world from 1970 to 2016 (Figure 1).
To visualize this dataset, I decided to implement k-means in PySpark to cluster the different geo-locations of the terrorist attacks.

## Preprocessing
Preprocessing was relatively straightforward for this dataset. Using the Pandas library, I created a DataFrame with only the latitude and longitude. After this, the data was cleaned by dropping any row with either the latitude or longitude missing. From this, I created a CSV file without any indices or headers in order to run it from Amazon S3.

## Method
As an illustrative example of clustering on this dataset, I chose to set k=5 and to use the great-circle distance metric because it would roughly correspond to a clustering for each continent and also because we are dealing with the approximate spherical geometry of the earth. The resulting centroids (Figure 2) appear to correspond to actual centers of terrorist attacks.

## Results

Figure 1: The entire Global Terror Attacks (1970 - 2016) dataset. The solid red circles are terrorist attacks before clustering is performed.

Figure 2: A map containing the centroids resulting from the k-means algorithm using k=5 and the great-circle distance metric on the whole Global Terror Attacks (1970 - 2016) dataset. It shows clusters of terrorist attacks in the following locations: Oceania, South Asia, Central Africa, Middle East, and South America. 

The five resulting centroid coordinates are:

| Latitude          | Longitude         |
| ----------------- |------------------ |
| 2.361956292733226 | 29.36602923946585 |
| 9.207740262716625 | 115.80481474889932|
| 28.406414022007812| 74.12808393025578 |
| 3.2801139683765075| -79.74785539671865|
| 38.414250505940146| 28.417748608803993|

Figure 1: 
![alt text][fig1]

Figure 2: 
![alt text][fig2]

Figure 3: 
![alt text][fig3]

[fig1]: https://github.com/lschlessinger1/Global-Terrorism-Database-K-Means/blob/master/fig1.png "Figure 1"
[fig2]: https://github.com/lschlessinger1/Global-Terrorism-Database-K-Means/blob/master/fig2.png "Figure 2"
[fig3]: https://github.com/lschlessinger1/Global-Terrorism-Database-K-Means/blob/master/fig3.png "Figure 3"

[1]: https://www.kaggle.com/START-UMD/gtd
