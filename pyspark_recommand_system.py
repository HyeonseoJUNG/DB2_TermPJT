import sklearn
import random
import findspark
findspark.init()

import pyspark
# Pyspark Library #
# SQL
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import mean, col, split, regexp_extract, when, lit

# ML
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, IndexToString
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import os
import pandas as pd
import numpy as np
os.chdir('/Users/user/Downloads/ml-25m')

class RecommandSystem():
    def __init__(self):
        super().__init__()
        # 스파크 세션 만들기
        self.spark = SparkSession\
                .builder\
                .appName('recommender_system')\
                .getOrCreate()

        self.df_movies = self.spark.read.csv(os.getcwd() + '/movies.csv',
                                   inferSchema=True, header=True)
        # self.df_movies.limit(10).toPandas()

        self.df_ratings = self.spark.read.csv(os.getcwd() + '/ratings.csv',
                                    inferSchema=True, header=True)
        # df_ratings.limit(10).toPandas()
        self.df_ratings = self.df_ratings.drop('timestamp')
        # self.df_ratings.limit(10).toPandas()

        from pyspark.sql.functions import isnan, when, count, col, isnull

        self.df_ratings.select([count(when(isnull(c), c)).alias(c) for c in self.df_ratings.columns]).show()

        # ALS recommender algorithm
        from pyspark.ml.recommendation import ALS
        self.df_ratings = self.df_ratings.limit(50000)
        self.train, self.test = self.df_ratings.randomSplit([0.75, 0.25])

        self.rec = ALS(maxIter=10,
                  regParam=0.01,
                  userCol='userId',
                  itemCol='movieId',
                  ratingCol='rating',  # label -> predict할 때는 필요 없음!
                  nonnegative=True,
                  coldStartStrategy='drop')
        # ALS모델 학습 -> dataframe을 넣어주기
        self.rec_model = self.rec.fit(self.train)

        # transform을 이용해 예측 -> dataframe을 넣어주기
        self.pred_ratings = self.rec_model.transform(self.test)
        self.pred_ratings.limit(5).toPandas()

        from pyspark.ml.evaluation import RegressionEvaluator
        evaluator = RegressionEvaluator(labelCol='rating',
                                        predictionCol='prediction',
                                        metricName='rmse')
        # evaluate 메소드에 예측값 담겨있는 dataframe 넣어주기
        self.rmse = evaluator.evaluate(self.pred_ratings)

        self.mae_eval = RegressionEvaluator(labelCol='rating',
                                       predictionCol='prediction',
                                       metricName='mae')
        self.mae = self.mae_eval.evaluate(self.pred_ratings)

        print("RMSE:", self.rmse)
        print("MAE:", self.mae)
        self.unique_movies = self.pred_ratings.select("movieId").distinct()
        print("Finish Initialized")

    def top_movies(self, user_id, n):
        """
        특정 user_id가 좋아할 만한 n개의 영화 추천해주는 함수
        """
        # unique_movies 데이터프레임을 'a'라는 데이터프레임으로 alias시키기
        a = self.unique_movies.alias('a')

        # 특정 user_id가 본 영화들만 담은 새로운 데이터프레임 생성
        watched_movies = self.pred_ratings.filter(self.pred_ratings['userId'] == user_id) \
            .select('movieId')

        # 특정 user_id가 본 영화들을 'b'라는 데이터프레임으로 alias시키기
        b = watched_movies.alias('b')

        # unique_movies를 기준으로 watched_movies를 조인시켜서 user_id가 보지 못한 영화들 파악 가능
        total_movies = a.join(b, a['movieId'] == b['movieId'],
                              how='left')
        # b 데이터프레임의 title_new값이 결측치를 갖고 있는 행의 a.title_new를 뽑아냄으로써 user_id가 아직 못본 영화들 추출
        # col('b.title_new') => b 데이터프레임의 title_new칼럼 의미(SQL처럼 가능!)
        remaining_movies = total_movies \
            .where(col('b.movieId').isNull()) \
            .select('a.movieId').distinct()
        # remaining_movies 데이터프레임에 특정 user_id값을 동일하게 새로운 변수로 추가해주기
        remaining_movies = remaining_movies.withColumn('userId',
                                                       lit(int(user_id)))
        # 위에서 만든 ALS 모델을 사용하여 추천 평점 예측 후 n개 만큼 view ->
        recommender = self.rec_model.transform(remaining_movies) \
            .orderBy('prediction', ascending=False) \
            .limit(n)
        # movieId에 해당하는 title과 genres를 가져오기 위하여 recommender와 df_movies를 join
        final_recommendations = recommender.join(self.df_movies, on=['movieId'], how='left_outer')

        return final_recommendations

    def top_total_movies(self):
        from pyspark.sql import functions as F
        top5_movies = self.df_ratings.drop('userId').groupBy('movieId').agg(F.sum('rating'))

        # df_temp = df_movies.drop('genres')
        top5_movies = top5_movies.join(self.df_movies, self.df_movies.movieId == top5_movies.movieId).drop(self.df_movies.movieId)
        top5_movies = top5_movies.groupBy('movieId', 'title', 'sum(rating)').count()
        top5_movies = top5_movies.orderBy(['sum(rating)'], ascending=[False])
        top5_movies = top5_movies.limit(5)

        # title에 해당하는 genres를 가져오기 위하여 top5_movies와 df_movies를 join
        final_top5_movies = top5_movies.join(self.df_movies, on=['title'], how='left_outer')
        return final_top5_movies