import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from dateutil.relativedelta import *
from containers.task1 import *
from containers.task4 import *
from containers.task5 import *
from containers.task6 import *
from containers.task3 import *
from containers.task2 import *


# Connection to spark
def connect_to_spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("spark") \
        .getOrCreate()


# Get categories from file
def load_categories(file_name):
    categories_list = []
    with open(file_name, 'r') as f:
        categories = json.load(f)
    for c in categories["items"]:
        if c["snippet"]["assignable"]:
            categories_list.append(CategoryDefault(c["id"], c["snippet"]["title"]))
    return categories_list


# System function for data edit
def change_date(str_date):
    y, d, m = str_date.split(".")
    return y + "." + m + '.' + d


def first_test(data):
    res = []
    selected_videos = data.filter(data.video_error_or_removed == False) \
        .groupBy('title').count().orderBy("count", ascending=False).collect()
    for row in selected_videos[:10]:
        r = data.filter(data.title == row.title).orderBy("trending_date").collect()
        t_d = []
        for _ in r:
            t_d.append(TrendingDayTask1(_.trending_date, _.views, _.likes, _.dislikes))
        video = VideoTask1(r[0].video_id, r[0].title, r[0].description, r[0].views, r[0].likes, r[0].dislikes, t_d)
        res.append(video)
    print("Test 1 is ready")
    return {"videos": res}


def second_test(data, start_date_glob, last_date_glob, categories_list):
    res = []
    current_date = start_date_glob
    next_week = current_date + relativedelta(days=+7)
    while next_week < last_date_glob:
        print(next_week)
        max_views = 0
        for c in categories_list:
            total_views = 0
            video_ids = []
            videos = data.filter(data.trending_date > current_date.strftime("%y.%m.%d")).filter(
                data.trending_date < next_week.strftime("%y.%m.%d")).filter(data.category_id == c.category_id)
            unique_videos = videos.dropDuplicates(subset=['video_id']).collect()
            for v in unique_videos:
                acc_v = videos.filter(videos.title == v.title).orderBy(videos.trending_date)
                if acc_v.count() > 1:
                    total_views += acc_v.orderBy(videos.trending_date,ascending=False).head().views - acc_v.head().views
                    video_ids.append(acc_v.head().video_id)
            if total_views > max_views:
                max_views = total_views
                w = WeekTask2(current_date, next_week, c.category_id, c.category_name, len(video_ids), total_views,
                              video_ids)
            print(c.category_name)
        res.append(w)

        current_date += relativedelta(days=+7)
        next_week += relativedelta(days=+7)
    print("Test 2 is ready")
    return {"weeks": res}


def collect_tags(video_id, tag_str):
    tag_dict = dict()
    tag_str = tag_str.replace('"', "")
    tags = tag_str.split('|')
    for t in tags:
        if t != '[none]':
            if t in tag_dict:
                tag_dict[t].append(video_id)
            else:
                tag_dict[t] = [video_id]
    return tag_dict


def third_test(data, start_date_glob, last_date_glob):
    res = []
    tag_dict = dict()
    current_date = start_date_glob
    next_month = current_date + relativedelta(days=+30)
    while next_month < last_date_glob:
        tag_list = []
        videos = data.dropDuplicates(subset=['video_id']).filter(
            data.trending_date > current_date.strftime("%y.%m.%d")).filter(
            data.trending_date < next_month.strftime("%y.%m.%d")).collect()
        for v in videos:
            tag_dict = collect_tags(v.video_id, v.tags)
        for k in sorted(tag_dict, key=lambda k: len(tag_dict[k]), reverse=True)[:10]:
            tag_list.append(TagTask3(k, len(tag_dict[k]), tag_dict[k]))
        res.append(MonthTask3(current_date, next_month, tag_list))
        current_date += relativedelta(days=+30)
        next_month += relativedelta(days=+30)
    print("Test 3 is ready")
    return {"months": res}


def forth_test(data):
    res = []
    edited_data = data.orderBy("trending_date", ascending=False).dropDuplicates(subset=['video_id'])
    selected_videos = edited_data.groupBy('channel_title').sum("views").orderBy("sum(views)", ascending=False)
    selected_videos = selected_videos.withColumnRenamed("sum(views)", "total_views").collect()
    for video in selected_videos[:20]:
        r = edited_data.filter(edited_data.channel_title == video.channel_title).orderBy("trending_date").collect()
        start_date = r[0].publish_time
        end_date = r[-1].publish_time
        v_s = []
        for tmp in r:
            v_s.append(VideoStatsTask4(tmp.video_id, tmp.views))
        res.append(ChannelTask4(video.channel_title, start_date, end_date, video.total_views, v_s))
    print("Test 4 is ready")
    return {"channels": res}


def fifth_test(data):
    res = []
    video_to_trending_days_list = data.filter(data.video_error_or_removed == False).groupBy('title').count()
    video_to_trending_days_list = video_to_trending_days_list.withColumnRenamed("count", "trending_days")
    channel_to_trending_days = data.groupBy("channel_title").count()
    channel_to_trending_days = channel_to_trending_days.withColumnRenamed("count", "trending_days").orderBy(
        "trending_days", ascending=False).collect()
    for channel in channel_to_trending_days[:10]:
        total_tr_days = 0
        videos = data.filter(data.video_error_or_removed == False).filter(
            data.channel_title == channel.channel_title).select("video_id", "title").distinct().collect()
        selected_v = []
        for video in videos:
            t = video_to_trending_days_list.filter(video_to_trending_days_list.title == video.title).head().trending_days
            v = VideoDayTask5(video.video_id, video.title, t)
            total_tr_days += t
            selected_v.append(v)
        res.append(ChannelTask5(channel.channel_title, total_tr_days, selected_v))
    print("Test 5 is ready")
    return {"channels": res}


def sixth_test(data, categories_list):
    import pyspark.sql.functions as F
    res = []
    for c in categories_list:
        video_list = []
        video_by_category = data.filter(data.category_id == c.category_id).filter(data.views >= 100000).withColumn(
            "ratio", F.col("likes") / F.col("dislikes"))
        video_by_category_modified = video_by_category.groupBy(video_by_category.title).agg(F.max('ratio')).orderBy(
            "max(ratio)", ascending=False).withColumnRenamed("max(ratio)", "ratio").collect()[:10]
        for v in video_by_category_modified:
            video = video_by_category.filter(video_by_category.title == v.title).filter(
                video_by_category.ratio == v.ratio).collect()[0]
            video_list.append(VideoTask6(video.video_id, video.title, video.ratio, video.views))
        res.append(CategoryTask6(c.category_id, c.category_name, video_list))
    print("Test 6 is ready")
    return {"categories": res}


def save_data(file_name, data, number_of_test):
    import os
    os.makedirs("results/" + str(number_of_test) + '/' + file_name[:2] + "/", exist_ok=True)
    with open("results/" + str(number_of_test) + '/' + file_name[:2] + "/result.json", 'w', encoding='utf-8') as f:
        f.write(json.dumps(data, ensure_ascii=False, default=lambda o: o.__dict__, sort_keys=True, indent=4))


def main(file_name, file_c_name):
    spark = connect_to_spark()
    categories = load_categories(file_c_name)
    data = spark.read.csv(file_name, inferSchema=True, header=True, multiLine=True).cache()
    udf_func = udf(change_date)
    data = data.withColumnRenamed("description\r", "description")
    data = data.withColumn("trending_date", udf_func(data.trending_date))
    start_date_glob = data.select("trending_date").distinct().orderBy("trending_date").collect()
    last_date_glob = start_date_glob[-1].trending_date
    start_date_glob = start_date_glob[0].trending_date
    last_date_glob = datetime.strptime(last_date_glob, "%y.%m.%d")
    start_date_glob = datetime.strptime(start_date_glob, "%y.%m.%d")
    start_time = time.time()
    res1 = first_test(data)
    time_1 = time.time()
    res2 = second_test(data, start_date_glob, last_date_glob, categories)
    time_2 = time.time()
    res3 = third_test(data, start_date_glob, last_date_glob)
    time_3 = time.time()
    res4 = forth_test(data)
    time_4 = time.time()
    res5 = fifth_test(data)
    time_5 = time.time()
    res6 = sixth_test(data, categories)
    time_6 = time.time()
    results = [res1,res2,res3,res4,res5,res6]
    times = [start_time, time_1, time_2, time_3, time_4, time_5, time_6]
    for res in range(len(results)):
        save_data(file_name, results[res], res + 1)
    with open("results/" + file_name[:2] + "_analysis.txt", 'w', encoding='utf-8') as f:
        f.write("-" * 19 + "\n")
        for t in range(1, len(times)):
            diff = (times[t] - times[t - 1]).__round__(2)
            f.write("| Test " + str(t) + " " * (8 - len(str(diff))) + str(diff) + "s |\n")
        f.write("-" * 19 + "\n")


if __name__ == "__main__":
   main(sys.argv[1],sys.argv[2])
