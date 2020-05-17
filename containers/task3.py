class MonthTask3:
    def __init__(self,start_date,end_date,tags):
        self.start_date = start_date.strftime("%m/%d/%Y, %H:%M:%S")
        self.end_date = end_date.strftime("%m/%d/%Y, %H:%M:%S")
        self.tags = tags


class TagTask3:
    def __init__(self,tag,number_of_videos,video_ids):
        self.tag = tag
        self.number_of_videos = number_of_videos
        self.video_ids = video_ids

