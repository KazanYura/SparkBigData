class WeekTask2:
    def __init__(self,start_date,end_date,category_id,category_name,number_of_videos,total_view,video_ids):
        self.start_date = start_date.strftime("%m/%d/%Y, %H:%M:%S")
        self.end_date = end_date.strftime("%m/%d/%Y, %H:%M:%S")
        self.category_id = category_id
        self.category_name = category_name
        self.number_of_videos = number_of_videos
        self.total_views = total_view
        self.video_ids = video_ids

