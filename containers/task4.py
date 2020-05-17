import json

from datetime import datetime

class ChannelTask4:
    def __init__(self,channel_name,start_date,end_date,total_views,videos_views):
        self.channel_name = channel_name
        self.start_date = start_date.strftime("%m/%d/%Y, %H:%M:%S")
        self.end_date = end_date.strftime("%m/%d/%Y, %H:%M:%S")
        self.total_views = total_views
        self.videos_views = videos_views

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)



class VideoStatsTask4:

    def __init__(self,video_id,views):
        self.video_id = video_id
        self.views = views
