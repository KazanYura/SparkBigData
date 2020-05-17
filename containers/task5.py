import json


class ChannelTask5:
    def __init__(self, channel_name, total_trending_days, video_days):
        self.channel_name = channel_name
        self.total_trending_days = total_trending_days
        self.video_days = video_days

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)


class VideoDayTask5:
    def __init__(self, video_id, video_title, trending_days):
        self.video_id = video_id
        self.video_title = video_title
        self.trending_days = trending_days

