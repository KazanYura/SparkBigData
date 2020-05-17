import json


class VideoTask1:
    def __init__(self, id,title,description,latest_view,latest_likes,latest_dislikes,trending_days):
        self.trending_days = trending_days
        self.id = id
        self.title = title
        self.description = description
        self.latest_view = latest_view
        self.latest_likes = latest_likes
        self.latest_dislikes = latest_dislikes

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)


class TrendingDayTask1:
    def __init__(self,date,views,likes,dislikes):
        self.date = date
        self.views = views
        self.likes = likes
        self.dislikes = dislikes
