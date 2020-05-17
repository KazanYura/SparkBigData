class CategoryTask6:
    def __init__(self, category_id, category_name, videos):
        self.category_id = category_id
        self.category_name = category_name
        self.videos = videos


class VideoTask6:
    def __init__(self, video_id, video_title, ratio_likes_dislikes, views):
        self.video_id = video_id
        self.video_title = video_title
        self.ratio_likes_dislikes = ratio_likes_dislikes
        self.views = views

class CategoryDefault:
    def __init__(self, category_id, category_name):
        self.category_id = category_id
        self.category_name = category_name

    def toString(self):
        return "Category:" + self.category_name
