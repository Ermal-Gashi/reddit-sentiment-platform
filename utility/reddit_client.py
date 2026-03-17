import praw
from config import REDDIT_CLIENT

def make_client():
    return praw.Reddit(**REDDIT_CLIENT)
