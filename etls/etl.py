import praw
import sys
import pandas as pd
import numpy as np
from utils.constants import POST_FIELDS

def connect_api(client_id, client_secret, user_agent) -> 'praw.Reddit':
    try:
        reddit = praw.Reddit(client_id=client_id,
                             client_secret=client_secret,
                             user_agent=user_agent)
        
        print("Connected to Reddit!")
        return reddit
    except Exception as e:
        print(e)
        sys.exit(1)

# Update the type hint for reddit_instance to 'praw.Reddit'
def post_extraction(reddit_instance: 'praw.Reddit', subreddit: str, time_filter: str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    post_lists = []

    for post in posts:
        post_dict = vars(post)
        print(post_dict)
        post = {key: post_dict[key] for key in POST_FIELDS}
        post_lists.append(post)
    

    return post_lists

def data_transform(redd_post: pd.DataFrame):
    # Convert 'created_utc' to datetime
    redd_post['created_utc'] = pd.to_datetime(redd_post['created_utc'], unit='s')

    # Convert 'author' to string
    redd_post['author'] = redd_post['author'].astype(str)

    # Ensure 'num_comments' and 'score' are integers
    redd_post['num_comments'] = redd_post['num_comments'].astype(int)
    redd_post['score'] = redd_post['score'].astype(int)

    # Convert 'upvote_ratio' to a ratio (integer handling might not be appropriate as it's a ratio)
    redd_post['upvote_ratio'] = redd_post['upvote_ratio'].astype(float)

    # Convert 'title' to string
    redd_post['title'] = redd_post['title'].astype(str)

    # Handle newly added fields
    # 'locked' as boolean
    redd_post['locked'] = redd_post['locked'].astype(bool)

    # For textual fields like 'removal_reason', 'report_reasons', 'removed_by', 'mod_reason_by', convert them to strings
    # It's also important to handle missing or NaN values appropriately
    text_fields = ['removal_reason', 'report_reasons', 'removed_by', 'mod_reason_by']
    for field in text_fields:
        redd_post[field] = redd_post[field].fillna('NA').astype(str)

    return redd_post


def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)