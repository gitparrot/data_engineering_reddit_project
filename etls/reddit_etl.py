import praw
import sys
import pandas as pd
import numpy as np
from utils.constants import POST_FIELDS

def connect_reddit(client_id, client_secret, user_agent) -> 'praw.Reddit':
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
def extract_posts(reddit_instance: 'praw.Reddit', subreddit: str, time_filter: str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    post_lists = []

    for post in posts:
        post_dict = vars(post)
        post = {key: post_dict[key] for key in POST_FIELDS}
        post_lists.append(post)
    
    return post_lists

def transform_data(post_df: pd.DataFrame):
    # Convert 'created_utc' to datetime
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')

    # Ensure 'over_18' is a boolean
    post_df['over_18'] = post_df['over_18'].astype(bool)

    # Convert 'author' to string
    post_df['author'] = post_df['author'].astype(str)

    # Handle 'edited' field
    # Assuming edited is True if post was edited, False otherwise, and replace non-boolean with mode
    edited_mode = (post_df['edited'] != False).mode()[0]  # Mode after excluding False values
    post_df['edited'] = np.where(post_df['edited'] == False, False, edited_mode)

    # Ensure 'num_comments' and 'score' are integers
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)

    # Convert 'upvote_ratio' to a ratio (integer handling might not be appropriate as it's a ratio)
    post_df['upvote_ratio'] = post_df['upvote_ratio'].astype(float)

    # Convert 'title' to string
    post_df['title'] = post_df['title'].astype(str)

    # Handle newly added fields
    # 'locked' as boolean
    post_df['locked'] = post_df['locked'].astype(bool)

    # For textual fields like 'removal_reason', 'report_reasons', 'removed_by', 'mod_reason_by', convert them to strings
    # It's also important to handle missing or NaN values appropriately
    text_fields = ['removal_reason', 'report_reasons', 'removed_by', 'mod_reason_by']
    for field in text_fields:
        post_df[field] = post_df[field].fillna('Not Available').astype(str)

    return post_df


def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)