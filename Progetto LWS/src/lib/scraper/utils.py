from bs4 import BeautifulSoup
import requests
import random
from .user_agents import user_agents_list
import time
from tqdm import tqdm

# Restituisce un user_agent randomico tra quelli presenti in user_agents_list
def get_random_user_agent():
    
    return random.choice(user_agents_list)