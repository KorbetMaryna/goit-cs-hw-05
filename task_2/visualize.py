import string
import asyncio
import httpx
import logging

from matplotlib import pyplot as plt
from collections import defaultdict, Counter


async def get_text(url):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.text
        except (httpx.HTTPError, httpx.RequestError) as e:
            logging.error(f"Error while fetching URL: {e}")
            return None

def remove_punctuation(text):
    return text.translate(str.maketrans("", "", string.punctuation))

async def map_function(word):
    return word, 1

def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

async def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)

async def map_reduce(url):
    text = await get_text(url)
    if text is None:
        return {}
    
    text = remove_punctuation(text)
    words = text.split()

    mapped_result = await asyncio.gather(*[map_function(word) for word in words])
    shuffled_values = shuffle_function(mapped_result)

    reduced_result = await asyncio.gather(*[reduce_function(values) for values in shuffled_values])
    return dict(reduced_result)

def visualize_top_words(result, top_n=10):
    top_words = Counter(result).most_common(top_n)
    words, counts = zip(*top_words)

    plt.figure(figsize=(10, 6))
    plt.barh(words, counts, color='skyblue')
    plt.xlabel('Frequency')
    plt.ylabel('Words')
    plt.title(f'Top {top_n} Most Frequent Words')
    plt.show()

if __name__ == '__main__':
    format = "%(threadName)s %(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    result = asyncio.run(map_reduce(url))
    visualize_top_words(result, top_n=10)