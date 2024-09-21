import json
import time
import requests
import tweepy
import os
from twarc import Twarc2, expansions



auth = tweepy.OAuthHandler(API_KEY, API_KEY_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True)
client = Twarc2(bearer_token= bearer_token)

gpt_console_output = open("gpt_console_output.txt", "a", encoding="utf-8")



def write_json(file_name, object):
    file_path='{}.json'.format(file_name)
    print(file_path)
    with open(file_path, 'a', encoding='utf-8') as file:
        # Check if file is empty
        file.seek(0)
        first_char = file.read(1)
        print("here")
        if not first_char:
            # If the file is empty, write the first JSON object without a comma
            file.write(json.dumps(object))
        else:
            # If the file is not empty, add a comma before writing the new JSON object
            file.seek(0, 2)  # Move the file pointer to the end of the file
            file.write(',')
            file.write(json.dumps(object))
    file.close()



def create_request(query_params):
    headers = {
        "Authorization": f"Bearer {bearer_token}"
    }

    response = requests.get("https://api.twitter.com/2/tweets/search/all", headers=headers, params=query_params)

    if response.status_code != 200:
        if response.status_code == 429:
            # Wait for 15 minutes before making a new request
            print("Rate limit exceeded. Waiting for 15 minutes...")
            gpt_console_output.write("Rate limit exceeded. Waiting for 15 minutes...")
            time.sleep(15 * 60)
            return create_request(query_params)
        elif response.status_code == 130 or response.status_code == 500 or response.status_code == 503 or response.status_code == 504:
            # Wait for 15 minutes before making a new request
            print("Rate limit exceeded. Waiting for 1 minute...")
            gpt_console_output.write("Rate limit exceeded. Waiting for 1 minute...")
            time.sleep(60)
            return create_request(query_params)
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
            gpt_console_output.write(f"Request failed with status code {response.status_code}: {response.text}")
            raise Exception(f"Request failed with status code {response.status_code}: {response.text}")

    json_response = response.json()
    return json_response



def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r



def connect_to_endpoint(url, user_fields):
    response = requests.request("GET", url, auth=bearer_oauth)

    if response.status_code == 429:
        # Wait for 15 minutes before making a new request
        print("Rate limit exceeded. Waiting for 15 minutes...")
        gpt_console_output.write("Rate limit exceeded. Waiting for 15 minutes...")
        time.sleep(15 * 60)
        return connect_to_endpoint(url, user_fields)

    elif response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def main():
    gpt_tweets = open("gpt_tweets.json", "a", encoding="utf-8")
    gpt_users = open("gpt_tweeters.json", "a", encoding="utf-8")
    gpt_authors_ids = open("gpt_users_ids.json", "a", encoding="utf-8")
    gpt_twt_ids = open("gpt_twt_ids.json", "a", encoding="utf-8")
    gpt_console_output = open("gpt_console_output.txt", "a", encoding="utf-8")


    twt_ids = []
    authors = []


    # get english chatGPT tweets/retweets
    count = 0
    query_params = {
        "query": "(Chat GPT OR ChatGPT OR GPT) lang:en",
        "start_time": "2022-12-02T17:00:00.000Z",
        "end_time": "2023-01-02T17:00:00.000Z",
        "max_results": 500,
        "tweet.fields": "id,created_at,lang,text,author_id,public_metrics,entities,geo,in_reply_to_user_id,withheld",
        "user.fields": "id,username,name,description,location,created_at,verified,protected,public_metrics",
        "expansions": "author_id"
    }

    # the first page
    try:
        json_response = create_request(query_params)
        if "meta" in json_response:
            count += json_response['meta']['result_count']

        if "data" in json_response:
            for i in range(len(json_response['data'])):
                try:
                    print(json_response['data'][i])
                    # get authors
                    try:
                        user = api.get_user(user_id=json_response['data'][i]['author_id'])
                        user_str = json.dumps(user._json, ensure_ascii=False)
                        print(user_str)
                        json_user = json.loads(user_str)  # convert string to json
                        screen_name = json_user["screen_name"]
                        gpt_console_output.write(str(user_str))
                        gpt_users.write(user_str)
                        authors.append(json_response['data'][i]['author_id'])
                        gpt_authors_ids.write(json_response['data'][i]['author_id']+ '\n')
                    except ValueError as e:
                        print(f"Error: {e}")
                        gpt_console_output.write(f"Error: {e}")

                    json_response['data'][i]["screen_name"] = screen_name  # add screen name to tweet
                    gpt_console_output.write(str(json_response['data'][i]))
                    newJson = json.dumps(json_response['data'][i], ensure_ascii=False)
                    gpt_tweets.write(newJson)
                    gpt_twt_ids.write(json_response['data'][i]['id']+ '\n')
                    twt_ids.append(json_response['data'][i]['id'])

                except ValueError as e:
                    print(f"Error: {e}")
                    gpt_console_output.write(f"Error: {e}")

    except ValueError as e:
        print(f"Error: {e}")
        gpt_console_output.write(f"Error: {e}")

    # next pages
    try:
        while ('next_token' in json_response['meta']):
            try:
                print("next page")
                gpt_console_output.write("next page")
                next = json_response.get("meta", {}).get("next_token")
                query_params["next_token"] = next
                json_response = create_request(query_params)
                if "meta" in json_response:
                    count += json_response['meta']['result_count']

                if "data" in json_response:
                    for i in range(len(json_response['data'])):
                        print(json_response['data'][i])
                        # get authors
                        try:
                            user = api.get_user(user_id=json_response['data'][i]['author_id'])
                            user_str = json.dumps(user._json, ensure_ascii=False)
                            print(user_str)
                            json_user = json.loads(user_str)  # convert string to json
                            screen_name = json_user["screen_name"]
                            gpt_console_output.write(str(user_str))
                            gpt_users.write(user_str)
                            gpt_authors_ids.write(json_response['data'][i]['author_id']+ '\n')
                            authors.append(json_response['data'][i]['author_id'])
                        except ValueError as e:
                            print(f"Error: {e}")
                            gpt_console_output.write(f"Error: {e}")

                        json_response['data'][i]["screen_name"] = screen_name  # add screen name to tweet
                        gpt_console_output.write(str(json_response['data'][i]))
                        newJson = json.dumps(json_response['data'][i], ensure_ascii=False)
                        gpt_tweets.write(newJson)
                        gpt_twt_ids.write(json_response['data'][i]['id']+ '\n')
                        twt_ids.append(json_response['data'][i]['id'])

            except ValueError as e:
                print(f"Error: {e}")
                gpt_console_output.write(f"Error: {e}")

    except ValueError as e:
        print(f"Error: {e}")
        gpt_console_output.write(f"Error: {e}")


    print(count)
    gpt_console_output.write(str(count))



if __name__ == "__main__":
    main()