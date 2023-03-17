import sys
import time

import requests
import webbrowser
import csv
from typing import Dict, List, Union, Optional, Tuple, Any
from urllib.parse import quote, unquote

# Define Stack Overflow API endpoints
API_BASE_URL = 'https://api.stackexchange.com/2.3/'
API_USERS_ENDPOINT = 'users/{user_ids}'
API_ANSWERS_ENDPOINT = 'answers/{answers_ids}'
API_AUTH_ENDPOINT = 'https://stackoverflow.com/oauth/dialog'

# Define authentication credentials
CLIENT_ID = '25628'
REDIRECT_URI = 'https://stackexchange.com'
SCOPE = 'no_expiry'

NUM_QUESTIONS = 5
MAX_DEPTH = 15
MAX_REQUEST_PER_SECOND = 20
REPUTATION_THRESHOLD = 0

analyzed_users = set()

time_last_request = 0


def send_request(url: str, token: str) -> str:
    global time_last_request
    headers = {'Authorization': 'Bearer ' + token}
    key = "*0NsGzZLCl28rdMwbKiHQg(("
    remaining_time = (1/MAX_REQUEST_PER_SECOND) - (time.time() - time_last_request)
    if remaining_time > 0:
        time.sleep(remaining_time*1.25)
    response = requests.get(url+f"&key={key}", headers=headers)
    time_last_request = time.time()

    try:
        response_json = response.json()
    except ValueError:
        raise Exception("Response is not valid JSON")

    if "backoff" in response_json:
        backoff_seconds = response_json['backoff']
        print(f'Received backoff - wait for {backoff_seconds} seconds before making further requests')
        time.sleep(backoff_seconds*1.10)
        response_json = send_request(url, token)

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        print(f'Quota exceeded - retrying after {retry_after} seconds')
        time.sleep(retry_after)
        response_json = send_request(url, token)
    elif response.status_code != 200:
        raise Exception(f"Request failed with status code {response.status_code}")

    return response_json


def get_answers_to_top_questions_tag(user_id, token, tag, num_of_questions=NUM_QUESTIONS):
    url = API_BASE_URL + API_USERS_ENDPOINT.format(user_ids=user_id) + f"/tags/{quote(tag)}/top-questions" \
                                                                              f"?pagesize={2*num_of_questions}&order" \
                                                                              f"=desc&sort=votes&site=stackoverflow"
    response_data = send_request(url, token)
    if "items" not in response_data:
        raise Exception("Response is missing expected key 'items'")

    top_questions = []
    idx = 0
    found_questions = 0
    list_questions = response_data["items"]
    while idx < len(list_questions) and found_questions < num_of_questions:
        question = list_questions[idx]
        if "accepted_answer_id" in question:
            found_questions += 1
            top_questions.append({
                "score": question["score"],
                "answer_id": question["accepted_answer_id"]
            })
        idx += 1

    return top_questions


def get_user_ids_from_answers(ans_ids, token):
    ans_ids_str = ";".join(str(x) for x in ans_ids)
    url = API_BASE_URL + API_ANSWERS_ENDPOINT.format(answers_ids=ans_ids_str) + "?order=desc&sort=creation&site" \
                                                                                "=stackoverflow"
    response_data = send_request(url, token)
    if "items" not in response_data:
        raise Exception("Response is missing expected key 'items'")

    ans_score_user = []
    answers = response_data["items"]
    for ans in answers:
        if ans["owner"]["user_type"] == "does_not_exist":
            continue
        user_id = ans["owner"]["user_id"]
        ans_score = ans["score"]
        ans_id = ans["answer_id"]
        ans_score_user.append({
            "answer_id": ans_id,
            "user_id": user_id,
            "score": ans_score
        })

    return ans_score_user


# Define function to retrieve user information
def get_users_info(user_ids: List[Union[int, str]], token: str) -> Dict:
    user_ids_str = ";".join(str(x) for x in user_ids)
    url = API_BASE_URL + API_USERS_ENDPOINT.format(user_ids=user_ids_str) + "?order=desc&sort=reputation&site" \
                                                                            "=stackoverflow"
    response_data = send_request(url, token)
    if "items" not in response_data:
        raise Exception("Response is missing expected key 'items'")

    users_info = {}
    for user_info in response_data["items"]:
        users_info[user_info['user_id']] = {
                'display_name': unquote(user_info['display_name']),
                'reputation': user_info["reputation"],
            }

    return users_info


# Define function to retrieve user's most used programming language
def get_user_top_tag(user_id: int, token: str) -> str:
    url = API_BASE_URL + API_USERS_ENDPOINT.format(user_ids=user_id) + '/top-tags?pagesize=1&site=stackoverflow'
    response_data = send_request(url, token)
    if "items" not in response_data:
        raise Exception("Response is missing expected key 'items'")
    items = response_data['items']
    if len(items) == 0:
        return None

    top_tag = unquote(items[0]['tag_name'])
    return top_tag


# Define function to recursively retrieve user relationships and reputations up to a depth of 5
def get_user_relationships(user_ids: List[Union[int, str]], token: str, depth: int = 1, reputation_threshold: int = 0) -> \
        Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if depth > MAX_DEPTH:
        return [], []

    nodes = []
    edges = []
    users_info = get_users_info(user_ids, token)
    for user_id, user_info in users_info.items():
        if user_id in analyzed_users or user_info["reputation"] < reputation_threshold:
            continue
        analyzed_users.add(user_id)

        answers_ids = []
        new_users_ids = []
        user_top_tag = get_user_top_tag(user_id, token)
        if user_top_tag is not None:
            top_answers = get_answers_to_top_questions_tag(user_id, token, user_top_tag)
            if len(top_answers) > 0:
                answers_ids = [str(answer["answer_id"]) for answer in top_answers]
                ans_users_ids = get_user_ids_from_answers(answers_ids, token)
                for answer in ans_users_ids:
                    edge = {
                        'source': user_id,
                        'target': answer["user_id"],
                        'score': answer["score"],
                        'ans_id': answer["answer_id"]
                    }
                    edges.append(edge)
                    new_users_ids.append(answer["user_id"])

        node = {
            'user_id': user_id,
            'name': user_info["display_name"],
            'question_tag': user_top_tag,
            'reputation': user_info["reputation"]
        }
        nodes.append(node)

        print(
            f"{' ' * (depth - 1)}{depth} - User: {user_info['display_name']} (Reputation: {user_info['reputation']}, "
            f"Top tag: {user_top_tag} Top answers: {answers_ids})")

        if len(new_users_ids) > 0:
            new_nodes, new_edges = get_user_relationships(new_users_ids, token, depth + 1)
            nodes += new_nodes
            edges += new_edges

    return nodes, edges


def write_nodes_to_csv(nodes, filepath):
    with open(filepath, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Id', 'Label', 'Question_Tag', 'Weight'])
        for node in nodes:
            writer.writerow([node['user_id'], node['name'], node['question_tag'], node['reputation']])
    print(f'Successfully wrote {len(nodes)} nodes to {filepath}.')


def write_edges_to_csv(edges, filepath):
    with open(filepath, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Source', 'Target', 'Type', 'Weight', 'Ans_ID'])
        for edge in edges:
            writer.writerow([edge['source'], edge['target'], 'Directed', edge['score'], edge['ans_id']])
    print(f'Successfully wrote {len(edges)} edges to {filepath}.')


if __name__ == "__main__":
    # Begin user authentication process
    auth_params = {
        'client_id': CLIENT_ID,
        'redirect_uri': REDIRECT_URI,
        'scope': SCOPE
    }
    auth_url = API_AUTH_ENDPOINT + '?' + '&'.join([f"{k}={v}" for k, v in auth_params.items()])
    webbrowser.open(auth_url)

    # Wait for user to grant permission and be redirected back to our application with an access token

    access_token = ""
    while not access_token:
        response_url = input("Enter the URL you were redirected to after granting permission: ")
        if 'access_token' in response_url:
            access_token = response_url.split('=')[1]

    # Use access token to retrieve the authenticated user's ID
    me_params = {
        'access_token': access_token
    }
    init_user_id = "4357115"
    #init_user_id = "276068"
    # Call the function to retrieve user relationships and reputations recursively up to a depth of 5
    data_nodes, data_edges = get_user_relationships([init_user_id], access_token, 1, REPUTATION_THRESHOLD)
    write_nodes_to_csv(data_nodes, "nodes.csv")
    write_edges_to_csv(data_edges, "edges.csv")
