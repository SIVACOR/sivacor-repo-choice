"""
dockerhub_filter.py

Fetches Docker Hub repositories and tags for specified namespaces, applies filtering rules from a YAML config, and outputs filtered and unfiltered lists as YAML.

Usage:
    python dockerhub_filter.py CONFIG.yaml [--allowed ALLOWED.yaml] [--all ALL.yaml]

Arguments:
    CONFIG.yaml           Path to YAML config file (required)

Options:
    --allowed FILE        Output file for filtered repositories (default: allowed_repos.yaml)
    --all FILE            Output file for all repositories (default: all_repos.yaml)
    -h, --help            Show this help message and exit
"""

import argparse
import requests
import yaml

import re
from collections import defaultdict


DOCKER_HUB_TAGS_API = "https://hub.docker.com/v2/namespaces/{namespace}/repositories/{repository}/tags?page_size=100"
DOCKER_HUB_REPOS_API = "https://hub.docker.com/v2/namespaces/{namespace}/repositories?page_size=100"


def load_config(config_path):
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)



def fetch_all_repositories(namespace):
    repos = []
    url = DOCKER_HUB_REPOS_API.format(namespace=namespace)
    while url:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        repos.extend(data.get('results', []))
        url = data.get('next')
    return repos

def fetch_all_tags(namespace, repository):
    tags = []
    url = DOCKER_HUB_TAGS_API.format(namespace=namespace, repository=repository)
    while url:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        tags.extend(data.get('results', []))
        url = data.get('next')
    return tags



def filter_names(names, filters):
    filtered = names
    for rule in filters:
        if 'repo_regex' in rule:
            regex = re.compile(rule['repo_regex'])
            filtered = [n for n in filtered if regex.match(n)]
    return filtered



# Natural sort helper
def natural_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]

def filter_tags(tags, filters):
    filtered = tags
    for rule in filters:
        # Only one of tag-related filters per rule dict
        if 'tag_regex' in rule:
            regex = re.compile(rule['tag_regex']) if rule['tag_regex'] else None
            if regex:
                filtered = [t for t in filtered if regex.match(t['name'])]
            # Blacklist
            if 'blacklist' in rule:
                blacklist = set(rule['blacklist'])
                filtered = [t for t in filtered if t['name'] not in blacklist]
            # keep_latest_n
            if 'keep_latest_n' in rule:
                n = rule['keep_latest_n']
                def tag_sort_key(t):
                    if t['name'] == 'latest':
                        return (0, )
                    return (1, [(-x if isinstance(x, int) else x) for x in natural_key(t['name'])])
                filtered = sorted(filtered, key=tag_sort_key)
                filtered = filtered[:n]
        elif rule.get('keep_most_recent'):
            if filtered:
                filtered = [max(filtered, key=lambda t: t['last_updated'])]
    return filtered

def main(config_path, allowed_output_path, all_repos_output_path):
    config = load_config(config_path)
    output = defaultdict(list)
    all_repos_output = defaultdict(list)
    import copy
    software_output = defaultdict(lambda: defaultdict(list))
    software_list = config.get('software', [])
    software_meta = {s['name']: s for s in software_list}
    software_sortorder = {k: v.get('sort_order', 999) for k, v in software_meta.items()}
    software_desc = {k: v.get('description', '') for k, v in software_meta.items()}
    unknown_software = set()
    for repo_conf in config['repositories']:
        namespace = repo_conf['namespace']
        repo_filters = [f for f in repo_conf.get('filters', []) if 'repo_regex' in f]
        tag_filters = [f for f in repo_conf.get('filters', []) if 'tag_regex' in f or f.get('keep_most_recent') or 'keep_latest_n' in f]
        software_list = repo_conf.get('software', [])
        for software in software_list:
            if software not in software_meta:
                unknown_software.add(software)

        # Get all repositories in the namespace
        all_repos = fetch_all_repositories(namespace)
        repo_names = [r['name'] for r in all_repos]
        all_repos_output[namespace] = repo_names
        filtered_repo_names = filter_names(repo_names, repo_filters) if repo_filters else repo_names

        for repository in filtered_repo_names:
            tags = fetch_all_tags(namespace, repository)
            filtered_tags = filter_tags(tags, tag_filters) if tag_filters else tags
            # Sort tag names in descending natural order before output
            tag_names = [t['name'] for t in filtered_tags]
            tag_names_sorted = sorted(tag_names, key=natural_key, reverse=True)
            repo_key = f"{namespace}/{repository}"
            output[repo_key] = tag_names_sorted
            for software in software_list:
                software_output[software][repo_key] = copy.deepcopy(tag_names_sorted)

    repo_sortorder = {}
    for repo_key in output:
        found = False
        for repo_conf in config['repositories']:
            namespace = repo_conf['namespace']
            software_list = repo_conf.get('software', [])
            for software in software_list:
                sorder = software_sortorder.get(software, 999)
                if repo_key.startswith(namespace):
                    if repo_key not in repo_sortorder or sorder < repo_sortorder[repo_key]:
                        repo_sortorder[repo_key] = sorder
                    found = True
            if found:
                break
        if not found:
            repo_sortorder[repo_key] = 999
    sorted_repos = sorted(output.keys(), key=lambda r: repo_sortorder.get(r, 999))
    allowed_sorted = {k: output[k] for k in sorted_repos}
    with open(allowed_output_path, 'w', encoding='utf-8') as f:
        yaml.dump(allowed_sorted, f)

    with open(all_repos_output_path, 'w', encoding='utf-8') as f:
        yaml.dump(dict(all_repos_output), f)

    sorted_software = sorted(software_output.keys(), key=lambda s: software_sortorder.get(s, 999))
    allowed_by_software = {}
    for k in sorted_software:
        allowed_by_software[k] = {
            'description': software_desc.get(k, ''),
            'repos': dict(software_output[k])
        }
    with open('allowed_repos_by_software.yaml', 'w', encoding='utf-8') as f:
        yaml.safe_dump(allowed_by_software, f, default_flow_style=False, sort_keys=False)

    if unknown_software:
        for s in unknown_software:
            print(f"[ERROR] Software '{s}' is referenced in a repository but not defined in the 'software' section of the config.")
            print(f"Suggested template to add to your YAML config:\\n  {s}:\\n    sortorder: <number>\\n    description: '<description>'\\n")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch and filter Docker Hub repositories/tags as specified in a YAML config."
    )
    parser.add_argument(
        "config",
        metavar="CONFIG.yaml",
        help="YAML config file specifying namespaces and filtering rules."
    )
    parser.add_argument(
        "--allowed",
        metavar="ALLOWED.yaml",
        default="allowed_repos.yaml",
        help="Output file for filtered repositories (default: allowed_repos.yaml)"
    )
    parser.add_argument(
        "--all",
        metavar="ALL.yaml",
        default="all_repos.yaml",
        help="Output file for all repositories (default: all_repos.yaml)"
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    main(args.config, args.allowed, args.all)
