from argparse import ArgumentParser
import sqlite3
import os
import json
from typing import List


def get_player_fields() -> List[str]:
    player_fields = [
        'hash TEXT PRIMARY KEY',
        'name TEXT',
        # NOTE: In theory some players could play on multiple teams
        'team TEXT',
    ]
    return player_fields


def get_match_fields() -> List[str]:
    match_fields = [
            'id INT PRIMARY KEY',
            'balls_per_over INT DEFAULT 6',
            'city TEXT',
            # Maybe just keep start/end dates?
            # Some SQL implementations have a DATE type but not sqlite (?)
            'dates BLOB',
            # TODO: maintain separate event database?
            'event_match_number INT',
            'event_name TEXT',
            'gender TEXT',
            'match_type TEXT',
            'match_type_number INT',
            # ...
            'season TEXT',
            'team_type TEXT',
            # teams should always be a list of 2 teams
            'team_a TEXT',
            'team_b TEXT',
            # .. many more
        ]
    return match_fields


def get_schema(table_name: str, fields: List[str]) -> str:
    fields_lines = ',\n'.join(fields)
    schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
        {fields_lines}
        )
        """
    return schema


def build_from_cricsheet():
    parser = ArgumentParser("build from cricsheet")
    parser.add_argument("input_jsons", nargs="+",
                        help="Input JSON match files")
    parser.add_argument("--db", default=":memory:", help="sqlite db")
    args = parser.parse_args()

    db_con = sqlite3.connect(args.db)
    cur = db_con.cursor()

    match_schema = get_schema("matches", get_match_fields())
    print(match_schema)
    # Could executemany or executescript for greater efficiency
    cur.execute(match_schema)

    match_insert_fields = (
        "id",
        "match_type",
        "match_type_number",
        # "dates", # blob doesn't work right now
        "team_a",
        "team_b",
    )
    input_jsons = args.input_jsons
    for match_json in input_jsons[:10]:
        match_data = {}
        match_json_base = os.path.basename(match_json)
        assert match_json_base[-5:] == ".json"
        match_data["id"] = int(match_json_base[:-5])
        # for match_json in input_jsons:
        with open(match_json, 'r') as fin:
            match_dict = json.load(fin)
        # keys are ['meta', 'info', 'innings']
        # print(match_dict.keys())
        match_meta = match_dict["meta"]
        match_data["data_version"] = match_meta["data_version"]
        match_data["data_revision"] = match_meta["revision"]
        match_data["data_created"] = match_meta["created"]

        match_info = match_dict["info"]
        print(match_info)
        # "innings" has the ball-by-ball data
        innings = match_dict["innings"]
        # print(innings)
        match_data["match_type"] = match_info["match_type"]
        match_data["match_type_number"] = match_info["match_type_number"]
        # match_data["dates"] = match_info["dates"]
        [team_a, team_b] = match_info["teams"]
        match_data["team_a"] = team_a
        match_data["team_b"] = team_b
        match_values = tuple([match_data[k] for k in match_insert_fields])

        # TODO: This would be more efficient if we inserted multiple matches at
        # once
        cur.execute(
                f"""
                INSERT INTO matches
                {match_insert_fields}
                VALUES {match_values}
                """
                )
        # print(match_meta)
        # print(match_info)

    for row in cur.execute("SELECT * FROM matches LIMIT 3"):
        print(row)


if __name__ == "__main__":
    build_from_cricsheet()
