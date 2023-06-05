from fastapi import FastAPI
from pydantic import BaseModel
from bson import ObjectId
import motor.motor_asyncio
import pydantic
import numpy as np
import datetime
from fastapi.middleware.cors import CORSMiddleware
from config import MONGO_DETAILS

pydantic.json.ENCODERS_BY_TYPE[ObjectId] = str


app = FastAPI()


origins = [
    "http://localhost",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)
db = client.test


@app.get("/train/{train_number}")
async def root(train_number):
    collection = db.get_collection("trains").find({"number": int(train_number)})
    my_list = []
    async for element in collection:
        print(element)
    return my_list


@app.get("/trains/")
async def trains(train_number, day, month, year):
    collection = db.get_collection("trains").find(
        {
            "number": int(train_number),
            "day": int(day),
            "month": int(month),
            "year": int(year),
        }
    )
    my_list = []
    async for element in collection:
        my_list.append((element["day"], element["month"], element["year"]))
    return my_list


@app.get("/{train_id}/stations")
async def train_stations(train_id):
    train = await db.trains.find_one({"_id": ObjectId(train_id)})
    print(train)
    return train


@app.get("/date/{date}/")
async def train_in_date(date):
    collection = db.trains.find(
        {"year": int(date[:4]), "month": int(date[5:7]), "day": int(date[8:10])}
    )
    my_list = []
    async for train in collection:
        my_list.append(train)
    return my_list


@app.get("/big-delay/")
async def big_delay():
    collection = db.trains.aggregate(
        [
            {
                "$project": {
                    "schedule": {
                        "$filter": {
                            "input": "$schedule",
                            "as": "schedule",
                            "cond": {"$gte": ["$$schedule.arrival_delay", 100]},
                        }
                    }
                }
            },
            {"$match": {"schedule.0": {"$exists": True}}},
        ]
    )
    my_list = []
    async for element in collection:
        my_list.append(element)
    return my_list


@app.get("/travel-time-example")
async def travel_time_example():
    collection = db.trains.aggregate(
        [
            {"$match": {"name": "MALCZEWSKI"}},
            {"$unwind": {"path": "$schedule"}},
            {
                "$group": {
                    "_id": "$schedule.point.name",
                    "avg_travel_time": {"$avg": "$schedule.travel_time"},
                }
            },
            {"$match": {"avg_travel_time": {"$ne": None}}},
        ]
    )
    my_list = []
    async for element in collection:
        print(element)
        my_list.append(element)
    return my_list


@app.get("/travel-time-in-year/{category}/{year}/")
async def travel_time_in_year(category, year):
    category = int(category)
    if category == 0:
        category = [1, 2]
    collection = db.trains.aggregate(
        [
            {"$match": {"year": int(year), "category": category}},
            {"$unwind": {"path": "$schedule"}},
            {
                "$group": {
                    "_id": {
                        "month": "$month",
                        "point_position": "$schedule.point.position",
                    },
                    "avg_travel_time": {"$avg": "$schedule.travel_time"},
                    "avg_stop_time": {"$avg": "$schedule.stop_time"},
                }
            },
            {"$match": {"avg_travel_time": {"$ne": None}}},
            {"$sort": {"_id.point_position": 1, "_id.month": 1}},
        ]
    )
    my_list = []
    async for element in collection:
        print(element)
        my_list.append(element)
    return my_list


class QueryDate(BaseModel):
    day: int
    month: int
    year: int


@app.post("/get-timetable")
async def get_timetable(date: QueryDate):
    collection = db.trains.aggregate(
        [
            {"$match": {"year": date.year, "month": date.month, "day": date.day}},
            {
                "$project": {
                    "schedule": {
                        "$filter": {
                            "input": "$schedule",
                            "as": "schedule",
                            "cond": {"$eq": ["$$schedule.has_stop", 1]},
                        }
                    },
                    "number": 1,
                    "name": 1,
                    "category": 1,
                    "direction": 1,
                }
            },
            {"$sort": {"schedule.point.position": 1}},
        ]
    )
    my_list = []
    async for element in collection:
        train_info = dict()
        train_number = element["number"]
        if element["name"]:
            train_name = element["name"]
        else:
            train_name = ""
        train_info["train_name"] = f"{train_number} {train_name} rozkładowy"
        train_info["category"] = element["category"]
        train_info["schedule"] = list()
        train_delayed_info = dict()
        train_delayed_info["train_name"] = f"{train_number} {train_name } rzeczywisty"
        train_delayed_info["schedule"] = list()
        train_delayed_info["category"] = element["category"]
        for stop_info in element["schedule"]:
            if stop_info["departure_time"]:
                train_info["schedule"].append(
                    {
                        "x": datetime.datetime.fromtimestamp(
                            stop_info["departure_time"]
                        ),
                        "y": stop_info["point"]["position"],
                    }
                )
                train_delayed_info["schedule"].append(
                    {
                        "x": datetime.datetime.fromtimestamp(
                            stop_info["departure_time"]
                            + stop_info["departure_delay"] * 60
                        ),
                        "y": stop_info["point"]["position"],
                    }
                )
                if element["direction"] == 1:
                    insert_position = len(train_info["schedule"])
                else:
                    insert_position = -1
            else:
                insert_position = len(train_info["schedule"])
            if stop_info["arrival_time"]:
                train_info["schedule"].insert(
                    insert_position,
                    {
                        "x": datetime.datetime.fromtimestamp(stop_info["arrival_time"]),
                        "y": stop_info["point"]["position"],
                    },
                )
                train_delayed_info["schedule"].insert(
                    insert_position,
                    {
                        "x": datetime.datetime.fromtimestamp(
                            stop_info["arrival_time"] + stop_info["arrival_delay"] * 60
                        ),
                        "y": stop_info["point"]["position"],
                    },
                )

        my_list.append(train_info)
        my_list.append(train_delayed_info)
    return my_list


class QueryData(BaseModel):
    day: int
    month: int
    year: int
    direction: int
    category: int
    time_scope: str


@app.post("/line-travel-data/")
async def line_travel_data(data: QueryData):
    if data.category == 0:
        data.category = [1, 2]
    else:
        data.category = [data.category]

    if data.direction == 0:
        data.direction = [1, 2]
    else:
        data.direction = [data.direction]

    data.day = [data.day]
    data.month = [data.month]

    if data.time_scope == "month":
        data.day = [x for x in range(1, 32)]
    if data.time_scope == "year":
        data.day = [x for x in range(1, 32)]
        data.month = [x for x in range(1, 13)]

    collection = db.trains.aggregate(
        [
            {
                "$match": {
                    "year": data.year,
                    "month": {"$in": data.month},
                    "day": {"$in": data.day},
                    "category": {"$in": data.category},
                    "direction": {"$in": data.direction},
                }
            },
            {"$unwind": {"path": "$schedule"}},
            {
                "$project": {
                    "schedule": 1,
                    "is_punctual_on_arrival": {
                        "$cond": [{"$gt": ["$schedule.arrival_delay", 5]}, 0, 1]
                    },
                    "is_punctual_on_departure": {
                        "$cond": [{"$gt": ["$schedule.departure_delay", 5]}, 0, 1]
                    },
                    "is_delay_gained": {
                        "$cond": [
                            {
                                "$gt": [
                                    {
                                        "$subtract": [
                                            "$schedule.departure_delay",
                                            "$schedule.arrival_delay",
                                        ]
                                    },
                                    0,
                                ]
                            },
                            0,
                            1,
                        ]
                    },
                }
            },
            {"$match": {"schedule.has_stop": 1}},
            {
                "$group": {
                    "_id": {
                        "point_name": "$schedule.point.name",
                        "point_position": "$schedule.point.position",
                        "point_type": "$schedule.point.type",
                    },
                    "train_count": {"$sum": 1},
                    "max_departure_delay": {"$max": "$schedule.departure_delay"},
                    "max_arrival_delay": {"$max": "$schedule.arrival_delay"},
                    "avg_departure_delay": {"$avg": "$schedule.departure_delay"},
                    "avg_arrival_delay": {"$avg": "$schedule.arrival_delay"},
                    "avg_schedule_stop_time": {"$avg": "$schedule.stop_time"},
                    "max_delay_gained": {
                        "$max": {
                            "$subtract": [
                                "$schedule.departure_delay",
                                "$schedule.arrival_delay",
                            ]
                        }
                    },
                    "avg_delay_gained": {
                        "$avg": {
                            "$subtract": [
                                "$schedule.departure_delay",
                                "$schedule.arrival_delay",
                            ]
                        }
                    },
                    "percentage_of_punctual_on_arrival": {
                        "$avg": "$is_punctual_on_arrival"
                    },
                    "percentage_of_punctual_on_departure": {
                        "$avg": "$is_punctual_on_departure"
                    },
                    "percentage_of_train_without_gained_delay": {
                        "$avg": "$is_delay_gained"
                    },
                    "avg_real_stop_time": {
                        "$avg": {
                            "$subtract": [
                                {
                                    "$add": [
                                        "$schedule.departure_time",
                                        {
                                            "$multiply": [
                                                "$schedule.departure_delay",
                                                60,
                                            ]
                                        },
                                    ]
                                },
                                {
                                    "$add": [
                                        "$schedule.arrival_time",
                                        {"$multiply": ["$schedule.arrival_delay", 60]},
                                    ]
                                },
                            ]
                        }
                    },
                }
            },
            {"$sort": {"_id.point_position": 1}},
        ]
    )
    my_list = []
    async for element in collection:
        my_list.append(element)
    return my_list


class StationQuery(BaseModel):
    day: int
    month: int
    year: int
    direction: int
    category: int
    station_name: str


@app.post("/station-data/")
async def stat(data: StationQuery):
    if data.category == 0:
        data.category = [1, 2]
    else:
        data.category = [data.category]
    if data.direction == 0:
        data.direction = [1, 2]
    else:
        data.direction = [data.direction]
    collection = db.trains.aggregate(
        [
            {
                "$match": {
                    "year": data.year,
                    "month": data.month,
                    "day": data.day,
                    "category": {"$in": data.category},
                    "direction": {"$in": data.direction},
                }
            },
            {"$unwind": {"path": "$schedule"}},
            {
                "$match": {
                    "schedule.point.name": data.station_name,
                    "schedule.has_stop": 1,
                }
            },
            {
                "$group": {
                    "_id": {
                        "point_name": "$schedule.point.name",
                        "point_position": "$schedule.point.position",
                    },
                    "arrival_delays": {"$push": "$schedule.arrival_delay"},
                    "departure_delays": {"$push": "$schedule.departure_delay"},
                    "stop_times": {"$push": "$schedule.stop_time"},
                    "delay_gained": {
                        "$push": {
                            "$subtract": [
                                "$schedule.departure_delay",
                                "$schedule.arrival_delay",
                            ]
                        }
                    },
                }
            },
        ]
    )
    my_dict = dict()
    my_dict["arrival_delays"] = dict()
    my_dict["departure_delays"] = dict()
    my_dict["stop_times"] = dict()
    async for element in collection:
        arrival_del, arr_del_bins = np.histogram(
            element["arrival_delays"],
            [
                -10,
                -5,
                -3,
                -2,
                -1,
                0,
                1,
                2,
                3,
                4,
                5,
                10,
                15,
                20,
                30,
                40,
                50,
                60,
                80,
                100,
                120,
                300,
                1000,
            ],
        )
        departure_del, dep_del_bins = np.histogram(
            element["departure_delays"],
            [
                -10,
                -5,
                -3,
                -2,
                -1,
                0,
                1,
                2,
                3,
                4,
                5,
                10,
                15,
                20,
                30,
                40,
                50,
                60,
                80,
                100,
                120,
                300,
                1000,
            ],
        )
        stop_times, stop_times_bins = np.histogram(
            element["stop_times"],
            [
                -10,
                -5,
                -3,
                -2,
                -1,
                0,
                1,
                2,
                3,
                4,
                5,
                10,
                15,
                20,
                30,
                40,
                50,
                60,
                80,
                100,
                120,
                300,
                1000,
            ],
        )
        my_dict["arrival_delays"]["data"] = arrival_del.tolist()
        my_dict["arrival_delays"]["bins"] = arr_del_bins.tolist()
        my_dict["departure_delays"]["data"] = departure_del.tolist()
        my_dict["departure_delays"]["bins"] = dep_del_bins.tolist()
        my_dict["stop_times"]["data"] = stop_times.tolist()
        my_dict["stop_times"]["bins"] = stop_times_bins.tolist()
        return my_dict
        # arrival_del, arr_del_bins = np.histogram(element['arrival_delays'],
        #  [-10, -5, -3, -2, -1, 0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 40, 50, 60, 80, 100, 120, 300, 1000])


def train_helper(train):
    return {"train": train["number"]}
