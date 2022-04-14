import json
from typing import Optional, List

from dotenv import load_dotenv
from fastapi import FastAPI, Query
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import Response
from bgpkit import Parser

load_dotenv()

description = """

*MRT Data Parsing API*

"""

app = FastAPI(
    title="MRT Data Parsing API",
    description=description,
    version="0.1.0",
    contact={
        "name": "Contact",
        "url": "https://bgpkit.com",
        "email": "data@bgpkit.com"
    },
    docs_url='/docs',
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ListEntry(BaseModel):
    url: str
    project: str
    collector: str
    data_type: str
    size: int


class ListResult(BaseModel):
    count: int
    total_size: int
    error: Optional[str]
    files: List[ListEntry]


class Entry(BaseModel):
    timestamp: float
    elem_type: str
    peer_ip: str
    peer_asn: int
    prefix: str
    next_hop: Optional[str]
    as_path: Optional[str]
    origin_asns: Optional[List[str]]
    origin: Optional[str]
    local_pref: Optional[int]
    med: Optional[int]
    communities: Optional[List[str]]
    atomic: Optional[str]
    aggr_asn: Optional[Optional[int]]
    aggr_ip: Optional[Optional[str]]


class ParseResult(BaseModel):
    count: int
    error: Optional[str]
    msgs: List[Entry]


class ProcessResult(BaseModel):
    count: int
    error: Optional[str]
    msgs: List[Entry]
    files: ListResult


@app.post("/parse", response_model=ParseResult, response_description="Parsed API", )
async def parse_single_file(request: Request,
                            url: str = Query(..., description="URL to the MRT file to parse"),
                            prefix: str = Query(None, description="filter by prefix"),
                            asn: int = Query(None, description="filter by AS number"),
                            as_path: str = Query(None, description="filter by AS path"),
                            limit: int = Query(None, description="limit the number of messages to return"),
                            ):
    filters = {}
    if prefix:
        filters["prefix"] = prefix
    if asn and asn >= 0:
        filters["origin_asn"] = str(asn)
    if as_path:
        filters["as_path"] = as_path

    parser = Parser(url=url, filters=filters)

    count = 0
    elems = []
    while True:
        msg = parser.parse_next()
        if not msg:
            break
        count += 1

        elems.append(msg)
        if limit and count >= limit:
            break

    return Response(json.dumps({"data": elems}), media_type="application/json")


@app.post("/files", response_model=ListResult, response_description="List files", )
async def search_files(
        ts_start: str = Query(..., description="start timestamp, in unix time or RFC3339 format"),
        ts_end: str = Query(..., description="end timestamp, in unix time or RFC3339 format"),
        project: str = Query(None, description="filter by project name, i.e. route-views or riperis"),
        collector: str = Query(None, description="filter by collector name, e.g. rrc00 or route-views2")
):
    pass


@app.post("/search", response_model=ProcessResult, response_description="Parsed API", )
async def search_messages(
        ts_start: str = Query(..., description="start timestamp, in unix time or RFC3339 format"),
        ts_end: str = Query(..., description="end timestamp, in unix time or RFC3339 format"),
        project: str = Query(None, description="filter by project name, i.e. route-views or riperis"),
        collector: str = Query(None, description="filter by collector name, e.g. rrc00 or route-views2"),

        origin: str = Query(None, description="filter by origin as"),
        prefix: str = Query(None, description="filter by prefix"),
        as_path: str = Query(None, description="filter by AS path regular expression"),
        msg_type: str = Query(None, description="filter by message type, i.e. announcement or withdrawal"),
):
    pass
