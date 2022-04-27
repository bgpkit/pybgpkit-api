import json
from typing import Optional, List

import bgpkit
from arrow import ParserError
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import Response

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


def parse_file(
        url: str,
        prefix: Optional[str] = None,
        asn: Optional[int] = None,
        as_path: Optional[str] = None,
        msg_type: Optional[str] = None,
        limit: Optional[int] = None,
):
    filters = {}
    if prefix:
        filters["prefix"] = prefix
    if asn and asn >= 0:
        filters["origin_asn"] = str(asn)
    if as_path:
        filters["as_path"] = as_path
    if msg_type:
        if msg_type.startswith("w"):
            filters["type"] = "withdraw"
        if msg_type.startswith("a"):
            filters["type"] = "announce"

    parser = bgpkit.Parser(url=url, filters=filters)
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

    return elems


@app.post("/parse", response_model=ParseResult, response_description="Parsed API", )
async def parse_single_file(request: Request,
                            url: str = Query(..., description="URL to the MRT file to parse"),
                            prefix: str = Query(None, description="filter by prefix"),
                            asn: int = Query(None, description="filter by AS number"),
                            as_path: str = Query(None, description="filter by AS path"),
                            limit: int = Query(None, description="limit the number of messages to return"),
                            ):
    elems = parse_file(url, prefix, asn, as_path, limit, None)
    return Response(json.dumps({"data": elems}), media_type="application/json")


def convert_broker_item(item):
    if item.collector_id.startswith("rrc"):
        project = "riperis"
    else:
        project = "routeviews"

    if item.exact_size>0:
        size = item.exact_size
    else:
        size = item.rough_size

    return ListEntry(
        url=item.url,
        project=project,
        collector=item.collector_id,
        data_type=item.data_type,
        size=size,
    )


def query_broker(ts_start, ts_end, project, collector):
    broker = bgpkit.Broker()
    items = broker.query(ts_start=ts_start, ts_end=ts_end, project=project,
                         collector_id=collector, data_type="update")
    return items


@app.post("/files", response_model=ListResult, response_description="List files", )
async def search_files(
        ts_start: str = Query(..., description="start timestamp, in unix time or RFC3339 format"),
        ts_end: str = Query(..., description="end timestamp, in unix time or RFC3339 format"),
        project: str = Query(None, description="filter by project name, i.e. route-views or riperis"),
        collector: str = Query(None, description="filter by collector name, e.g. rrc00 or route-views2")
):
    try:
        items = query_broker(ts_start, ts_end, project, collector)
    except ParserError:
        res = jsonable_encoder(ListResult(count=0, total_size=0, error="invalid timestamp", files=[]))
        return Response(json.dumps(res), media_type="application/json")
    files = [convert_broker_item(i) for i in items]
    res = ListResult(count=len(files), total_size=sum([f.size for f in files]), error=None, files=files)
    return Response(json.dumps(jsonable_encoder(res)), media_type="application/json")


@app.post("/search", response_model=ProcessResult, response_description="Parsed API", )
async def search_messages(
        ts_start: str = Query(..., description="start timestamp, in unix time or RFC3339 format"),
        ts_end: str = Query(..., description="end timestamp, in unix time or RFC3339 format"),
        project: str = Query(None, description="filter by project name, i.e. route-views or riperis"),
        collector: str = Query(None, description="filter by collector name, e.g. rrc00 or route-views2"),

        origin: int = Query(None, description="filter by origin as"),
        prefix: str = Query(None, description="filter by prefix"),
        as_path: str = Query(None, description="filter by AS path regular expression"),
        msg_type: str = Query(None, description="filter by message type, i.e. announcement or withdrawal"),
):
    try:
        items = query_broker(ts_start, ts_end, project, collector)
    except ParserError:
        res = jsonable_encoder(ProcessResult(count=0, error="invalid timestamp", msgs=[]))
        # todo: fix data format
        return Response(json.dumps(res), media_type="application/json")
    files = ListResult(count=len(items), total_size=0, error=None, files=[convert_broker_item(i) for i in items])

    items = items
    elems = []

    # TODO:

    # with WorkerPool(n_jobs=5) as pool:
    #     results = pool.map(parse_file, item.url, prefix, origin, as_path, msg_type)

    for item in items:
        print(f"parsing {item.url}...")
        elems.extend(parse_file(item.url, prefix=prefix, asn=origin, as_path=as_path, msg_type=msg_type))
        print(f"parsing {item.url}...done")

    res = jsonable_encoder(ProcessResult(count=len(elems), error=None, msgs=elems[:1], files=files))

    return Response(json.dumps(res), media_type="application/json")
