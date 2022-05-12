import datetime
import json
import multiprocessing
import logging
from typing import Optional, List

import bgpkit
import requests
from arrow import ParserError
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.encoders import jsonable_encoder
from mpire import WorkerPool
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import Response

load_dotenv()
logging.basicConfig(
    # filename='HISTORYlistener.log',
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

description = """
BGPKIT Parsing API provides BGP MRT data searching and parsing functionalities via RESTful API.
"""


app = FastAPI(
    title="MRT Data Parsing API",
    description=description,
    version="0.1.0",
    # contact={
    #     "name": "Contact",
    #     "url": "https://bgpkit.com",
    #     "email": "data@bgpkit.com"
    # },
    docs_url='/docs',
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class MrtFile(BaseModel):
    url: str
    project: str
    collector: str
    data_type: str
    size: int


class FileSearchResult(BaseModel):
    count: int
    total_size: int
    error: Optional[str]
    files: List[MrtFile]


class BgpMessage(BaseModel):
    timestamp: float
    timestamp_str: str
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
    msgs: List[BgpMessage]
    files: FileSearchResult


def parse_file(
        url: str,
        prefix: Optional[str] = None,
        include_super: bool = False,
        include_sub: bool = False,
        asn: Optional[int] = None,
        as_path: Optional[str] = None,
        msg_type: Optional[str] = None,
        peer_ip: Optional[str] = None,
        peer_asn: Optional[str] = None,
        limit: Optional[int] = None,
):
    logging.info(f"parsing {url} now... {peer_asn}, {peer_ip}")
    filters = {}
    if prefix:
        if not include_super and not include_sub:
            filters["prefix"] = prefix
        elif include_super and include_sub:
            filters["prefix_super_sub"] = prefix
        elif include_super:
            filters["prefix_super"] = prefix
        elif include_sub:
            filters["prefix_sub"] = prefix

    if asn and asn >= 0:
        filters["origin_asn"] = str(asn)
    if as_path:
        filters["as_path"] = as_path
    if msg_type:
        if msg_type.startswith("w"):
            filters["type"] = "withdraw"
        if msg_type.startswith("a"):
            filters["type"] = "announce"
    if peer_ip:
        filters["peer_ip"] = peer_ip
    if peer_asn:
        filters["peer_asn"] = peer_asn

    parser = bgpkit.Parser(url=url, filters=filters)
    count = 0
    elems = []
    while True:
        msg = parser.parse_next()
        if not msg:
            break
        count += 1
        msg["timestamp_str"] = datetime.datetime.fromtimestamp(int(msg["timestamp"]), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        elems.append(msg)
        if limit and count >= limit:
            break
    logging.info(f"parsing {url} now... done")
    return elems


def get_file_size(url: str) -> int:
    response = requests.head(url)
    if 'content-length' not in response.headers:
        return 0
    try:
        size = int(response.headers['content-length'])
        return size
    except ValueError:
        return 0


def convert_broker_item(item) -> MrtFile:
    if item.collector_id.startswith("rrc"):
        project = "riperis"
    else:
        project = "routeviews"

    if item.exact_size > 0:
        size = item.exact_size
    else:
        size = item.rough_size

    return MrtFile(
        url=item.url,
        project=project,
        collector=item.collector_id,
        data_type=item.data_type,
        size=size,
    )


def query_files(ts_start, ts_end, project, collector) -> List[MrtFile]:
    broker = bgpkit.Broker(page_size=10000)
    items = broker.query(ts_start=ts_start, ts_end=ts_end, project=project,
                         collector_id=collector, data_type="update", print_url=True)
    files = [convert_broker_item(i) for i in items]
    return files


@app.get("/parse", response_model=ParseResult, response_description="Parsed API", )
async def parse_single_file(
        url: str = Query(..., description="URL to the MRT file to parse"),
        prefix: str = Query(None, description="filter by prefix"),
        include_super: bool = Query(False, description="whether to include super prefix"),
        include_sub: bool = Query(False, description="whether to include sub prefix"),
        asn: int = Query(None, description="filter by AS number"),
        as_path: str = Query(None, description="filter by AS path"),
        peer_ip: str = Query(None, description="filter by collector peer IP address"),
        peer_asn: str = Query(None, description="filter by collector peer IP ASN"),
        msg_type: str = Query(None, description="message type, announcement or withdrawal"),
        limit: int = Query(None, description="limit the number of messages to return"),
):
    """
    The `/parse` endpoint provides parsing functionality for one single MRT file.
    """
    elems = parse_file(
        url=url,
        prefix=prefix,
        include_sub=include_sub,
        include_super=include_super,
        asn=asn,
        as_path=as_path,
        msg_type=msg_type,
        peer_ip=peer_ip,
        peer_asn=peer_asn,
        limit=limit
    )
    if "rrc" in url:
        project = "riperis"
        collector = url.split("/")[3]
    elif "route-views" in url:
        project = "routeviews"
        collector = url.split("/")[3]
    else:
        project = "unknown"
        collector = "unknown"
    list_res = FileSearchResult(count=1, total_size=0, error=None, files=[MrtFile(url=url, project=project, collector=collector, data_type="update", size=get_file_size(url))])
    res = jsonable_encoder(ParseResult(count=len(elems), error=None, msgs=elems, files=list_res))
    return Response(json.dumps(res), media_type="application/json")


@app.get("/files", response_model=FileSearchResult, response_description="List files", )
async def search_files(
        ts_start: str = Query(..., description="start timestamp, in unix time or RFC3339 format"),
        ts_end: str = Query(..., description="end timestamp, in unix time or RFC3339 format"),
        project: str = Query(None, description="filter by project name, i.e. route-views or riperis"),
        collector: str = Query(None, description="filter by collector name, e.g. rrc00 or route-views2")
):
    """
    The `/files` endpoint provides searching for MRT files from RIPE RIS and RouteViews collectors.
    """
    try:
        files = query_files(ts_start, ts_end, project, collector)
    except ParserError:
        res = jsonable_encoder(FileSearchResult(count=0, total_size=0, error="invalid timestamp", files=[]))
        return Response(json.dumps(res), media_type="application/json")
    res = FileSearchResult(count=len(files), total_size=sum([f.size for f in files]), error=None, files=files)
    return Response(json.dumps(jsonable_encoder(res)), media_type="application/json")


@app.get("/search", response_model=ParseResult, response_description="Parsed API", )
async def search_messages(
        ts_start: str = Query(..., description="start timestamp, in unix time or RFC3339 format"),
        ts_end: str = Query(..., description="end timestamp, in unix time or RFC3339 format"),
        project: str = Query(None, description="filter by project name, i.e. route-views or riperis"),
        collector: str = Query(None, description="filter by collector name, e.g. rrc00 or route-views2"),
        origin: int = Query(None, description="filter by origin as"),
        peer_ip: str = Query(None, description="filter by collector peer IP address"),
        peer_asn: str = Query(None, description="filter by collector peer IP ASN"),
        prefix: str = Query(None, description="filter by prefix"),
        include_super: bool = Query(False, description="include super prefix"),
        include_sub: bool = Query(False, description="include sub prefix"),
        as_path: str = Query(None, description="filter by AS path regular expression"),
        msg_type: str = Query(None, description="filter by message type, i.e. announcement or withdrawal"),
        msgs_limit: int = Query(100, description="limit the number of BGP messages returned for an API call"),
        files_limit: int = Query(None, description="limit the number of that will be used for parsing"),
        dry_run: bool = Query(False, description="whether to skip parsing"),
):
    """
    The `/search` provides the combination of searching for MRT data files as well as parallel parsing of the data files
    and filtering based on provided parameters. The parsing phase spins up at most N independent processes where N is
    the number of the logical CPU cores, and each process handles the parsing of one single MRT file. At the end of
    the process, all parsed and filtered messages will be combined back together and returned as a single of list BGP
    messages in JSON format.
    """
    try:
        files = query_files(ts_start, ts_end, project, collector)
    except ParserError:
        res = jsonable_encoder(ParseResult(count=0, error="invalid timestamp", msgs=[]))
        return Response(json.dumps(res), media_type="application/json")

    if files_limit and files_limit > 0:
        files = files[:files_limit]

    list_res = FileSearchResult(count=len(files), total_size=sum([f.size for f in files]), error=None, files=files)
    logging.info(f"total of {len(files)} files to parse with total size of {list_res.total_size}")

    elems = []
    if not dry_run:
        with WorkerPool(n_jobs=multiprocessing.cpu_count()) as pool:
            params = [(f.url, prefix, include_super, include_sub, origin, as_path, msg_type, peer_ip, peer_asn, None)
                      for f in files]
            results = pool.map(parse_file, params)
            for res in results:
                elems.extend(res)

            if msgs_limit > 0:
                elems = elems[:msgs_limit]
        logging.info(f"total msgs count: {len(elems)}")

    res = jsonable_encoder(ParseResult(count=len(elems), error=None, msgs=elems, files=list_res))
    return Response(json.dumps(res), media_type="application/json")
