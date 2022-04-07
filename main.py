import json
from typing import Optional, List

from dotenv import load_dotenv
from fastapi import FastAPI
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


class Entry(BaseModel):
    tal: str
    prefix: str
    max_len: int
    asn: int
    date_ranges: List[List[str]]


class Result(BaseModel):
    count: int
    error: Optional[str]
    data: List[Entry]


@app.get("/parse", response_model=Result, response_description="Parsed API", )
async def parse(request: Request,
                url: str,
                prefix: str = "",
                asn: int = -1,
                as_path: str = "",
                limit: int = None,
                debug: bool = False,
                ):
    filters = {}
    if prefix:
        filters["prefix"] = prefix
    if asn >= 0:
        filters["origin_asn"] = str(asn)
    if as_path:
        filters["as_path"] = as_path

    parser = Parser(url=url, filters=filters)
    elems = parser.parse_all()
    if limit and limit > 0:
        elems = elems[:limit]
    if debug:
        return Response("{'result': 'done'}", media_type="application/json")

    return Response(json.dumps({"data": elems}), media_type="application/json")
