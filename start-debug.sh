#!/bin/bash

python3 -m venv venv
pip3 install --upgrade pip
pip3 install -r requirements.txt

python3 -m uvicorn main:app --host 0.0.0.0 --reload
