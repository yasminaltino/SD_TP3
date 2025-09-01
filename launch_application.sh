#!/bin/bash
python3 syncs/application/main/mqtt_connection/application_gateway.py 5000 &
python3 syncs/application/main/mqtt_connection/application_gateway.py 5001 &
python3 syncs/application/main/mqtt_connection/application_gateway.py 5002 &
python3 syncs/application/main/mqtt_connection/application_gateway.py 5003 &
python3 syncs/application/main/mqtt_connection/application_gateway.py 5004 &


wait