#!/bin/bash
sudo apt update
sudo apt install -y python3-pip
pip3 install streamlit requests plotly
streamlit run app.py --server.port 8501 --server.address 0.0.0.0