FROM python:3-slim

WORKDIR /orders_analysis

# Download and install DuckDB CLI
RUN apt-get update && apt-get install -y wget unzip
RUN wget https://artifacts.duckdb.org/latest/duckdb-binaries-linux.zip
RUN unzip duckdb-binaries-linux.zip
RUN unzip duckdb_cli-linux-amd64.zip  
RUN rm duckdb-binaries-linux.zip duckdb_cli-linux-amd64.zip libduckdb-linux-amd64.zip 
RUN chmod +x ./duckdb

# install depencencies
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /orders_analysis

# Set the PYTHONPATH so that the 'src' module can be found by tests
ENV PYTHONPATH=/orders_analysis/src

# Add aliases for major tasks
RUN echo "alias runtests='python -m unittest discover -s tests'" >> ~/.bashrc
RUN echo "alias runduckdb='./duckdb orders.db'" >> ~/.bashrc
RUN echo "alias runpipeline='python src/main.py'" >> ~/.bashrc
RUN echo "alias runflaskapp='python src/api.py'" >> ~/.bashrc

# Source the bashrc to activate the aliases
RUN /bin/bash -c "source ~/.bashrc"

EXPOSE 5000

#  Set entrypoint to first run main.py then api.py
ENTRYPOINT ["bash", "-c", "python ./src/main.py && python ./src/api.py"]
