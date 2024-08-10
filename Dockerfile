FROM python:3-slim

# set working directory
WORKDIR /orders_analysis

# Download and install DuckDB CLI
RUN apt-get update && apt-get install -y wget unzip
RUN wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip
RUN unzip duckdb_cli-linux-amd64.zip
RUN rm duckdb_cli-linux-amd64.zip
RUN chmod +x ./duckdb

# install depencencies
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project code
COPY . /orders_analysis

# Set the PYTHONPATH so that the 'src' module can be found
ENV PYTHONPATH=/orders_analysis/src

# Run the data processing 
RUN python src/main.py

# Add aliases for major tasks
RUN echo "alias runtests='python -m unittest discover -s tests'" >> ~/.bashrc
RUN echo "alias rundb='./duckdb orders.db'" >> ~/.bashrc
RUN echo "alias runpipeline='python src/main.py'" >> ~/.bashrc

# Source the bashrc to activate the aliases
RUN /bin/bash -c "source ~/.bashrc"

# Run the tests
# RUN python -m unittest discover -s tests

# Set entrypoint
ENTRYPOINT ["/bin/bash"]
