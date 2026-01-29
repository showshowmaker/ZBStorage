FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    build-essential cmake git \
    libssl-dev libprotobuf-dev protobuf-compiler \
    libgflags-dev libleveldb-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN cmake -S . -B build && cmake --build build -j

EXPOSE 8000
CMD ["/app/build/real_node_server", "--config=/etc/real_node/real_node.conf", "--port=8000"]
