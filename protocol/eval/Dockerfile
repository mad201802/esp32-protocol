FROM ubuntu:22.04

# Update package list and install time and perf tools
RUN apt-get update && \
    apt-get install -y \
    time \
    linux-tools-6.8.0-60-generic \
    && rm -rf /var/lib/apt/lists/*