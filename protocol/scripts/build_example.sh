#!/bin/bash

# Build rust examples inside a docker container
# Usage: ./build_example.sh [example1] [example2] ...
# If no arguments provided, builds all examples

set -e

# Function to get all examples from the examples directory
get_all_examples() {
    local examples=()
    if [ -d "examples" ]; then
        for file in examples/*.rs; do
            if [ -f "$file" ]; then
                # Extract filename without path and extension
                local example_name=$(basename "$file" .rs)
                examples+=("$example_name")
            fi
        done
    fi
    echo "${examples[@]}"
}

# Function to build all examples
build_all_examples() {
    local all_examples=($(get_all_examples))
    
    if [ ${#all_examples[@]} -eq 0 ]; then
        echo "No examples found in examples/ directory"
        exit 1
    fi
    
    echo "Building all examples: ${all_examples[*]}"
    
    # Build cargo command with all examples in a single container
    local cargo_cmd=""
    for example in "${all_examples[@]}"; do
        if [ -n "$cargo_cmd" ]; then
            cargo_cmd="$cargo_cmd && "
        fi
        cargo_cmd="${cargo_cmd}cargo build --example $example"
    done
    
    docker run --rm --user 1000 -v "$PWD":/usr/src/protocol -w /usr/src/protocol rust:1.86.0-slim sh -c "$cargo_cmd"
}

# Function to build specific examples
build_specific_examples() {
    local examples=("$@")
    echo "Building specific examples: ${examples[*]}"
    
    # Build cargo command with all specified examples in a single container
    local cargo_cmd=""
    for example in "${examples[@]}"; do
        if [ -n "$cargo_cmd" ]; then
            cargo_cmd="$cargo_cmd && "
        fi
        cargo_cmd="${cargo_cmd}cargo build --example $example"
    done
    
    # Run all build commands in a single Docker container
    docker run --rm --user 1000 -v "$PWD":/usr/src/protocol -w /usr/src/protocol rust:1.86.0-slim sh -c "$cargo_cmd"
}

# Main logic
if [ $# -eq 0 ]; then
    build_all_examples
else
    build_specific_examples "$@"
fi

echo "Build completed successfully!"