#!/bin/bash

# Function to scan and list directories for TEST_PACKS
generate_test_packs() {
    local search_dir="$1"
    TEST_PACKS=()
    for dir in "${search_dir}"/*_pack; do
        if [ -d "$dir" ]; then
            dir_name=$(basename "$dir")
            TEST_PACKS+=("$dir_name")
        fi
    done
}

# Function to scan and list directories inside the 'data/' directory for TEST_DATASETS
generate_test_datasets() {
    local data_dir="$1"
    TEST_DATASETS=()  # Initialize an empty array
    for dir in "${data_dir}"/*; do
        if [ -d "$dir" ]; then
            dir_name=$(basename "$dir")
            TEST_DATASETS+=("$dir_name")
        fi
    done
}

# Assuming the current directory is the location of this script
# And the root directory of your project is one level up
ROOT_DIR=$(pwd)
DATA_DIR="${ROOT_DIR}/tests/data"

# Call the function to generate the TEST_PACKS array
generate_test_packs "${ROOT_DIR}"

# Call the function to generate the TEST_DATASETS array
generate_test_datasets "${DATA_DIR}"

current_datetime=$(date +"%Y%m%d")

# Iterate over each dataset
for dataset in "${TEST_DATASETS[@]}"; do
    # For each dataset, iterate over each test pack
    for pack in "${TEST_PACKS[@]}"; do
        mkdir -p "${DATA_DIR}/${dataset}/output/${current_datetime}"
        log_file="${DATA_DIR}/${dataset}/output/${current_datetime}/${dataset}_${pack}.log"
        # Redirect the outputs of this iteration to the log file
        {
            echo "Processing Dataset: ${dataset} with Test Pack: ${pack}"

            # 1. Copy source_conf.json from the dataset directory into the current test pack directory
            cp "${DATA_DIR}/${dataset}/source_conf.json" "${ROOT_DIR}/${pack}"

            # 2. Change to the test pack directory
            cd "${ROOT_DIR}/${pack}"

            # 3. Execute the run.sh script
            if [ -x "./run.sh" ]; then
                ./run.sh
            else
                echo "run.sh is not executable or not found in $pack. Skipping."
                continue
            fi

            # 4. Once processing is done, copy the result files back into the dataset directory
            cp metrics.json "${DATA_DIR}/${dataset}/output/${current_datetime}/${pack}_metrics.json"
            cp recommendations.json "${DATA_DIR}/${dataset}/output/${current_datetime}/${pack}_recommendations.json"
            cp schemas.json "${DATA_DIR}/${dataset}/output/${current_datetime}/${pack}_schemas.json"
            cp *.html "${DATA_DIR}/${dataset}/output/${current_datetime}/"
            cp *.xlsx "${DATA_DIR}/${dataset}/output/${current_datetime}/"
            cp *.csv "${DATA_DIR}/${dataset}/output/${current_datetime}/"

            # 6. Cleanup
            # Remove all .json files except pack_conf.json
            find "${ROOT_DIR}/${pack}/" -type f -name "*.json" ! -name "pack_conf.json" -exec rm {} +

            # Remove all .html files
            find "${ROOT_DIR}/${pack}/" -type f -name "*.html" -exec rm {} +

            echo "Completed: ${dataset} with ${pack}"
            echo "Cleanup done for $pack."
        } &> "${log_file}"

        # Return to the root directory for the next iteration
        cd "${ROOT_DIR}"
    done
done

echo "All Tests Completed."
