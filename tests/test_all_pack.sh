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
DATA_DIR="${ROOT_DIR}/data"

# Call the function to generate the TEST_PACKS array
generate_test_packs "${ROOT_DIR}"

# Call the function to generate the TEST_DATASETS array
generate_test_datasets "${DATA_DIR}"

# Iterate over each dataset
for dataset in "${TEST_DATASETS[@]}"; do
    # For each dataset, iterate over each test pack
    for pack in "${TEST_PACKS[@]}"; do
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
        cp metrics.json "${DATA_DIR}/${dataset}/${pack}_metrics.json"
        cp recommendations.json "${DATA_DIR}/${dataset}/${pack}_recommendations.json"
        cp schemas.json "${DATA_DIR}/${dataset}/${pack}_schemas.json"

        # 5. Print a completion message (optional)
        echo "Completed: ${dataset} with ${pack}"

        # 6. Cleanup
        rm "${ROOT_DIR}/${pack}/source_conf.json" "${ROOT_DIR}/${pack}/metrics.json" "${ROOT_DIR}/${pack}/recommendations.json" "${ROOT_DIR}/${pack}/schemas.json" || echo "Error removing source_conf.json from $pack"

        # Return to the root directory for the next iteration
        cd "${ROOT_DIR}"
    done
done

echo "All Tests Completed."
