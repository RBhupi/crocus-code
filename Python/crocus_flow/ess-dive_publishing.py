import requests
import json
import os
import argparse

def upload_to_archive(token, base_url, endpoint, json_ld, upload_directory):
    # Set up the authorization header
    header_authorization = "bearer {}".format(token)
    
    # Prepare the files to be uploaded
    files_tuples_array = []
    files_tuples_array.append(("json-ld", json.dumps(json_ld)))
    
    # Traverse the directory and its subdirectories to add all files to the upload
    for root, dirs, files in os.walk(upload_directory):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            files_tuples_array.append(("data", open(file_path, 'rb')))
    
    # Construct the full API URL
    post_packages_url = "{}{}".format(base_url, endpoint)
    
    # Make the POST request to upload the data
    post_package_response = requests.post(post_packages_url,
                                          headers={"Authorization":header_authorization},
                                          files=files_tuples_array)
    
    # Check the response status code
    if post_package_response.status_code == 201:
        # Success
        response = post_package_response.json()
        print(f"View URL: {response['viewUrl']}")
        print(f"Name: {response['dataset']['name']}")
    else:
        # There was an error
        print(post_package_response.text)

def main():
    parser = argparse.ArgumentParser(description="Upload data and metadata to the archive.")
    parser.add_argument('--token', required=True, help="API token for authorization")
    parser.add_argument('--base_url', help="Base URL of the API", default="https://api-sandbox.ess-dive.lbl.gov/")
    parser.add_argument('--json_metadata', required=True, help="Path to the JSON metadata file")
    parser.add_argument('--upload_directory', required=True, help="Directory containing files to upload")

    args = parser.parse_args()

    # Read the JSON metadata file
    with open(args.json_metadata, 'r') as json_file:
        json_content = json_file.read()
        json_ld = json.loads(json_content)

    # Call the function with the provided parameters
    upload_to_archive(args.token, args.base_url, "packages", json_ld, args.upload_directory)

if __name__ == "__main__":
    main()