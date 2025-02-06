####################################################################################
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
####################################################################################


####################################################################################
# Main script used to provision the different asset used in the following demo:
#               Python 
#
# Author: Damien Contreras cdamien@google.com
####################################################################################
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.protobuf import descriptor_pb2
import logging
import json

import simple_json_pb2

# Project and dataset IDs
project_id = "sfsc-srtt-shared"  
dataset_id = "storage_api_demo"  
table_id = "landing_table" 

##### Example - data that we will be sending to BigQuery
message1 = simple_json_pb2.SimpleJson()
message1.json_data = '{"name": "Alice", "age": 30, "city": "New York"}' #your data

message1 = simple_json_pb2.SimpleJson()
message1.json_data = '{"name": "Damien", "age": 46, "city": "Cupertino"}' #your data

message2 = simple_json_pb2.SimpleJson()
message2.json_data = '{"name": "Annie", "age": 17, "city": "San Francisco"}' #your data
##### Example

write_client = bigquery_storage_v1.BigQueryWriteClient()
parent = write_client.table_path(project_id, dataset_id, table_id)
stream_name = f'{parent}/_default'
write_stream = types.WriteStream()

request_template = types.AppendRowsRequest()
request_template.write_stream = stream_name

proto_schema = types.ProtoSchema()
proto_descriptor = descriptor_pb2.DescriptorProto()
simple_json_pb2.SimpleJson.DESCRIPTOR.CopyToProto(proto_descriptor)
proto_schema.proto_descriptor = proto_descriptor
proto_data = types.AppendRowsRequest.ProtoData()
proto_data.writer_schema = proto_schema
request_template.proto_rows = proto_data

append_rows_stream = writer.AppendRowsStream(write_client, request_template)

# prepare msgs to be sent
proto_rows = types.ProtoRows()
proto_rows.serialized_rows.append(message1.SerializeToString())
proto_rows.serialized_rows.append(message2.SerializeToString())
proto_rows.serialized_rows.append(message3.SerializeToString())

request = types.AppendRowsRequest()
proto_data = types.AppendRowsRequest.ProtoData()
proto_data.rows = proto_rows
request.proto_rows = proto_data

#debug only
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("debug.log"),
            logging.StreamHandler()
        ])
try:
    response_future = append_rows_stream._open(request)
    response = response_future.result()  # Get the response from the opening
    print("Stream opened successfully.")
   
    append_rows_stream.send(request)
    print("Data sent successfully.") 
except Exception as e:
    print(f"Error sending data: {e}")