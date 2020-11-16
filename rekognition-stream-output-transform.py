from __future__ import print_function

import base64
import json
from datetime import datetime

print('Loading function')


def lambda_handler(event, context):
    output = []

    for record in event['records']:
        print(record['recordId'])
        payload = base64.b64decode(record['data'])

        # Do custom processing on the payload here
        
        payload_dict = json.loads(payload)
        parsed_payload = dict()

        for key, value in payload_dict["InputInformation"]["KinesisVideo"].items():
            parsed_payload[key] = value

        parsed_payload["Status"] = payload_dict["StreamProcessorInformation"]["Status"]
        
        parsed_payload["CorrectTimestamp"] = datetime.fromtimestamp(parsed_payload["ProducerTimestamp"] + parsed_payload["FrameOffsetInSeconds"]).strftime('%Y-%m-%d %H:%M:%S.%f')

        parsed_payload["MatchedFaces"] = []
        for i in payload_dict["FaceSearchResponse"]:
            for j in i["MatchedFaces"]:
                parsed_payload["MatchedFaces"].append({"FaceId": j["Face"]["FaceId"],"Confidence": j["Face"]["Confidence"],"ImageId": j["Face"]["ImageId"],
                "ExternalImageId": j["Face"]["ExternalImageId"]
        })

        enc_parsed_payload = json.dumps(parsed_payload, indent=2).encode('utf-8')
        
        # Do custom processing on the payload here

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(enc_parsed_payload)
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}
