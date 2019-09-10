#!/usr/bin/env python

import argparse
import re
import io
import os
import logging
import datetime
import time
from google.cloud import pubsub_v1


logging.basicConfig()
regexp_left = re.compile(r"left : ([^\s]+)")
regexp_right = re.compile(r"right : ([^\s]+)")
regexp_top = re.compile(r"top : ([^\s]+)")
regexp_bottom = re.compile(r"bottom : ([^\s]+)")

# create publisher
topic_name = "projects/{}/topics/{}".format(os.getenv("GCP_PROJECT"), os.getenv("PUB_SUB_TOPIC"))
publisher = pubsub_v1.PublisherClient()

def video_intelligence_annotate(outputfile):

    # tracking message publish time
    last_zoom_event = None

    with io.open(outputfile, 'r') as video_intelligence_output:
        while True:
            entity = {}
            line = video_intelligence_output.readline()
            if not line:
                time.sleep(5)

            try:
                if "Entity description:" in line:
                    # process the next 5 lines
                    # assumes that each object detected will consume 5 lines for output. will need to udpate if changes
                    entity["entity_desc"] = line.split("Entity description:",1)[1].strip()
                    entity["track_id"] = video_intelligence_output.readline().split("Track Id:", 1)[1].strip()
                    entity["entity_id"] = video_intelligence_output.readline().split("Entity Id:", 1)[1].strip()
                    entity["confidence"] = video_intelligence_output.readline().split("Confidence:", 1)[1].strip()
                    entity["time"] = video_intelligence_output.readline().split("Time:", 1)[1].strip()
                    bounding_box = video_intelligence_output.readline().split("Bounding box position", 1)[1].strip()
                    entity["left"] = regexp_left.search(bounding_box).group(1)
                    entity["right"] = regexp_right.search(bounding_box).group(1)
                    entity["top"] = regexp_top.search(bounding_box).group(1)
                    entity["bottom"] = regexp_bottom.search(bounding_box).group(1)

                    # flag a zoom event in pub/sub
                    if (entity["entity_desc"].lower() == "person" and \
                        float(entity["confidence"]) > 0.60 and \
                        (last_zoom_event is None or last_zoom_event <= datetime.datetime.now() - datetime.timedelta(seconds=10 ))):
                        print("zoom event {}".format(datetime.datetime.now()))
                        entity["zoom"] = "1"
                        last_zoom_event = datetime.datetime.now() 
                    else:
                        entity["zoom"] = "0"
                    publish_topic(entity)
            except Exception as e:
                print("Error while parasing dectection: {}".format(e))

def publish_topic(entity):
    publisher.publish(topic_name, b'detected_objects', **entity) 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'file_path', help='Local file location for video intelligence output.')
    args = parser.parse_args()

    video_intelligence_annotate(args.file_path)
