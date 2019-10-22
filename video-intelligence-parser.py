#!/usr/bin/env python

import argparse
import re
import io
import os
import logging
import datetime
import time
from sets import Set as set 
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
    last_zoom_entity_desc = set()
    detection_object = set(os.getenv("DETECTION_OBJS").split(","))
    print("detection object(s) for:  {}".format(detection_object))
    detection_confidence = 0.50
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
                    entity["device_id"] = "esp32_BCD168"
                    entity["sensor_name"] = "AXIS M1065-LW"
                    entity["stream_time"] = str(os.getenv("DATE_TIME"))

                    # validate the type of objects of interest
                    # nothing happens assume default values
                    # if detection_object != os.getenv("DETECT_OBJ_NAME", detection_object):
                    #     detection_object = os.getenv("DETECT_OBJ_NAME")
                    #     print("detection object name is {}".format(detection_object))
                    if detection_confidence != float(os.getenv("DETECT_OBJ_CONFIDENCE", detection_confidence)):
                        detection_confidence = float(os.getenv("DETECT_OBJ_CONFIDENCE"))
                        print("detection object confidence is {}".format(detection_confidence))

                    # reset zoom entity every 6 mins
                    if (last_zoom_event is not None):
                        if (last_zoom_event <= datetime.datetime.now() - datetime.timedelta(minutes=6)):
                            last_zoom_entity_desc.clear()

                    # flag a zoom event in pub/sub
                    if entity["entity_desc"].lower() in detection_object and \
                        float(entity["confidence"]) > detection_confidence and \
                        entity["entity_desc"] not in last_zoom_entity_desc and \
                        (last_zoom_event is None or last_zoom_event <= datetime.datetime.now() - datetime.timedelta(seconds=30 )):
                        print("zoom event of {} for trackID {} occured at {} with confidence {} with system time of {}".format(entity["entity_desc"], entity["track_id"], entity["time"], entity["confidence"], datetime.datetime.now()))
                        entity["zoom"] = "1"
                        last_zoom_event = datetime.datetime.now()
                        last_zoom_entity_desc.add(entity["entity_desc"])
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
