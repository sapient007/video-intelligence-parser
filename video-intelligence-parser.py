#!/usr/bin/env python

# Copyright (c) 2019 Google LLC
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import argparse
import re
import io
import os
import logging
import time
from google.cloud import pubsub_v1


logging.basicConfig()
regexp_left = re.compile(r"left : ([^\s]+)")
regexp_right = re.compile(r"right : ([^\s]+)")
regexp_top = re.compile(r"top : ([^\s]+)")
regexp_bottom = re.compile(r"bottom : ([^\s]+)")

#create publisher
topic_name = "projects/{}/topics/{}".format(os.getenv("GCP_PROJECT"), os.getenv("PUB_SUB_TOPIC"))
publisher = pubsub_v1.PublisherClient()

def video_intelligence_annotate(outputfile):
    with io.open(outputfile, 'r') as video_intelligence_output:
        while True:
            entity = {}
            line = video_intelligence_output.readline()
            if not line:
                time.sleep(5)
            # finds line that is an entity and read the next 5 lines to construct dictionary
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
                publish_topic(entity)

def publish_topic(entity):
    publisher.publish(topic_name, b'detected_objects', **entity) 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'file_path', help='Local file location for video intelligence output.')
    args = parser.parse_args()

    video_intelligence_annotate(args.file_path)