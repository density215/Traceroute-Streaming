#!/usr/bin/python

import os
import time
import re
import logging
import argparse
import ujson as json

from ripe.atlas.cousteau import AtlasStream

from pertdb import Results,store_results
   # thread.start_new_thread(store_result, (args[0],))                  # start a new thread to store result

ap = argparse.ArgumentParser(description='Insert Traceroute Result Stream into PertDB')
ap.add_argument('secs', type=int, help='number of seconds to stream consume')
ap.add_argument('-s', '--server', help='server address')
ap.add_argument("-v", "--verbosity", action="count", help="increase output verbosity")
cmdl = ap.parse_args()


nrecs = 0
lastrec = 0
lastsec = int(time.time())
buf  = []


def be_verbose(level, text):

        global cmdl

        if cmdl.verbosity >= level:
                print time.strftime("%H:%M:%S: ", time.gmtime(time.time())),
                print text

        
def on_result_response(*args):                                          ## Atlas Stream Result Handler

        global  lastsec, lastrec, nrecs, buf


        a = args[0]
        t = int(time.time())

        if t != lastsec:
                be_verbose(2, "%d results ..." % (nrecs-lastrec))
                lastrec = nrecs
                lastsec = t


        a['pdb_received'] = t
        a['pdb_source'] = 1
        be_verbose(3,json.dumps(a))
        buf.append(a)
        nrecs +=1

atlas_stream = AtlasStream()

if cmdl.server:
        atlas_stream.iosocket_server = cmdl.server                      # override stream server address

atlas_stream.connect()  

channel = "result"
atlas_stream.bind_channel(channel, on_result_response)                  # establish callback

stream_parameters = {"type": "traceroute"}
atlas_stream.start_stream(stream_type="result", **stream_parameters)    # start streaming

be_verbose(1, "stream starting ...")
atlas_stream.timeout(seconds=cmdl.secs)                                 # this really starts it ....
