import re
import os
import time
import sys
import ujson as json
import logging
from datetime import datetime

from peewee import *

re_duperr = re.compile("^Duplicate entry .* for key ")

################   Open DB ##########################

IncomingDB = MySQLDatabase('incoming', host='127.0.0.1', port=47536, user='pertdb', password='YatK7nCPVEjifGkxoyh62U6qOOEA7sqr')
IncomingDB.connect()

AdjcDB = MySQLDatabase('adjc', host='127.0.0.1', port=47536, user='pertdb', password='YatK7nCPVEjifGkxoyh62U6qOOEA7sqr')
AdjcDB.connect()

#####AdjcDB.set_autocommit(False)


################   Misc Helpers  ##########################


def mk_typeid(type, af):
        if (type == 'ping'):
                if (af==4):
                        return(1)
                else:
                        return(3)
        elif (type == 'traceroute'):
                if (af==4):
                        return(2)
                else:
                        return(4)
        elif (type == 'dns'):
                if (af==4):
                        return(6)
                else:
                        return(7)
        elif (type == 'http'):
                if (af==4):
                        return(8)
                else:
                        return(9)
        elif (type == 'sslcert'):
                if (af==4):
                        return(11)
                else:
                        return(12)
        elif (type == 'ntp'):
                if (af==4):
                        return(13)
                else:
                        return(14)
        else:
                logging.error("Unknown measurement type string: %s, af=%d", type, af)
                return(0)
                

################   Measurements  ##########################

class Msmts(Model):
        msmid = IntegerField()
        retrieved = DateTimeField()
        created = DateTimeField()
        status = IntegerField()
        start = DateTimeField()
        stop = DateTimeField()
        interval = IntegerField()
        typeid = IntegerField()
        af = IntegerField()
        isoneoff = BooleanField()
        blob = TextField()

        class Meta:
            database = IncomingDB

if not Msmts.table_exists():
        IncomingDB.create_tables([Msmts])
        IncomingDB.create_index(Msmts, ('msmid',), unique=True)
        IncomingDB.create_index(Msmts, ('retrieved',), unique=False)
        IncomingDB.create_index(Msmts, ('created',), unique=False)
        IncomingDB.create_index(Msmts, ('status',), unique=False)
        IncomingDB.create_index(Msmts, ('start',), unique=False)
        IncomingDB.create_index(Msmts, ('stop',), unique=False)
        IncomingDB.create_index(Msmts, ('typeid',), unique=False)


def canon_msmt(m):
        if 'probes' in m:
                del(m['probes'])
        if 'type' in m:
                if 'id' in m['type']:
                        m['typeid'] = m['type']['id']
                else:
                        m['typeid'] = mk_typeid(m['type'], m['af'])

        if (m['stop_time'] == None):
                m['stop_time'] = 0

        if 'msm_id' not in m:
                m['msm_id'] = m['id']

        if m['interval'] == None:
                m['interval'] = 0

        m['pertdb_canon'] = 1
        return(m)
        
def store_msmt(m):
        if not 'pertdb_canon' in m:
                m = canon_msmt(m)

        if (m['stop_time'] == 0):
                stop = "2100-01-01 00:00:00"
        else:   
                stop = datetime.utcfromtimestamp(m['stop_time'])

        r = Msmts(
                retrieved =     datetime.utcfromtimestamp(time.time()),
                msmid =         m['msm_id'],
                created =       datetime.utcfromtimestamp(m['creation_time']), 
                status =        m['status']['id'], 
                start =         datetime.utcfromtimestamp(m['start_time']), 
                stop =          stop,
                interval =      m['interval'],
                typeid =        m['typeid'],
                af =            m['af'], 
                isoneoff =      m['is_oneoff'],
                blob =          json.dumps(m)
        )
        r.save()
        
#       try:
#               r.save()
#       except IntegrityError:
#               logging.error(sys.exc_value[2])
#       finally:
#               return


def list_msmts(m):
        q = (Msmts
                .select()
                .where(Msmts.msmid << m)
        )

        found =[]
        for d in q:
                print "%9d %d %d %s - %s %d %6d" % (d.msmid, d.status, d.isoneoff, d.start, d.stop, d.typeid, d.interval)
                found.append(d.msmid)
        notfound = difflist(m, found)
        for d in notfound:
                print "%9d not found" % d


def list_msmt(d):
        print "%9d %d %d %s - %s %d %6d" % (d.msmid, d.status, d.isoneoff, d.start, d.stop, d.typeid, d.interval)


def dump_msmt(msmt):
        skip = ['probes', 'result']
                
        for k in msmt.keys():
                print "%22.22s" % k,
                if k in skip:
                        print
                else:
                        print msmt[k]
        print


################   Results  ##########################

class Results(Model):
        received = IntegerField()
        timestamp = IntegerField()
        prbid = IntegerField()
        msmid = IntegerField()
        source = IntegerField()
        pid = IntegerField()
        result = BlobField()

        class Meta:
                database = IncomingDB

if not Results.table_exists():
        IncomingDB.create_tables([Results])
        IncomingDB.create_index(Results, ('timestamp', 'prbid', 'msmid'), unique=True)
        IncomingDB.create_index(Results, ('received',))


def store_result(r):
        res = Results(
                received =      r['pdb_received'],
                timestamp =     r['timestamp'], 
                prbid =         r['prb_id'], 
                msmid =         r['msm_id'], 
                source =        r['pdb_source'],
                pid =           os.getpid(),
                result =        json.dumps(r),
        )
 

        try:
                res.save()
        except IntegrityError:
                if re_duperr.match(sys.exc_value[2]) == None:
                        logging.error(sys.exc_value[2])
        finally:
                return


def store_results(results):

        res = []
        nres = 0
        pid = os.getpid()

        for r in results:
                res.append( 
                        {
                                'received' :    r['pdb_received'],
                                'timestamp' :   r['timestamp'], 
                                'prbid' :       r['prb_id'], 
                                'msmid' :       r['msm_id'], 
                                'source' :      r['pdb_source'],
                                'pid':          pid,
                                'result' :      json.dumps(r),
                        }
                )

                nres +=1
                if nres >100:

                        try:
                                Results.insert_many(res).execute()
                        except IntegrityError:
                                if re_duperr.match(sys.exc_value[1]) == None:
                                        print sys.exc_value[1]

                        nres = 0
                        res =[]

        if nres >0:
                try:
                        Results.insert_many(res).execute()
                except IntegrityError:
                        if re_duperr.match(sys.exc_value[1]) == None:
                                print sys.exc_value[1]

