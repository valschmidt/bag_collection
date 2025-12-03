# A python module for reading data from a collection of ROS1 bag files.
#
# Val Schmidt
# Center for Coastal and Ocean Mapping
# University of New Hampshire
# Copyright 2025

import os
import rosbag
import re
import numpy as np
import datetime
import pickle
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

def _process_bag_info(baginfo):
    """
    Worker function for multiprocessing: open a bag file and extract metadata.
    Returns a tuple (path, result_dict) where result_dict contains keys:
      - start_time (float) or None
      - end_time (float) or None
      - topics (list) or []
      - msg_types (list) or []
      - error (None or str)
    """
    path = baginfo.get('path') if isinstance(baginfo, dict) else baginfo
    try:
        b = rosbag.Bag(path)
        start = b.get_start_time()
        end = b.get_end_time()
        tt = b.get_type_and_topic_info()
        topics = list(tt.topics.keys())
        msg_types = list(tt.msg_types.keys())
        b.close()
        return (path, {
            'start_time': start,
            'end_time': end,
            'topics': topics,
            'msg_types': msg_types,
            'error': None
        })
    except Exception as e:
        return (path, {'error': str(e)})


class bag_collection():
    '''A class to manage a collection of bag files.'''
    def __init__(self,directory):

        self.directory = directory
        self.bagfiles = []
        self.file_index = None
        self.topics = set()
        self.msg_types = set()
        self.msg_defs = dict()
        
    def find_bag_files(self,path, extension='.bag'):
        """Recursively finds all files with the given extension in the specified directory."""
        
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(extension):
                    info = {'path': os.path.join(root, file)}
                    info.update({'size': os.path.getsize(info['path'])})
                    info.update({'mtime': os.path.getmtime(info['path'])})
                    self.bagfiles.append(info)
                    # yield os.path.join(root, file)

    def print_bag_files(self):
        '''Print a list of the bag files in the collection.'''
        if self.bagfiles is None:
            self.find_bag_files(path.self.directory)

        for f in self.bagfiles:
            print("\t%s" % f['path'])


    def index_collection(self, force_reindex=False,pickle_file='bag_collection.pkl'):
        '''Create an index of the collection
        
        The index consists of a list of baginfo dictionaries, one per bag file,
        as well as a set of all topics and message types in the collection.

        Each baginfo dictionary contains:
            'path' : full path to bag file
            'size' : size of bag file in bytes
            'mtime': modification time of bag file (UNIX timestamp)
            'start_time': start time of bag file (UNIX timestamp)
            'end_time': end time of bag file (UNIX timestamp)
            'topics': list of topics in bag file
            'msg_types': list of message types in bag file
            'msg_defs': dictionary of message definitions for message types in 
            the collection

        '''
        if not force_reindex and os.path.exists(pickle_file):
            print(f"Loading index from {pickle_file}...")
            with open(pickle_file, 'rb') as f:
                data = pickle.load(f)
                if data['collection_directory'] != self.directory:
                    print("Error: pickle file was created for a different directory.")
                    return
                # self.bagfiles = data['bagfiles']
                prev_bagfiles = data['bagfiles']
                prev_paths = [bf['path'] for bf in prev_bagfiles]
                self.topics = data['topics']
                self.msg_types = data['msg_types']
                self.msg_defs = data['msg_defs']
            return

        print("Indexing collection...")
        self.msg_types = set()
        self.topics = set()
        prev_paths = []

        if len(self.bagfiles) == 0:
            self.find_bag_files(path=self.directory)

        # Decide which files need indexing (skip unchanged ones if we had a previous index)
        bagfiles_to_process = []
        total_bytes_to_process = 0

        for baginfo in self.bagfiles:
            if (baginfo['path'] in prev_paths and 
                baginfo['size'] == prev_bagfiles['size']):
                    continue
            bagfiles_to_process.append(baginfo)
            total_bytes_to_process += baginfo['size']

        # Parallel indexing of bag files.
        nprocs = max(1, min(cpu_count(), 4))  # limit to reasonable number of processes
        print(f"Indexing {len(bagfiles_to_process)} files with {nprocs} workers...")
        indexing_results = []

        dt = datetime.datetime.now()
        if bagfiles_to_process:
            with Pool(processes=nprocs) as pool:
                # map each baginfo dict to worker
                bytes_processed = 0
                with tqdm(total=total_bytes_to_process, unit='B', unit_scale=True, desc="Indexing") as pbar:
                    for path, res in pool.imap_unordered(_process_bag_info, bagfiles_to_process):
                        indexing_results.append((path, res))
                        if res['error'] is None:
                            bytes_processed = next(bf['size'] for bf in bagfiles_to_process if bf['path'] == path)
                            # print(f"Processed {path}: {bytes_processed}/{total_bytes_to_process} bytes ({(bytes_processed/total_bytes_to_process)*100:.2f}%)")
                            pbar.update(bytes_processed)
                    
        z = 0
        toskip = []                    
        # Update bagfiles with results
        for baginfo in self.bagfiles:
            for path, res in indexing_results:
                if baginfo['path'] == path:
                    if res['error'] is None:
                        # Capture the start and end time of the bag file.
                        baginfo.update({'start_time': res['start_time']})
                        baginfo.update({'end_time': res['end_time']})
                        # Capture the topics and message types.
                        # These are stored in a global set for the collection, and also
                        # in the baginfo dictionary for each bag file.
                        baginfo.update({'topics': res['topics']})
                        baginfo.update({'msg_types': res['msg_types']})
                        self.msg_types = self.msg_types.union(res['msg_types'])
                        self.topics = self.topics.union(res['topics'])
                        # Capture the message definitions for the global set of message types.
                        for message_type in res['msg_types']:
                            if message_type not in self.msg_defs:
                                self.msg_defs[message_type] = self.get_message_definition(message_type)
                    else:
                        print(f"Error processing {path}: {res['error']}")
                        toskip.append(z)
            z=z+1
        
        print("%d, %f" % (z,(datetime.datetime.now()-dt).total_seconds()) )

        
        # z = 0
        # toskip=[]
        # for baginfo in self.bagfiles:

        #     # skip files that haven't changed since last index
        #     if baginfo['path'] in prev_paths and baginfo['size'] == prev_bagfiles(baginfo['path'])['size']:
        #         z+=1
        #         continue

        #     dt = datetime.datetime.now()
        #     try:
        #         b = rosbag.Bag(baginfo['path'])
        #         # Capture the start and end time of the bag file.
        #         baginfo.update({'start_time': b.get_start_time()})
        #         baginfo.update({'end_time': b.get_end_time()})
        #         # Get topics and message types. 
        #         # These are stored as a global set for the collection, and also
        #         # in the baginfo dictionary for each bag file.
        #         tt = b.get_type_and_topic_info()
        #         self.msg_types = self.msg_types.union(tt.msg_types.keys())
        #         self.topics = self.topics.union(tt.topics.keys())
        #         baginfo.update({'topics': list(tt.topics.keys())})
        #         baginfo.update({'msg_types': list(tt.msg_types.keys())})
        #         # Get message definitions for the global set of message types.
        #         for k,v in tt.msg_types.items():
        #             if k not in self.msg_defs:
        #                 self.msg_defs[k] = self.get_message_definition(v)
        #         b.close()

        #     except (rosbag.ROSBagException, rosbag.ROSBagUnindexedException, ValueError):
        #         print("Error. Skipping %s" % baginfo["path"])
        #         toskip.append(z)
        #     print(baginfo)
        #     z+=1
        #     print("%d, %f" % (z,(datetime.datetime.now()-dt).total_seconds()) )

        for i in sorted(toskip, reverse=True):
            del self.bagfiles[i]
            #tt = b.get_type_and_topic_info()
            #self.msg_types = self.msg_types.union(tt.msg_types.keys())
            #self.topics = self.topics.union(tt.topics.keys())
            #z = z + 1
            #if z % np.floor(len(self.bagfiles)/10) == 0:
            #    print("%0.1f percent complete..." % z/len(self.bagfiles)*100)
        
        # Save index to pickle
        with open(pickle_file, 'wb') as f:
            pickle.dump({
                'collection_directory': self.directory,
                'bagfiles': self.bagfiles,
                'topics': self.topics,
                'msg_types': self.msg_types,
                'msg_defs': self.msg_defs
            }, f)
        print(f"Index saved to {pickle_file}.")

    def print_index(self):
        '''Print a summary of the index.'''
        if len(self.bagfiles) == 0:
            self.find_bag_files(path=self.directory)
        if len(self.topics) == 0:
            self.index_collection()

        print("Bag files in collection:")
        for bf in self.bagfiles:
            print("\t%s, size: %d, start: %f, end: %f, mtime: %f" % 
                  (bf['path'],bf['size'],bf.get('start_time',0),bf.get('end_time',0),bf['mtime']))
        print("Topics in collection:")
        for t in self.topics:
            print("\t%s" % t)
        print("Message types in collection:")
        for mt in self.msg_types:
            print("\t%s" % mt)
        print("Message definitions in collection:")
        for k,v in self.msg_defs.items():
            print(f"Message type: {k}")
            print(v)

    def print_topic_sample(self,regexp=None):
        '''A method to print only topics from first 10 files.
        
        This might be useful when the file archive is long, because reading
        get_type_and_topic_info() is slow over a network.'''
        if len(self.bagfiles) == 0:
            self.find_bag_files(path=self.directory)
        z = 0
        for baginfo in self.bagfiles:
            b = rosbag.Bag(baginfo['path'])
            print("Processing %s." % baginfo["path"])
            tt = b.get_type_and_topic_info()
            self.msg_types = self.msg_types.union(tt.msg_types.keys())
            self.topics = self.topics.union(tt.topics.keys())
            z = z + 1
            if z > 10:
                break

        self.print_topics(regexp=regexp)

    def print_topics(self,regexp=None):
        '''Print topics in the collection.
        
        Optionally only print topics that match a python regular expression.
        '''
 
        print("TOPICS:")
        if len(self.topics) == 0:
            self.index_collection()
        if len(self.topics) != 0:
            if regexp is None:
                for t in self.topics:
                    print("\t%s" % t)
            else:
                for t in self.topics:
                    if re.search(regexp,t):
                        print("\t%s" % t)
        else:
            print("No topics.")

    def get_fields_by_topic(self,topic=None):
        '''A method to show what fields are available in a message.'''

        pass

    def get_field_from_bag(self,filename=None,topic = None,field = None):
        '''A utility function to extract a field from a single bag file.
        
        TODO: Should check for header and use that if it exists, otherwise
        use the ROS timestamp.'''
        b = rosbag.Bag(filename)
        dts = list()
        values = list()
        for topic,message,timestamp in b.read_messages(topics=topic):

            dts.append(float(message.header.stamp.secs) + 
                       float(message.header.stamp.nsecs/1e9))
            values.append(eval('message.' + field))
    
        return dts, values
    
    def get_field(self, topic=None, field=None, start_time=None, end_time=None):
        '''A method to get all occurances of a field in the collection'''

        # Convert string times to UNIX timestamps if provided
        def to_unix(ts):
            if ts is None:
                return None
            if isinstance(ts, (float, int)):
                return float(ts)
            dt = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            return dt.timestamp()

        start_ts = to_unix(start_time)
        end_ts = to_unix(end_time)

        timestamp = []
        values = []
        if len(self.bagfiles) == 0:
            self.find_bag_files(path=self.directory)
        z = 0
        for baginfo in self.bagfiles:
            if start_ts is not None and baginfo.get('end_time',0) < start_ts:
                continue
            if end_ts is not None and baginfo.get('start_time',0) > end_ts:
                continue
            print("Processing %s." % baginfo["path"])
            t, f = self.get_field_from_bag(filename=baginfo['path'],topic=topic,field=field)
            timestamp.extend(t)
            values.extend(f)
            z = z + 1
            #if z > 3:
            #    break

            timestamp = np.array(timestamp)
            values = np.array(values)
            if start_ts is not None:
                mask = timestamp >= start_ts
                timestamp = timestamp[mask]
                values = values[mask]
            if end_ts is not None:
                mask = timestamp <= end_ts
                timestamp = timestamp[mask]
                values = values[mask]
            timestamp = timestamp.tolist()
            values = values.tolist()

        return timestamp,values

    def get_message_definition(self,topic=None):
        '''A method to get the message definition for a topic.'''

        if len(self.bagfiles) == 0:
            self.find_bag_files(self.directory)

        for baginfo in self.bagfiles:
            if topic not in baginfo.get('topics',[]):
                continue
            print("Processing %s." % baginfo["path"])
            b = rosbag.Bag(baginfo['path'])
            foundTopic = False
            for bagtopic, msg, t in b.read_messages():
                if bagtopic == topic:
                    return msg._full_text
                    foundTopic = True
                    break
            if foundTopic:
                break
        return None

    def print_message_def(self,topic=None):
        '''A method to print the message definition for a topic.'''

        if topic in self.msg_defs:
            print(f"Message definition for topic {topic}:")
            print(self.msg_defs[topic])
            return

        msg_def = self.get_message_definition(topic=topic)
        if msg_def is not None:
            print(f"Message definition for topic {topic}:")
            print(msg_def)
        



