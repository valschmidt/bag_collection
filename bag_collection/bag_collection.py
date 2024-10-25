# A python module for reading data from a collection of bag files.

import os
import rosbag
import re
import numpy as np
import datetime

class bag_collection():
    def __init__(self,directory):

        self.directory = directory
        self.bagfiles = []
        self.file_index = None
        self.topics = set()
        self.msg_types = set()

    def find_bag_files(self,path, extension='.bag'):
        """Recursively finds all files with the given extension in the specified directory."""
        
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(extension):
                    info = {'path': os.path.join(root, file)}
                    self.bagfiles.append(info)
                    # yield os.path.join(root, file)

    def print_bag_files(self):
        '''Print a list of the bag files in the collection.'''
        if self.bagfiles is None:
            self.find_bag_files(path.self.directory)

        for f in self.bagfiles:
            print("\t%s" % f['path'])


    def index_collection(self):
        '''Create an index of the collection
        
        Currently this gets a list of all possible topics and message types.
        It doesn't really index the files in the sense of knowing the start/stop
        times of each file or what messages they contain. TODO

        '''
        print("Indexing collection...")
        self.msg_types = set()
        self.topics = set()
        if len(self.bagfiles) == 0:
            self.find_bag_files(path=self.directory)
        z = 0
        toskip=[]
        for baginfo in self.bagfiles:
            dt = datetime.datetime.now()
            b = rosbag.Bag(baginfo['path'])
            try:
                baginfo.update({'start_time': b.get_start_time()})
                baginfo.update({'end_time': b.get_end_time()})
            except rosbag.ROSBagException:
                print("Error. Skipping %s" % baginfo["path"])
                toskip.append(z)
            print(baginfo)
            z+=1
            print("%d, %f" % (z,(datetime.datetime.now()-dt).total_seconds()) )
        # remote files that don't records
        for i in sorted(toskip, reverse=True):
            del self.bagfiles[i]
            #tt = b.get_type_and_topic_info()
            #self.msg_types = self.msg_types.union(tt.msg_types.keys())
            #self.topics = self.topics.union(tt.topics.keys())
            #z = z + 1
            #if z % np.floor(len(self.bagfiles)/10) == 0:
            #    print("%0.1f percent complete..." % z/len(self.bagfiles)*100)
    
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
            if z > 1:
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
    
        return dts,values
    
    def get_field(self, topic=None, field=None):
        '''A method to get all occurances of a field in the collection'''
        timestamp = []
        values = []
        if len(self.bagfiles) == 0:
            self.find_bag_files(path=self.directory)
        z = 0
        for baginfo in self.bagfiles:
            t, f = self.get_field_from_bag(filename=baginfo['path'],topic=topic,field=field)
            timestamp.extend(t)
            values.extend(f)
            z = z + 1
            #if z > 3:
            #    break

        return timestamp,values

    def print_message_def(self,topic=None):
        '''A method to print the message definition for a topic.'''

        if len(self.bagfiles) == 0:
            self.find_bag_files(self.directory)

        for baginfo in self.bagfiles:
            b = rosbag.Bag(baginfo['path'])
            foundTopoic = False
            for bagtopic, msg, t in b.read_messages():
                if bagtopic == topic:
                    print(msg._full_text)
                    foundTopic = True
                    break
            if foundTopic:
                break


