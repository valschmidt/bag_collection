# A python module for reading data from a collection of bag files.

import os
import rosbag
import re

class bag_collection():
    def __init__(self,directory):

        self.directory = directory
        self.file_list = None
        self.start_time = None
        self.stop_time = None
        self.file_index = None
        self.topics = set()
        self.msg_types = set()

    def find_bag_files(self,path, extension='.bag'):
        """Recursively finds all files with the given extension in the specified directory."""
        self.file_list = []
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(extension):
                    self.file_list.append(os.path.join(root, file))
                    # yield os.path.join(root, file)

    def print_bag_files(self):
        '''Print a list of the bag files in the collection.'''
        if self.file_index is None:
            self.find_bag_files(path.self.directory)

        for f in self.file_list:
            print("\t%s" % f)


    def index_collection(self):
        '''Create an index of the collection
        
        Currently this gets a list of all possible topics and message types.
        It doesn't really index the files in the sense of knowing the start/stop
        times of each file or what messages they contain. TODO

        '''
        print("Indexing collection...")
        self.msg_types = set()
        self.topics = set()
        if self.file_list is None:
            self.find_bag_files(path=self.directory)
        z = 0
        for bagfile in self.file_list:
            b = rosbag.Bag(bagfile)
            tt = b.get_type_and_topic_info()
            self.msg_types = self.msg_types.union(tt.msg_types.keys())
            self.topics = self.topics.union(tt.topics.keys())
            z = z + 1
            if z % np.floor(len(self.file_list)/10) == 0:
                print("%0.1f\% complete..." % z/len(self.file_list)*100)
    
    def print_topic_sample(self,regexp=None):
        '''A method to print only topics from first 10 files.
        
        This might be useful when the file archive is long, because reading
        get_type_and_topic_info() is slow over a network.'''
        if self.file_list is None:
            self.find_bag_files(path=self.directory)
        z = 0
        for bagfile in self.file_list:
            b = rosbag.Bag(bagfile)
            print("Processing %s." % bagfile)
            tt = b.get_type_and_topic_info()
            self.msg_types = self.msg_types.union(tt.msg_types.keys())
            self.topics = self.topics.union(tt.topics.keys())
            z = z + 1
            if z > 1:
                break

        self.print_topics()

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
        if self.file_list is None:
            self.find_bag_files(path=self.directory)
        z = 0
        for b in self.file_list:
            t, f = self.get_field_from_bag(filename=b,topic=topic,field=field)
            timestamp.append(t)
            values.append(f)
            z = z + 1
            if z > 3:
                break

        return timestamp,values




