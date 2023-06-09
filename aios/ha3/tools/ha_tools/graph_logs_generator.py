# Python
import tensorflow as tf
from tensorflow.python.ops import control_flow_ops
from optparse import OptionParser
import sys
import os

class Ha3GraphGenerator(object):
    '''
default_searcher_generator.py
    {-i so_dir                  | --input=so_dir}
    {-o output_name             | --output=output_name}
    '''

    def __init__(self):
        self.parser = OptionParser(usage=self.__doc__)

    def usage(self):
        print self.__doc__ % {'prog' : sys.argv[0]}

    def addOptions(self):
        self.parser.add_option('-i', '--input', action='store', dest='defSoDir')
        self.parser.add_option('-o', '--output', action='store', dest='outputName')

    def parseParams(self, optionList):
        self.optionList = optionList
        self.addOptions()
        (options, args) = self.parser.parse_args(optionList)
        self.options = options
        ret = self.checkOptionsValidity(options, args)
        if not ret:
            print "ERROR: checkOptionsValidity Failed!"
            return False
        self.initMember(options, args)
        return True

    def checkOptionsValidity(self, options, args):
        if options.defSoDir == None or options.defSoDir == '':
            options.defSoDir = '/ha3_develop/install_root/usr/local/lib64/libha3_suez_turing_opdef.so'
        if len(args) < 2:
            print "ERROR: Usage: graph_logs_generator [options] graph_file"
            return False
        if options.outputName == None or options.outputName == '':
            options.outputName = os.path.basename(args[1]) + '.logs'
        return True

    def initMember(self, options, args):
        self.defSoDir = options.defSoDir
        self.outputName = options.outputName
        self.graph_file = args[1]

    def getOutputName(self):
        return self.outputName

    def generateLogs(self):
        try:
            tf.load_op_library(self.defSoDir)
        except:
            print 'ERROR: load opdefs lib failed'
            sys.exit(-1)
        with tf.gfile.FastGFile(self.graph_file, "rb") as f:
            graph_def = tf.GraphDef()
            graph_def.ParseFromString(f.read())
            with tf.Graph().as_default() as graph:
                tf.import_graph_def(graph_def)
                tf.summary.FileWriter(self.outputName, graph)

if __name__ == '__main__':
    graphGenerator = Ha3GraphGenerator()

    if not graphGenerator.parseParams(sys.argv):
        graphGenerator.usage()
        sys.exit(-1)

    graphGenerator.generateLogs()
    
