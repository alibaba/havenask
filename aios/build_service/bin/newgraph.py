#!/bin/env python
import sys
import os

def createGraphFile(path, graphName):
    if os.path.exists(path + "/" + graphName + ".graph"):
        print "graph file " + path + "/" + graphName + ".graph exists!"
        return False

    context = ""
    context += "--[[\n"
    context += "Graph.parameter : graph related kv parameters \n"
    context += "Graph.loadSimpleFlow(taskType, taskIdentifier, taskKVParam = nil) : return flowObject\n"
    context += "Graph.loadFlow(flowFileName, flowKVParam = nil) : return flowObject\n"
    context += "Graph.loadSubGraph(graphId, graphFileName, graphKVParam = nil) : return true : false\n"
    context += "Graph.setUpstream(flow, upstreamFlow, waitStatus = \"finish\"|\"stop\"|\"xxx\") \n"
    context += "Graph.declareFriend(flow, friendFlow) \n"
    context += "Graph.getFlowIdByTag(tag) : return flowId list \n"
    context += "Graph.getFlow(flowId) : return flowObject \n"

    context += "--]]\n"
    
    context += "" + "\n"
    context += "--#import(Tool)" + "\n"
    context += "" + "\n"

    context += "function graphDef()\n"
    context += "" + "\n"
    context += "    return true" + "\n"
    context += 'end\n'
    context += '\n'

    f = file(path + "/" + graphName + ".graph", "w")
    f.write(context)
    f.close()
    return True

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage:", sys.argv[0], "<graphName>"
        sys.exit(1)
    path = os.getcwd()
    createGraphFile(path, sys.argv[1])
