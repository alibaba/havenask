#!/bin/env python
import sys
import os

def createFlowFile(path, flowName):
    if os.path.exists(path + "/" + flowName + ".flow"):
        print "flow file " + path + "/" + flowName + ".flow exists!"
        return False

    context = ""
    context += "--[[\n"
    context += "Flow.parameter : flow related kv parameters \n"
    context += "Flow.createTask(taskId, taskType, taskKvParam = nil) \n"
    context += "Flow.getTaskProperty(taskId, key, flowId = nil) \n"
    context += "Flow.getTaskInFlow(taskId, flowId) \n"
    context += "Flow.setError(msg) \n"
    context += "Flow.setFatalError(msg) \n"
    context += "Flow.setProperty(key, value) \n"
    context += "Flow.getProperty(key) \n"
    context += "Flow.startTask(taskObj, kvParam = nil) \n"
    context += "Flow.finishTask(taskObj, kvParam = nil) \n"
    context += "######################################################### \n"
    context += "function stepRunFlow() will be called by single TaskFlow, return true means eof of flow \n"
    context += "--]]\n"

    context += "" + "\n"
    context += "--#import(Tool)" + "\n"
    context += "" + "\n"

    context += "function stepRunFlow()\n"
    context += "" + "\n"
    context += "    return true" + "\n"
    context += 'end\n'
    context += '\n'

    f = file(path + "/" + flowName + ".flow", "w")
    f.write(context)
    f.close()
    return True

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage:", sys.argv[0], "<flowName>"
        sys.exit(1)
    path = os.getcwd()
    createFlowFile(path, sys.argv[1])
