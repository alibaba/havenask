#include "navi/tester/NaviTesterResource.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Node.h"
#include "navi/tester/TesterDef.h"

namespace navi {

const std::string NaviTesterResource::RESOURCE_ID = "navi.tester_r";

NaviTesterResource::NaviTesterResource()
    : _controlNode(nullptr)
    , _testNode(nullptr)
{
}

NaviTesterResource::~NaviTesterResource() {
}

void NaviTesterResource::def(ResourceDefBuilder &builder) const {
    builder
        .name(RESOURCE_ID, RS_RUN_GRAPH_EXTERNAL);
}

bool NaviTesterResource::config(ResourceConfigContext &ctx) {
    return true;
}

ErrorCode NaviTesterResource::init(ResourceInitContext &ctx) {
    return EC_NONE;
}

void NaviTesterResource::initLogger(const std::string &logPrefix,
                                    const NaviLoggerPtr &logger)
{
    _logger = NaviObjectLogger(this, logPrefix.c_str(), logger);
}

void NaviTesterResource::addInputNode(Node *node) {
    _inputPortMap.emplace(node->getName(), NodeTestPort(node));
}

void NaviTesterResource::addOutputNode(Node *node) {
    _outputPortMap.emplace(node->getName(), NodeTestPort(node));
}

void NaviTesterResource::setControlNode(Node *node) {
    _controlNode = node;
}

Node *NaviTesterResource::getControlNode() const {
    return _controlNode;
}

void NaviTesterResource::setTestNode(Node *node) {
    _testNode = node;
}

Node *NaviTesterResource::getTestNode() const {
    return _testNode;
}

bool NaviTesterResource::validate() const {
    return _controlNode && _testNode;
}

bool NaviTesterResource::setInput(const std::string &portName,
                                  const DataPtr &data,
                                  bool eof)
{
    StreamData streamData;
    streamData.data = data;
    streamData.eof = eof;
    return setInput(portName, streamData);
}

bool NaviTesterResource::setInput(const std::string &portName,
                                  const StreamData &data)
{
    auto it = _inputPortMap.find(portName);
    if (_inputPortMap.end() == it) {
        NAVI_LOG(ERROR, "kernel has no input port named [%s]",
                 portName.c_str());
        return false;
    }
    auto &streamData = it->second.data;
    if (streamData.eof) {
        NAVI_LOG(ERROR, "input port data after eof, port [%s]",
                 portName.c_str());
        return false;
    }
    streamData = data;
    streamData.hasValue = true;
    auto node = it->second.node;
    node->setFrozen(false);
    bool ret = false;
    if (node->isStarted()) {
        ret = node->schedule(node);
    } else {
        ret = node->startSchedule(node);
    }
    node->setFrozen(true);
    if (!ret) {
        if (node->isFinished()) {
            NAVI_LOG(ERROR, "kernel finished");
        } else {
            NAVI_LOG(ERROR, "input port data override, port [%s]",
                     portName.c_str());
        }
        return false;
    }
    return true;
}

bool NaviTesterResource::getInput(const std::string &portName, StreamData &data) {
    auto it = _inputPortMap.find(portName);
    if (_inputPortMap.end() == it) {
        NAVI_LOG(ERROR, "kernel has no input port named [%s]",
                 portName.c_str());
        return false;
    }
    auto &portData = it->second.data;
    data = portData;
    portData.reset(false);
    return true;
}

bool NaviTesterResource::setOutput(const std::string &portName,
                                   const StreamData &data)
{
    auto it = _outputPortMap.find(portName);
    if (_outputPortMap.end() == it) {
        NAVI_LOG(ERROR, "kernel has no output port named [%s]",
                 portName.c_str());
        return false;
    }
    auto &streamData = it->second.data;
    if (streamData.eof) {
        NAVI_LOG(ERROR, "output port data after eof, port [%s]",
                 portName.c_str());
        return false;
    }
    streamData = data;
    streamData.hasValue = true;
    return true;
}

bool NaviTesterResource::getOutput(const std::string &portName, DataPtr &data,
                                   bool &eof)
{
    data = nullptr;
    eof = false;
    StreamData streamData;
    if (!getOutput(portName, streamData)) {
        return false;
    }
    if (streamData.hasValue) {
        data = streamData.data;
        eof = streamData.eof;
    }
    return true;
}

bool NaviTesterResource::getOutput(const std::string &portName,
                                   StreamData &data)
{
    auto it = _outputPortMap.find(portName);
    if (_outputPortMap.end() == it) {
        NAVI_LOG(ERROR, "kernel has no output port named [%s]",
                 portName.c_str());
        return false;
    }
    auto node = it->second.node;
    bool ret = false;
    if (node->isStarted()) {
        ret = node->schedule(node, true);
    } else {
        ret = node->startSchedule(node, true);
    }
    if (!ret) {
        NAVI_LOG(ERROR, "output port [%s] has no data", portName.c_str());
        return false;
    }
    auto &portData = it->second.data;
    data = portData;
    portData.reset(false);
    return true;
}

REGISTER_RESOURCE(NaviTesterResource);

}

