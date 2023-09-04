#ifndef NAVI_NAVITESTERRESOURCE_H
#define NAVI_NAVITESTERRESOURCE_H

#include "navi/engine/Data.h"
#include "navi/engine/Resource.h"

namespace navi {

class Node;

struct NodeTestPort {
public:
    NodeTestPort(Node *node_)
        : node(node_)
    {
    }
public:
    Node *node;
    StreamData data;
};

class NaviTesterResource : public Resource
{
public:
    NaviTesterResource();
    ~NaviTesterResource();
    NaviTesterResource(const NaviTesterResource &) = delete;
    NaviTesterResource &operator=(const NaviTesterResource &) = delete;
public:
    void def(ResourceDefBuilder &builder) const override;
    bool config(ResourceConfigContext &ctx) override;
    ErrorCode init(ResourceInitContext &ctx) override;
public:
    void initLogger(const std::string &logPrefix, const NaviLoggerPtr &logger);
    void addInputNode(Node *node);
    void addOutputNode(Node *node);
    void setControlNode(Node *node);
    Node *getControlNode() const;
    void setTestNode(Node *node);
    Node *getTestNode() const;
    bool validate() const;
public:
    bool setInput(const std::string &portName, const DataPtr &data, bool eof);
    bool setInput(const std::string &portName, const StreamData &data);
    bool getInput(const std::string &portName, StreamData &data);
    bool compute();
    bool setOutput(const std::string &portName, const StreamData &data);
    bool getOutput(const std::string &portName, DataPtr &data, bool &eof);
    bool getOutput(const std::string &portName, StreamData &data);
public:
    static const std::string RESOURCE_ID;
private:
    DECLARE_LOGGER();
    Node *_controlNode;
    Node *_testNode;
    std::unordered_map<std::string, NodeTestPort> _inputPortMap;
    std::unordered_map<std::string, NodeTestPort> _outputPortMap;
};

NAVI_TYPEDEF_PTR(NaviTesterResource);

}

#endif //NAVI_NAVITESTERRESOURCE_H
