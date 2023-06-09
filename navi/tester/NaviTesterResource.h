/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
