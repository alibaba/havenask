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
#ifndef NAVI_LOCALPORTBASE_H
#define NAVI_LOCALPORTBASE_H

#include "navi/engine/Port.h"

namespace navi {

class ReadyBitMap;
class Node;

class LocalPortBase : public Port
{
public:
    LocalPortBase(const NodePortDef &nodePort,
                  const NodePortDef &peerPort,
                  IoType ioType,
                  SubGraphBorder *border);
    ~LocalPortBase();
private:
    LocalPortBase(const LocalPortBase &);
    LocalPortBase &operator=(const LocalPortBase &);
public:
    void setNode(Node *node, ReadyBitMap *nodeReadyMap);
protected:
    void scheduleNode() const;
protected:
    autil::ThreadMutex _lock;
    Node *_node;
    ReadyBitMap *_nodeReadyMap;
};

}

#endif //NAVI_LOCALPORTBASE_H
