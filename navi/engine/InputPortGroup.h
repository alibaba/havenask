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
#ifndef NAVI_INPUTPORTGROUP_H
#define NAVI_INPUTPORTGROUP_H

#include "navi/common.h"
#include "navi/engine/Edge.h"
#include "navi/proto/KernelDef.pb.h"

namespace navi {

class ReadyBitMap;
class Node;
class PartInfo;

class InputPortGroup
{
public:
    InputPortGroup(InputTypeDef type);
    ~InputPortGroup();
private:
    InputPortGroup(const InputPortGroup &);
    InputPortGroup &operator=(const InputPortGroup &);
public:
    IndexType addInput(IndexType index, const EdgeOutputInfo &info);
    bool postInit(const PartInfo &partInfo, autil::mem_pool::Pool *pool);
    bool postInit(autil::mem_pool::Pool *pool);
    void setInputIndexStart(IndexType start);
    IndexType getInputIndexStart() const;
    ReadyBitMap *getReadyMap() const;
    size_t getInputCount() const;
    bool isOk() const;
    bool hasReady() const;
    bool isEof() const;
    void setEof(const Node *callNode) const;
    EdgeOutputInfo getEdgeOutputInfo(IndexType index) const;
private:
    static bool compare(const std::pair<IndexType, EdgeOutputInfo> &left,
                        const std::pair<IndexType, EdgeOutputInfo> &right);
private:
    InputTypeDef _type;
    bool _autoFillIndex;
    ReadyBitMap *_readyMap;
    std::vector<std::pair<IndexType, EdgeOutputInfo>> _inputs;
    IndexType _inputIndexStart;
};

extern std::ostream &operator<<(std::ostream &os, const InputPortGroup &inputGroup);
extern std::ostream &operator<<(std::ostream &os, const InputPortGroup *inputGroup);
}

#endif //NAVI_INPUTPORTGROUP_H
