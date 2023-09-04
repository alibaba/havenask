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
#ifndef NAVI_OUTPUTPORTGROUP_H
#define NAVI_OUTPUTPORTGROUP_H

#include "navi/common.h"

namespace navi {

class Node;
class Edge;
class PartInfo;
class ReadyBitMap;

class OutputPortGroup
{
public:
    OutputPortGroup();
    ~OutputPortGroup();
private:
    OutputPortGroup(const OutputPortGroup &);
    OutputPortGroup &operator=(const OutputPortGroup &);
public:
    void addOutput(IndexType index, Edge *edge);
    bool postInit(const PartInfo &partInfo);
    bool postInit();
    void setOutputIndexStart(IndexType start);
    IndexType getOutputIndexStart() const;
    ReadyBitMap *getReadyMap() const;
    size_t getOutputCount() const;
    bool isOk() const;
    bool isEof() const;
    Edge *getEdge(size_t index) const;
    size_t getOutputDegree() const;
    void forceEof() const;
private:
    static bool compare(const std::pair<IndexType, Edge *> &left,
                        const std::pair<IndexType, Edge *> &right);
private:
    ReadyBitMap *_readyMap;
    std::vector<std::pair<IndexType, Edge *> > _outputs;
    IndexType _outputIndexStart;
};

}

#endif //NAVI_OUTPUTPORTGROUP_H
