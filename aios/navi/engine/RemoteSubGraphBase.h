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
#ifndef NAVI_REMOTESUBGRAPHBASE_H
#define NAVI_REMOTESUBGRAPHBASE_H

#include "navi/engine/SubGraphBase.h"

namespace navi {

class RemoteSubGraphBase : public SubGraphBase
{
public:
    RemoteSubGraphBase(SubGraphDef *subGraphDef,
                       GraphParam *param,
                       const GraphDomainPtr &domain,
                       const SubGraphBorderMapPtr &borderMap,
                       Graph *graph);
    ~RemoteSubGraphBase();
private:
    RemoteSubGraphBase(const RemoteSubGraphBase &);
    RemoteSubGraphBase &operator=(const RemoteSubGraphBase &);
public:
    ErrorCode init() override;
    ErrorCode run() override;
    void terminate(ErrorCode ec) override;
};

}

#endif //NAVI_REMOTESUBGRAPHBASE_H
