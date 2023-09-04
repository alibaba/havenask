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
#ifndef NAVI_GRAPHDOMAINLOCAL_H
#define NAVI_GRAPHDOMAINLOCAL_H

#include "navi/engine/GraphDomain.h"

namespace navi {

class GraphDomainLocal : public GraphDomain
{
public:
    GraphDomainLocal(Graph *graph);
    ~GraphDomainLocal();
private:
    GraphDomainLocal(const GraphDomainLocal &);
    GraphDomainLocal &operator=(const GraphDomainLocal &);
public:
    bool run() override;
};

}

#endif //NAVI_GRAPHDOMAINLOCAL_H
