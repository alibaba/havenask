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
#include "navi/builder/SingleE.h"
#include "navi/proto/GraphDef.pb.h"

namespace navi {

SingleE::SingleE(EdgeDef *edge)
    : _edge(edge)
{
}

SingleE::~SingleE() {
}

void SingleE::require(bool require) {
    if (_edge) {
        _edge->set_require(require);
    }
}

std::ostream &operator<<(std::ostream &os, const SingleE &e) {
    if (!e._edge) {
        return os;
    }
    os << "edge "
       << e._edge->input().node_name() << ":" << e._edge->input().port_name()
       << " -> "
       << e._edge->output().node_name() << ":" << e._edge->output().port_name();
    return os;
}

}

