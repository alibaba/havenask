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
#include "navi/builder/BorderId.h"
#include "autil/HashAlgorithm.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/proto/NaviStream.pb.h"

namespace navi {

BorderId::BorderId(IoType ioType_, GraphId id_, GraphId peer_) {
    memset((void *)this, 0, sizeof(*this));
    ioType = ioType_;
    id = id_;
    peer = peer_;
}

BorderId::BorderId(GraphId id_, const BorderDef &borderDef)
    : BorderId((IoType)borderDef.io_type(),
               id_,
               (GraphId)borderDef.peer().graph_id())
{
}

BorderId::BorderId(const BorderIdDef &borderIdDef)
    : BorderId((IoType)borderIdDef.io_type(),
               borderIdDef.graph_id(),
               (GraphId)borderIdDef.peer())
{
}

bool BorderId::operator==(const BorderId &other) const {
    return ioType == other.ioType
        && id == other.id
        && peer == other.peer;
}

std::ostream &operator<<(std::ostream &os, const BorderId &borderId) {
    return os << "(" << &borderId << ","
              << "ioType[" << borderId.ioType << "],"
              << "id[" << borderId.id << "],"
              << "peer[" << borderId.peer << "]"
              << ")";
}

BorderIdJsonize::BorderIdJsonize(const BorderId &borderId_)
    : borderId(borderId_)
{
}

void BorderIdJsonize::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("io_type", borderId.ioType, borderId.ioType);
    json.Jsonize("graph", borderId.id, borderId.id);
    json.Jsonize("peer", borderId.peer, borderId.peer);
}

size_t BorderIdHash::operator()(const BorderId &border) const {
    return autil::HashAlgorithm::hashString64((const char *)&border,
                                              sizeof(border));
}

}
