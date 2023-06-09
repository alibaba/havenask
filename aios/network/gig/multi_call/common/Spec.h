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
#ifndef ISEARCH_MULTI_CALL_SPEC_H
#define ISEARCH_MULTI_CALL_SPEC_H

#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {

struct Spec {
    Spec();
    bool operator==(const Spec &rhs) const;
    bool operator!=(const Spec &rhs) const;
    std::string getAnetSpec(ProtocolType type) const;
    std::string getSpecStr(ProtocolType type) const;
    bool hasValidPort() const;

    std::string ip;
    int32_t ports[MC_PROTOCOL_UNKNOWN + 1];

    friend std::ostream& operator<<(std::ostream& os, const Spec& spec);
};

struct HeartbeatSpec {
    Spec spec;
    std::string clusterName;
    bool enableClusterBizSearch = false;
};

typedef std::vector<HeartbeatSpec> HeartbeatSpecVec;

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_SPEC_H
