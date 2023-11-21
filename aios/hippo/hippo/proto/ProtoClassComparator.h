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
#ifndef HIPPO_PROTOCLASSCOMPARATOR_H
#define HIPPO_PROTOCLASSCOMPARATOR_H

#include "hippo/proto/Common.pb.h"

namespace hippo {
namespace proto {

template<typename T>
struct ProtoMemberEqual {
    bool operator() (const T &lhs, const T&rhs) const {
        return lhs == rhs;
    }
};
bool operator<(const SlotId &lhs, const SlotId &rhs);
bool operator==(const Resource &lhs, const Resource &rhs);
bool operator!=(const Resource &lhs, const Resource &rhs);
bool operator==(const SlotResource &lhs, const SlotResource &rhs);
bool operator==(const ProcessStatus &lhs, const ProcessStatus &rhs);

}
}

#endif //HIPPO_PROTOCLASSCOMPARATOR_H
