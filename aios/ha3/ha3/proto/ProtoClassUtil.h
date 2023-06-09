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
#pragma once

#include <stdint.h>
#include <memory>
#include <string>

#include "autil/CompressionUtil.h"
#include "ha3/proto/BasicDefs.pb.h"

namespace isearch {
namespace proto {

class ProtoClassUtil
{
public:
    ProtoClassUtil();
    ~ProtoClassUtil();
private:
    ProtoClassUtil(const ProtoClassUtil &);
    ProtoClassUtil& operator = (const ProtoClassUtil &);
public:
    static std::string partitionIdToString(const PartitionID &partitionId);
    static std::string partitionIdToStringForDisplay(const PartitionID &partId);
    static std::string simplifyPartitionIdStr(const std::string &partId);
    static std::string toString(const PartitionID &partitionId);
    static PartitionID createPartitionID(
            const std::string &clusterName,
            RoleType type, 
            uint16_t from, uint16_t to, 
            uint32_t fullVersion, 
            uint32_t majorConfigVersion = 0);
    static const PartitionID& getQrsPartitionID();
    static CompressType convertCompressType(autil::CompressType type);
    static autil::CompressType toHaCompressType(CompressType type);
private:
};

typedef std::shared_ptr<ProtoClassUtil> ProtoClassUtilPtr;

} // namespace proto
} // namespace isearch

