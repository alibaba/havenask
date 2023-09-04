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
#include "ha3/proto/ProtoClassUtil.h"

#include <limits>
#include <sstream>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/proto/BasicDefs.pb.h"

using namespace std;
using namespace autil;
namespace isearch {
namespace proto {

ProtoClassUtil::ProtoClassUtil() {}

ProtoClassUtil::~ProtoClassUtil() {}

string ProtoClassUtil::partitionIdToString(const PartitionID &partId) {
    return RoleType_Name(partId.role()) + '_' + partId.clustername() + '_'
           + StringUtil::toString(partId.majorconfigversion()) + '_'
           + StringUtil::toString(partId.fullversion()) + '_'
           + StringUtil::toString(partId.range().from()) + '_'
           + StringUtil::toString(partId.range().to());
}

string ProtoClassUtil::partitionIdToStringForDisplay(const PartitionID &pid) {
    ostringstream oss;
    if (pid.role() != ROLE_QRS) {
        oss << pid.clustername() << ".";
    }
    oss << RoleType_Name(pid.role()) << ".Generation[" << pid.fullversion() << "]"
        << ".Range[" << pid.range().from() << "-" << pid.range().to() << "]";
    return oss.str();
}

string ProtoClassUtil::simplifyPartitionIdStr(const string &partId) {
    const string token = "ROLE_";
    if (partId.find(token) == 0) {
        return partId.substr(token.length());
    }
    return partId;
}

string ProtoClassUtil::toString(const PartitionID &partitionId) {
    return partitionId.ShortDebugString();
}

PartitionID ProtoClassUtil::createPartitionID(const std::string &clusterName,
                                              RoleType type,
                                              uint16_t from,
                                              uint16_t to,
                                              uint32_t fullversion,
                                              uint32_t majorConfigVersion) {
    Range range;
    range.set_from(from);
    range.set_to(to);
    PartitionID partitionID;
    *(partitionID.mutable_range()) = range;

    partitionID.set_clustername(clusterName);
    partitionID.set_majorconfigversion(majorConfigVersion);

    partitionID.set_role(type);
    partitionID.set_fullversion(fullversion);

    return partitionID;
}

const PartitionID &ProtoClassUtil::getQrsPartitionID() {
    static PartitionID qrsPartitionID
        = createPartitionID("", ROLE_QRS, 0, numeric_limits<uint16_t>::max(), 0, 0);

    return qrsPartitionID;
}

CompressType ProtoClassUtil::convertCompressType(autil::CompressType type) {
    switch (type) {
    case autil::CompressType::Z_SPEED_COMPRESS:
        return CT_Z_SPEED_COMPRESS;
    case autil::CompressType::Z_DEFAULT_COMPRESS:
        return CT_Z_DEFAULT_COMPRESS;
    case autil::CompressType::SNAPPY:
        return CT_SNAPPY;
    case autil::CompressType::LZ4:
        return CT_LZ4;
    default:
        return CT_NO_COMPRESS;
    }
}

autil::CompressType ProtoClassUtil::toHaCompressType(CompressType type) {
    switch (type) {
    case CT_Z_SPEED_COMPRESS:
        return autil::CompressType::Z_SPEED_COMPRESS;
    case CT_Z_DEFAULT_COMPRESS:
        return autil::CompressType::Z_DEFAULT_COMPRESS;
    case CT_SNAPPY:
        return autil::CompressType::SNAPPY;
    case CT_LZ4:
        return autil::CompressType::LZ4;
    default:
        return autil::CompressType::NO_COMPRESS;
    }
}

} // namespace proto
} // namespace isearch
