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
#include "swift/admin/AdminRequestChecker.h"

#include <iosfwd>
#include <set>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/result/Errors.h"
#include "autil/result/ForwardList.h"
#include "swift/admin/ParaChecker.h"
#include "swift/config/AdminConfig.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/util/LogicTopicHelper.h"

using namespace std;
using namespace swift::protocol;
using namespace swift::admin;
using namespace swift::util;
using namespace autil;
using namespace autil::result;

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, AdminRequestChecker);

const uint32_t MIN_PARTITION_COUNT = 0;
const uint32_t MAX_PARTITION_COUNT = 65536;
const uint32_t MIN_PARTITION_BUFFER_SIZE = 0;
const uint32_t MAX_PARTITION_BUFFER_SIZE = 256 * 1024;
const uint32_t MIN_RESOURCE = 0;
const uint32_t MAX_RESOURCE = 10000;
const uint32_t MIN_PARTITION_LIMIT = 1;
const int64_t MIN_OBSOLETE_FILE_TIME_INTERVAL = 1;
const int32_t MIN_RESERVED_FILE_COUNT = 2;

AdminRequestChecker::AdminRequestChecker() {}

AdminRequestChecker::~AdminRequestChecker() {}

Result<bool> AdminRequestChecker::checkTopicCreationRequest(const TopicCreationRequest *request,
                                                            config::AdminConfig *config,
                                                            bool checkPartCnt) {
    bool hasDfsRoot = !config->getDfsRoot().empty();
    // Check topic name
    if (!request->has_topicname() || !ParaChecker::checkValidTopicName(request->topicname())) {
        AUTIL_LOG(ERROR, "Invalid topic name [%s]", request->ShortDebugString().c_str());
        return RuntimeError::make("Invalid topic name");
    }
    if (checkPartCnt) {
        if (!request->has_partitioncount()) {
            AUTIL_LOG(ERROR, "Invalid topic partition, request [%s]", request->ShortDebugString().c_str());
            return RuntimeError::make("Topic partition not assigned");
        }
    }
    if (request->has_partitioncount()) {
        // Check partition count
        if (request->partitioncount() <= MIN_PARTITION_COUNT || request->partitioncount() > MAX_PARTITION_COUNT) {
            AUTIL_LOG(ERROR, "Partition count is invalid [%s]", request->ShortDebugString().c_str());
            return RuntimeError::make("Partition count is invalid[%d]", request->partitioncount());
        }
    }
    // Check partition buffer size
    if (request->has_partitionbuffersize()) {
        if (request->partitionbuffersize() <= MIN_PARTITION_BUFFER_SIZE ||
            request->partitionbuffersize() > MAX_PARTITION_BUFFER_SIZE) {
            AUTIL_LOG(ERROR, "Partition buffer size is invalid [%s]", request->ShortDebugString().c_str());
            return RuntimeError::make("Partition buffer size is invalid[%d]", request->partitionbuffersize());
        }
    } else if (request->partitionminbuffersize() <= MIN_PARTITION_BUFFER_SIZE ||
               request->partitionminbuffersize() > MAX_PARTITION_BUFFER_SIZE ||
               request->partitionmaxbuffersize() > MAX_PARTITION_BUFFER_SIZE ||
               request->partitionmaxbuffersize() < request->partitionminbuffersize()) {
        AUTIL_LOG(ERROR, "Partition buffer size is invalid [%s]", request->ShortDebugString().c_str());
        return RuntimeError::make("Partition buffer size is invalid min[%d] max[%d]",
                                  request->partitionminbuffersize(),
                                  request->partitionmaxbuffersize());
    }
    // Check resource
    if (request->resource() <= MIN_RESOURCE || request->resource() > MAX_RESOURCE) {
        AUTIL_LOG(ERROR, "Resource is invalid [%d]", request->resource());
        return RuntimeError::make("Resource is invalid[%d]", request->resource());
    }
    // Check partition limit
    if (request->partitionlimit() < MIN_PARTITION_LIMIT) {
        AUTIL_LOG(ERROR, "partition limit is invalid [%u]", request->partitionlimit());
        return RuntimeError::make("Partition limit is invalid[%d]", request->partitionlimit());
    }
    // check topic mode
    if (TOPIC_MODE_SECURITY == request->topicmode()) {
        if (!hasDfsRoot) {
            AUTIL_LOG(ERROR, "dataDir should not be empty, when topic mode is TOPIC_MODE_SECURITY");
            return RuntimeError::make("Dfsroot not assigned when topic mode is TOPIC_MODE_SECURITY");
        }
        if (request->maxwaittimeforsecuritycommit() < 0 || request->maxdatasizeforsecuritycommit() < 0) {
            AUTIL_LOG(ERROR, "maxWaitTime or maxDataSize should not less than zero");
            return RuntimeError::make("Invalid maxWaitTime[%ld] or maxDataSize[%ld]",
                                      request->maxwaittimeforsecuritycommit(),
                                      request->maxdatasizeforsecuritycommit());
        }
    }
    // check obsoleteFileTimeInterval
    int64_t timeInterval = request->obsoletefiletimeinterval();
    if (timeInterval != TopicCreationRequest::default_instance().obsoletefiletimeinterval() &&
        timeInterval < MIN_OBSOLETE_FILE_TIME_INTERVAL) {
        AUTIL_LOG(ERROR, "obsoleteFileTimeInterval is invalid [%ld].", timeInterval);
        return RuntimeError::make("Invalid obsoleteFileTimeInterval[%ld]", timeInterval);
    }
    // check reservedFileCount
    int32_t fileCount = request->reservedfilecount();
    if (fileCount != TopicCreationRequest::default_instance().reservedfilecount() &&
        fileCount < MIN_RESERVED_FILE_COUNT) {
        AUTIL_LOG(ERROR, "reservedFileCount is invalid [%d].", fileCount);
        return RuntimeError::make("Invalid reservedFileCount[%d]", fileCount);
    }
    if (request->has_needfieldfilter() && request->has_needschema()) {
        if (request->needfieldfilter() && request->needschema()) {
            AUTIL_LOG(ERROR, "needfieldfilter and needschema should set 1 only");
            return RuntimeError::make("needfieldfilter and needschema should set 1 only");
        }
    }
    if (request->topictype() == TOPIC_TYPE_PHYSIC && !LogicTopicHelper::isValidPhysicTopicName(request->topicname())) {
        AUTIL_LOG(ERROR, "can not create physic topic[%s]", request->topicname().c_str());
        return RuntimeError::make("can not create physic topic[%s]", request->topicname().c_str());
    }
    // check rangeCountInPartition
    if (request->has_rangecountinpartition() && 1 != request->rangecountinpartition()) {
        AUTIL_LOG(ERROR, "rangeCountInPartition[%d] invalid, must be 1", request->rangecountinpartition());
        return RuntimeError::make("rangeCountInPartition[%d] invalid, must be 1", request->rangecountinpartition());
    }
    return true;
}

bool AdminRequestChecker::checkTopicDeletionRequest(const TopicDeletionRequest *request) {
    if (request->topicname().empty()) {
        AUTIL_LOG(ERROR, "Invalid topic deletion request [%s].", request->ShortDebugString().c_str());
        return false;
    }
    return true;
}

bool AdminRequestChecker::checkTopicBatchDeletionRequest(const TopicBatchDeletionRequest *request) {
    set<string> delTopics;
    for (int i = 0; i < request->topicnames_size(); i++) {
        if (!request->topicnames(i).empty()) {
            delTopics.insert(request->topicnames(i));
        }
    }
    if (delTopics.empty()) {
        AUTIL_LOG(ERROR, "Invalid batch topic deletion request [%s].", request->ShortDebugString().c_str());
        return false;
    }
    return true;
}

bool AdminRequestChecker::checkTopicInfoRequest(const TopicInfoRequest *request) {
    if (!request->has_topicname()) {
        AUTIL_LOG(ERROR, "Invalid topic info request [%s].", request->ShortDebugString().c_str());
        return false;
    }
    return true;
}

bool AdminRequestChecker::checkPartitionInfoRequest(const PartitionInfoRequest *request) {
    if (!request->has_topicname() || request->partitionids_size() == 0) {
        AUTIL_LOG(ERROR, "Invalid partition info request [%s].", request->ShortDebugString().c_str());
        return false;
    }
    return true;
}

bool AdminRequestChecker::checkRoleAddressRequest(const RoleAddressRequest *request) {
    if (!request->has_role() || !request->has_status()) {
        AUTIL_LOG(ERROR, "Invalid role address request [%s].", request->ShortDebugString().c_str());
        return false;
    }
    RoleType role = request->role();
    RoleStatus status = request->status();
    if (role == ROLE_TYPE_NONE || status == ROLE_STATUS_NONE) {
        AUTIL_LOG(ERROR, "Invalid role address request [%s].", request->ShortDebugString().c_str());
        return false;
    }
    return true;
}

bool AdminRequestChecker::checkErrorRequest(const ErrorRequest *request) {
    if (!request->has_time() && !request->has_count()) {
        AUTIL_LOG(ERROR, "Invalid error request [%s].", request->ShortDebugString().c_str());
        return false;
    }
    return true;
}

bool AdminRequestChecker::checkLogicTopicModify(const TopicCreationRequest &target,
                                                const TopicCreationRequest &current) {
    // valid modify: L -> L, LP -> LP, N -> LP
    // invalid: L -> N, L -> LP, LP -> N, LP -> L, N -> L
    auto tp = target.topictype();
    auto cp = current.topictype();
    if (cp == tp) { // L -> L, LP -> LP
        return true;
    } else if (cp == TOPIC_TYPE_NORMAL && tp == TOPIC_TYPE_LOGIC_PHYSIC) { // N -> LP
        bool ret = target.topicname() == current.topicname() && target.partitioncount() == current.partitioncount() &&
                   target.topicmode() == current.topicmode() && target.needfieldfilter() == current.needfieldfilter() &&
                   target.compressmsg() == current.compressmsg() && target.compressthres() == current.compressthres() &&
                   target.dfsroot() == current.dfsroot() && target.topicgroup() == current.topicgroup() &&
                   target.rangecountinpartition() == current.rangecountinpartition() &&
                   target.needschema() == current.needschema() && target.sealed() == current.sealed() &&
                   0 == target.physictopiclst_size() && target.extenddfsroot_size() == current.extenddfsroot_size() &&
                   target.schemaversions_size() == current.schemaversions_size();
        if (!ret) {
            return false;
        }
        for (int i = 0; i < target.extenddfsroot_size(); ++i) {
            ret &= target.extenddfsroot(i) == current.extenddfsroot(i);
        }
        for (int i = 0; i < target.schemaversions_size(); ++i) {
            ret &= target.schemaversions(i) == current.schemaversions(i);
        }
        return ret;
    } else {
        return false;
    }
}

} // namespace admin
} // namespace swift
