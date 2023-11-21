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
#include <string>
#include <vector>

#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"

namespace build_service { namespace util {

class IndexPathConstructor
{
private:
    IndexPathConstructor();
    ~IndexPathConstructor();
    IndexPathConstructor(const IndexPathConstructor&);
    IndexPathConstructor& operator=(const IndexPathConstructor&);

public:
    static std::string getGenerationId(const std::string& indexRoot, const std::string& clusterName,
                                       const std::string& buildMode, const std::string& rangeFrom,
                                       const std::string& rangeTo);

    static bool parsePartitionRange(const std::string& partitionPath, uint32_t& rangeFrom, uint32_t& rangeTo);

    static std::string constructIndexPath(const std::string& indexRoot, const std::string& clusterName,
                                          const std::string& generationId, const std::string& rangeFrom,
                                          const std::string& rangeTo);

    static std::string constructSyncRtIndexPath(const std::string& indexRoot, const std::string& clusterName,
                                                const std::string& generationId, const std::string& rangeFrom,
                                                const std::string& rangeTo, const std::string& remoteRtDir);

    static std::string constructIndexPath(const std::string& indexRoot, const proto::PartitionId& partitionId);

    static std::string constructDocReclaimDirPath(const std::string& indexRoot, const proto::PartitionId& partitionId);

    static std::string constructDocReclaimDirPath(const std::string& indexRoot, const std::string& clusterName,
                                                  generationid_t generationId);

    static generationid_t getNextGenerationId(const std::string& indexRoot, const std::string& clusterName);
    static generationid_t getNextGenerationId(const std::string& indexRoot,
                                              const std::vector<std::string>& clusterNames);
    // note: may return incomplete getGenerationList when read the remote path failed.
    static std::vector<generationid_t> getGenerationList(const std::string& indexRoot, const std::string& clusterName);
    static std::string getGenerationIndexPath(const std::string& indexRoot, const std::string& clusterName,
                                              generationid_t generationId);
    static std::string getMergerCheckpointPath(const std::string& indexRoot, const proto::PartitionId& partitionId);
    static std::string constructGenerationPath(const std::string& indexRoot, const proto::PartitionId& partitionId);
    static std::string constructGenerationPath(const std::string& indexRoot, const std::string& clusterName,
                                               generationid_t generationId);

private:
    static bool isGenerationContainRange(const std::string& indexRoot, const std::string& clusterName,
                                         generationid_t generationId, const std::string& rangeFrom,
                                         const std::string& rangeTo);

public:
    static const std::string GENERATION_PREFIX;
    static const std::string PARTITION_PREFIX;
    static const std::string DOC_RECLAIM_DATA_DIR;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::util
