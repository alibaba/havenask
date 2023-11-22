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

#include <set>
#include <stdint.h>
#include <string>

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class BuildTaskValidator
{
public:
    BuildTaskValidator(const config::ResourceReaderPtr& resourceReader, const proto::BuildId& buildId,
                       const std::string& indexRoot, const std::string& generationZkRoot,
                       const proto::DataDescription& realTimeDataDesc);
    ~BuildTaskValidator();

    BuildTaskValidator(const BuildTaskValidator&) = delete;
    BuildTaskValidator& operator=(const BuildTaskValidator&) = delete;

public:
    static std::set<std::string> GetPartitionDirs(uint32_t partitionCount);
    static bool CleanTaskSignature(const config::ResourceReaderPtr& resourceReader, const std::string& indexRoot,
                                   const proto::BuildId& buildId, std::string* errorMsg);

public:
    bool ValidateAndWriteSignature(bool validateExistingIndex, std::string* errorMsg) const;

private:
    bool ValidateRealtimeInfo(const proto::DataDescription& dataDescription, const std::string& generationRoot,
                              std::string* errorMsg) const;

    bool ValidateRealtimeInfoAndPartitionCount(const std::string& generationRoot, const std::string& clusterName,
                                               std::string* errorMsg) const;

    bool ValidateSingleClusterRealtimeInfo(const std::string& clusterName) const;
    bool WriteTaskSignature(const std::string& signatureFilePath) const;
    bool ValidateAndWriteTaskSignature(const std::string& generationRoot, std::string* errorMsg) const;
    bool ValidatePartitionCount(const std::string& generationRoot, const std::string& clusterName,
                                std::string* errorMsg) const;
    bool ValidateTaskSignature(const std::string& signatureFilePath, bool* signatureExist, std::string* errorMsg) const;

public:
    // For test
    bool TEST_prepareFakeIndex() const;

public:
    struct BuildTaskSignature : public autil::legacy::Jsonizable {
        std::string zkRoot;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("task_zk_root", zkRoot, zkRoot);
        }
    };

public:
    static const std::string BUILD_TASK_SIGNATURE_FILE;

private:
    config::ResourceReaderPtr _resourceReader;
    proto::BuildId _buildId;
    std::string _indexRoot;
    std::string _generationZkRoot;
    proto::DataDescription _dataDescription;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildTaskValidator);

}} // namespace build_service::admin
