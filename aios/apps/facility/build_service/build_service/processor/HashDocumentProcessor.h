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

#include <string>
#include <vector>

#include "autil/HashFunctionBase.h"
#include "build_service/common_define.h"
#include "build_service/config/HashModeConfig.h"
#include "build_service/document/ExtendDocument.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/kv/Types.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/counter/CounterMap.h"

namespace build_service { namespace processor {

class HashDocumentProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;

public:
    HashDocumentProcessor();
    ~HashDocumentProcessor();

public:
    bool init(const DocProcessorInitParam& param) override;
    bool process(const document::ExtendDocumentPtr& document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs) override;

public:
    void destroy() override;
    DocumentProcessor* clone() override;
    std::string getDocumentProcessorName() const override { return PROCESSOR_NAME; }

private:
    struct ClusterHashMeta {
        std::string clusterName;
        std::vector<autil::HashFunctionBasePtr> hashFuncVec;
        std::vector<std::vector<std::string>> hashFieldVec;

        const autil::HashFunctionBasePtr& getHashFunc(regionid_t regionId) const { return hashFuncVec[regionId]; }

        const std::vector<std::string>& getHashField(regionid_t regionId) const { return hashFieldVec[regionId]; }

        void appendHashInfo(autil::HashFunctionBasePtr hashFunc, const std::vector<std::string>& hashField)
        {
            hashFuncVec.push_back(hashFunc);
            hashFieldVec.push_back(hashField);
        }
    };

private:
    bool createClusterHashMeta(const config::HashModeConfig& hashModeConfig, ClusterHashMeta& clusterHashMeta);
    bool addHashInfo(const config::HashModeConfig& hashModeConfig, const std::string& regionName,
                     ClusterHashMeta& clusterHashMeta);

private:
    indexlib::config::IndexPartitionSchemaPtr _schema;
    std::vector<std::string> _clusterNames;
    std::vector<ClusterHashMeta> _clusterHashMetas;
    indexlib::util::AccumulativeCounterPtr _hashFieldErrorCounter;
    regionid_t _regionCount = 1;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(HashDocumentProcessor);

}} // namespace build_service::processor
