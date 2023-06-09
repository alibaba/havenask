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

#include <unordered_map>

#include "build_service/common_define.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"

namespace build_service { namespace processor {

class HashFilterProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;

public:
    HashFilterProcessor();
    ~HashFilterProcessor();

public:
    bool init(const DocProcessorInitParam& param) override;
    bool process(const document::ExtendDocumentPtr& document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs) override;

public:
    void destroy() override;
    DocumentProcessor* clone() override;
    std::string getDocumentProcessorName() const override { return PROCESSOR_NAME; }

private:
    struct ClusterRange {
        uint64_t from = 0;
        uint64_t to = 65536;
    };

private:
    bool FilterByHashId(const std::string& clusterName, hashid_t hashId) const;

private:
    std::vector<std::string> _clusterNames;
    std::unordered_map<std::string, ClusterRange> _clusterRanges;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(HashFilterProcessor);

}} // namespace build_service::processor
