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

#include "build_service/common_define.h"
#include "build_service/document/ExtendDocument.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/kv/Types.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace processor {

class RegionDocumentProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;

public:
    RegionDocumentProcessor();
    ~RegionDocumentProcessor();

public:
    bool init(const DocProcessorInitParam& param) override;
    bool process(const document::ExtendDocumentPtr& document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs) override;

public:
    void destroy() override { delete this; }
    DocumentProcessor* clone() override { return new RegionDocumentProcessor(*this); }
    std::string getDocumentProcessorName() const override { return PROCESSOR_NAME; }

private:
    regionid_t _specificRegionId;
    std::string _regionFieldName;
    indexlib::config::IndexPartitionSchemaPtr _schema;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RegionDocumentProcessor);

}} // namespace build_service::processor
