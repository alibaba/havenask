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
#include <string>
#include <utility>
#include <vector>

#include "autil/StringView.h"
#include "build_service/common_define.h"
#include "build_service/document/RawDocument.h"
#include "build_service/processor/ProcessorMetricReporter.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_schema.h"

namespace build_service { namespace processor {

class DeleteSubRawDocRewriter
{
public:
    DeleteSubRawDocRewriter() = delete;
    ~DeleteSubRawDocRewriter() = delete;

    DeleteSubRawDocRewriter(const DeleteSubRawDocRewriter&) = delete;
    DeleteSubRawDocRewriter& operator=(const DeleteSubRawDocRewriter&) = delete;
    DeleteSubRawDocRewriter(DeleteSubRawDocRewriter&&) = delete;
    DeleteSubRawDocRewriter& operator=(DeleteSubRawDocRewriter&&) = delete;

public:
    static std::vector<document::RawDocumentPtr> rewrite(const indexlib::config::IndexPartitionSchemaPtr& schema,
                                                         const document::RawDocumentPtr& rawDoc,
                                                         ProcessorMetricReporter* reporter);

private:
    static std::pair<bool, std::vector<std::string>>
    GetOldSubPks(const std::string& subPkFieldName, const std::vector<autil::StringView>& modifiedFieldsVec,
                 const std::vector<autil::StringView>& modifiedValuesVec);
    static std::set<std::string> GetNewSubPks(const std::string& subPkFieldName,
                                              const document::RawDocumentPtr& rawDoc);

    static std::pair<bool, std::vector<document::RawDocumentPtr>>
    splitToDeleteSubAndAddDoc(const indexlib::config::IndexPartitionSchemaPtr& subSchema,
                              const document::RawDocumentPtr& rawDoc,
                              const std::vector<autil::StringView>& modifiedFieldsVec,
                              const std::vector<autil::StringView>& modifiedValuesVec,
                              const std::vector<std::string>& oldSubPks, const std::vector<std::string>& deleteSubPks);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DeleteSubRawDocRewriter);

}} // namespace build_service::processor
