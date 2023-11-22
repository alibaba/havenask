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

#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Span.h"
#include "build_service/common_define.h"
#include "build_service/document/RawDocument.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_schema.h"

namespace build_service { namespace processor {

class SubDocumentExtractor
{
public:
    static std::pair<std::vector<autil::StringView>, std::vector<autil::StringView>>
    getModifiedFieldsAndValues(const document::RawDocumentPtr& originalDocument);

public:
    SubDocumentExtractor(const indexlib::config::IndexPartitionSchemaPtr& schema);
    ~SubDocumentExtractor();

public:
    void extractSubDocuments(const document::RawDocumentPtr& originalDocument,
                             std::vector<document::RawDocumentPtr>* subDocuments);

private:
    void addFieldsToSubDocuments(autil::StringView fieldName, const std::vector<std::string>& subFieldValues,
                                 const document::RawDocumentPtr& originalDocument,
                                 std::vector<document::RawDocumentPtr>* subDocuments);
    void addFieldsToSubDocuments(autil::StringView fieldName, autil::StringView fieldValue,
                                 const document::RawDocumentPtr& originalDocument,
                                 std::vector<document::RawDocumentPtr>* subDocuments);
    void getSubModifiedFields(const std::vector<autil::StringView>& modifiedFieldsVec,
                              const std::vector<autil::StringView>& subFieldsVec,
                              std::vector<size_t>* subModifiedFieldsIndices);
    void setModifiedFieldsForSubDocs(const std::vector<autil::StringView>& modifiedFieldsVec,
                                     const std::vector<size_t>& subModifiedFieldsIndices,
                                     std::vector<document::RawDocumentPtr>* subDocuments);
    void setModifiedValuesForSubDocs(const document::RawDocumentPtr& originalDocument,
                                     const std::vector<autil::StringView>& modifiedFieldsVec,
                                     const std::vector<autil::StringView>& modifiedValuesVec,
                                     const std::vector<size_t>& subModifiedFieldsIndices,
                                     std::vector<document::RawDocumentPtr>* subDocuments);

private:
    const indexlib::config::IndexPartitionSchemaPtr _schema;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SubDocumentExtractor);

}} // namespace build_service::processor
