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
#include "indexlib/index/primary_key/PrimaryKeyIndexFieldsParser.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexFields.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PrimaryKeyIndexFieldsParser);

PrimaryKeyIndexFieldsParser::PrimaryKeyIndexFieldsParser() {}

PrimaryKeyIndexFieldsParser::~PrimaryKeyIndexFieldsParser() {}

Status PrimaryKeyIndexFieldsParser::Init(const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs,
                                         const std::shared_ptr<document::DocumentInitParam>& initParam)
{
    if (indexConfigs.size() != 1) {
        RETURN_STATUS_ERROR(InvalidArgs, "only support one primary key index but has [%lu]", indexConfigs.size());
    }
    auto indexConfig = indexConfigs[0];
    auto pkConfig = std::dynamic_pointer_cast<PrimaryKeyIndexConfig>(indexConfig);
    if (!pkConfig) {
        RETURN_STATUS_ERROR(Status::InvalidArgs, "cast to pk config failed");
    }
    auto fieldConfigs = pkConfig->GetFieldConfigs();
    if (fieldConfigs.size() != 1) {
        RETURN_STATUS_ERROR(InvalidArgs, "only support one primary key field but has  [%lu]", fieldConfigs.size());
    }
    auto fieldConfig = fieldConfigs[0];
    assert(fieldConfig);
    _pkFieldName = fieldConfig->GetFieldName();
    return Status::OK();
}

indexlib::util::PooledUniquePtr<document::IIndexFields>
PrimaryKeyIndexFieldsParser::Parse(const document::ExtendDocument& extendDoc, autil::mem_pool::Pool* pool,
                                   bool& hasFormatError) const
{
    hasFormatError = false;
    auto rawDoc = extendDoc.GetRawDocument();
    assert(rawDoc);
    if (!rawDoc) {
        return {};
    }
    auto pkFields = indexlib::util::MakePooledUniquePtr<PrimaryKeyIndexFields>(pool);
    pkFields->SetPrimaryKey(rawDoc->getField(_pkFieldName));
    return pkFields;
}
indexlib::util::PooledUniquePtr<document::IIndexFields>
PrimaryKeyIndexFieldsParser::Parse(autil::StringView serializedData, autil::mem_pool::Pool* pool) const
{
    auto pkFields = indexlib::util::MakePooledUniquePtr<PrimaryKeyIndexFields>(pool);
    autil::DataBuffer dataBuffer(const_cast<char*>(serializedData.data()), serializedData.size(), pool);
    try {
        pkFields->deserialize(dataBuffer);
    } catch (autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "deserialize pk fields failed: [%s]", e.what());
        return nullptr;
    }
    return pkFields;
}

} // namespace indexlibv2::index
