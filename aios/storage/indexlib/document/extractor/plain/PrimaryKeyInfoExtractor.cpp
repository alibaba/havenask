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
#include "indexlib/document/extractor/plain/PrimaryKeyInfoExtractor.h"

#include <memory>

#include "indexlib/document/IDocument.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"

using namespace std;
using namespace indexlibv2::document;
using namespace indexlibv2::document::extractor;

namespace indexlibv2::plain {
AUTIL_LOG_SETUP(indexlib.document, PrimaryKeyInfoExtractor);

PrimaryKeyInfoExtractor::PrimaryKeyInfoExtractor(const std::shared_ptr<config::IIndexConfig>& indexConfig)
    : _indexConfig(indexConfig)
{
}

PrimaryKeyInfoExtractor::~PrimaryKeyInfoExtractor() {}

Status PrimaryKeyInfoExtractor::ExtractField(IDocument* doc, void** field)
{
    auto normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(doc);
    if (!normalDoc) {
        AUTIL_LOG(ERROR, "can not extract primary key information from the given document.");
        return Status::Unknown("can not extract primary key information from the given document.");
    }
    const auto& indexDoc = normalDoc->GetIndexDocument();
    const std::string& keyStr = indexDoc->GetPrimaryKey();
    *field = (void*)(&keyStr);
    return Status::OK();
}

bool PrimaryKeyInfoExtractor::IsValidDocument(IDocument* doc)
{
    indexlibv2::document::NormalDocument* normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(doc);
    if (!normalDoc) {
        return false;
    }
    auto indexDoc = normalDoc->GetIndexDocument();
    if (!indexDoc) {
        return false;
    }
    if (!normalDoc->HasPrimaryKey()) {
        AUTIL_LOG(WARN, "AddDocument fail: Doc has no primary key");
        return false;
    }
    const std::string& keyStr = normalDoc->GetPrimaryKey();

    auto primaryKeyIndexConfig = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(_indexConfig);
    if (_indexConfig && !primaryKeyIndexConfig) {
        AUTIL_LOG(ERROR, "indexConfig must be PrimaryKeyIndexConfig");
        return false;
    }
    InvertedIndexType indexType = primaryKeyIndexConfig->GetInvertedIndexType();

    auto fieldConfig = primaryKeyIndexConfig->GetFieldConfig();
    auto fieldType = fieldConfig->GetFieldType();
    auto primaryKeyHashType = primaryKeyIndexConfig->GetPrimaryKeyHashType();

    bool isValid = indexlib::index::KeyHasherWrapper::IsOriginalKeyValid(fieldType, primaryKeyHashType, keyStr.c_str(),
                                                                         keyStr.size(), indexType == it_primarykey64);
    if (!isValid) {
        AUTIL_LOG(WARN, "AddDocument fail: Doc primary key [%s] is not valid", keyStr.c_str());
        return false;
    }
    return true;
}

} // namespace indexlibv2::plain
