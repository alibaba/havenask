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
#include "indexlib/document/normal/SummaryGroupFormatter.h"

#include "autil/ConstString.h"
#include "indexlib/document/normal/GroupFieldFormatter.h"
#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/document/normal/SummaryDocument.h"
#include "indexlib/document/normal/SummaryGroupFieldIter.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"
#include "indexlib/util/buffer_compressor/ZlibDefaultCompressor.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace document {
AUTIL_LOG_SETUP(indexlib.document, SummaryGroupFormatter);

SummaryGroupFormatter::SummaryGroupFormatter(
    const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig)
    : _summaryGroupConfig(summaryGroupConfig)
    , _summaryFieldIdBase(summaryGroupConfig->GetSummaryFieldIdBase())
{
    _groupFieldFormatter.reset(new GroupFieldFormatter);
    string compressType = "";
    if (_summaryGroupConfig && _summaryGroupConfig->IsCompress()) {
        compressType = _summaryGroupConfig->GetCompressType();
        if (compressType.empty()) {
            compressType = ZlibDefaultCompressor::COMPRESSOR_NAME;
        }
    }
    _groupFieldFormatter->Init(compressType);
}

SummaryGroupFormatter::SummaryGroupFormatter(const SummaryGroupFormatter& other)
    : SummaryGroupFormatter(other._summaryGroupConfig)
{
}

SummaryGroupFormatter::~SummaryGroupFormatter() {}

std::pair<Status, size_t>
SummaryGroupFormatter::GetSerializeLength(const std::shared_ptr<SummaryDocument>& document) const
{
    SummaryGroupFieldIter iter(document, _summaryGroupConfig);
    return _groupFieldFormatter->GetSerializeLength(iter);
}

Status SummaryGroupFormatter::SerializeSummary(const std::shared_ptr<SummaryDocument>& document, char* value,
                                               size_t length) const
{
    SummaryGroupFieldIter iter(document, _summaryGroupConfig);
    return _groupFieldFormatter->SerializeFields(iter, value, length);
}

bool SummaryGroupFormatter::DeserializeSummary(document::SearchSummaryDocument* document, const char* value,
                                               size_t length) const
{
    assert(document);
    auto [status, iter] = _groupFieldFormatter->Deserialize(value, length, document->getPool());
    if (!iter) {
        return false;
    }
    index::summaryfieldid_t summaryFieldId = _summaryFieldIdBase;
    while (iter->HasNext()) {
        StringView fieldValue = iter->Next();
        if (iter->HasError()) {
            return false;
        }
        document->SetFieldValue(summaryFieldId, fieldValue.data(), fieldValue.length(), false);
        ++summaryFieldId;
    }
    return true;
}
}} // namespace indexlib::document
