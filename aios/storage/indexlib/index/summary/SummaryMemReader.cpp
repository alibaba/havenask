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
#include "indexlib/index/summary/SummaryMemReader.h"

#include "indexlib/document/normal/SummaryGroupFormatter.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SummaryMemReader);

SummaryMemReader::SummaryMemReader(const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig,
                                   VarLenDataAccessor* accessor)
    : _summaryGroupConfig(summaryGroupConfig)
    , _accessor(accessor)
{
}

SummaryMemReader::~SummaryMemReader() {}

std::pair<Status, bool> SummaryMemReader::GetDocument(docid_t localDocId,
                                                      indexlib::document::SearchSummaryDocument* summaryDoc) const
{
    if ((uint64_t)localDocId >= _accessor->GetDocCount()) {
        return std::make_pair(Status::OK(), false);
    }

    uint8_t* value = NULL;
    uint32_t size = 0;
    _accessor->ReadData(localDocId, value, size);

    // TODO: ADD UT
    if (size == 0) {
        return std::make_pair(Status::OK(), false);
    }

    indexlib::document::SummaryGroupFormatter formatter(_summaryGroupConfig);
    if (formatter.DeserializeSummary(summaryDoc, (char*)value, (size_t)size)) {
        return std::make_pair(Status::OK(), true);
    }
    return std::make_pair(Status::InternalError("Deserialize in mem summary[docid = %d] FAILED.", localDocId), false);
}

} // namespace indexlibv2::index
