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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/summary/Constant.h"

namespace indexlib::document {
class SearchSummaryDocument;
}

namespace indexlibv2::index {
class SummaryMemReader;

class SummaryMemReaderContainer : private autil::NoCopyable
{
public:
    SummaryMemReaderContainer() = default;
    ~SummaryMemReaderContainer() = default;
    using SummaryGroupIdVec = std::vector<summarygroupid_t>;

public:
    const std::shared_ptr<SummaryMemReader>& GetSummaryMemReader(summarygroupid_t summaryGroupId) const;
    void AddReader(const std::shared_ptr<SummaryMemReader>& summaryMemReader);
    std::pair<Status, bool> GetDocument(docid_t docId, indexlib::document::SearchSummaryDocument* summaryDoc) const;
    std::pair<Status, bool> GetDocument(docid_t docId, const SummaryGroupIdVec& groupVec,
                                        indexlib::document::SearchSummaryDocument* summaryDoc) const;

private:
    std::vector<std::shared_ptr<SummaryMemReader>> _summaryMemReaderVec;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
