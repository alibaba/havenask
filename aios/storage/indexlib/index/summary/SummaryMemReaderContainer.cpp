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
#include "indexlib/index/summary/SummaryMemReaderContainer.h"

#include "indexlib/index/summary/SummaryMemReader.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SummaryMemReaderContainer);

const std::shared_ptr<SummaryMemReader>&
SummaryMemReaderContainer::GetSummaryMemReader(summarygroupid_t summaryGroupId) const
{
    assert(summaryGroupId >= 0 && summaryGroupId < (summarygroupid_t)_summaryMemReaderVec.size());
    return _summaryMemReaderVec[summaryGroupId];
}

void SummaryMemReaderContainer::AddReader(const std::shared_ptr<SummaryMemReader>& summaryMemReader)
{
    _summaryMemReaderVec.push_back(summaryMemReader);
}

std::pair<Status, bool>
SummaryMemReaderContainer::GetDocument(docid_t docId, indexlib::document::SearchSummaryDocument* summaryDoc) const
{
    for (auto& memReader : _summaryMemReaderVec) {
        if (!memReader) {
            continue;
        }
        auto [status, ret] = memReader->GetDocument(docId, summaryDoc);
        RETURN2_IF_STATUS_ERROR(status, false, "get document from mem reader fail, doc id = [%d]", docId);
        if (!ret) {
            return std::make_pair(Status::OK(), false);
        }
    }
    return std::make_pair(Status::OK(), true);
}

std::pair<Status, bool>
SummaryMemReaderContainer::GetDocument(docid_t docId, const SummaryGroupIdVec& groupVec,
                                       indexlib::document::SearchSummaryDocument* summaryDoc) const
{
    for (auto groupId : groupVec) {
        if (unlikely(groupId < 0 || groupId >= (summarygroupid_t)_summaryMemReaderVec.size())) {
            AUTIL_LOG(WARN, "invalid summary group id [%d], max group id [%d]", groupId,
                      (summarygroupid_t)_summaryMemReaderVec.size());
            return std::make_pair(Status::OK(), false);
        }
        auto memReader = _summaryMemReaderVec[(size_t)groupId];
        if (!memReader) {
            continue;
        }
        auto [status, ret] = memReader->GetDocument(docId, summaryDoc);
        RETURN2_IF_STATUS_ERROR(status, false, "get document from mem reader fail, doc id = [%d]", docId);
        if (!ret) {
            return std::make_pair(Status::OK(), false);
        }
    }
    return std::make_pair(Status::OK(), true);
}

} // namespace indexlibv2::index
