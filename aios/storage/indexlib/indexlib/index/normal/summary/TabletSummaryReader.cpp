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
#include "indexlib/index/normal/summary/TabletSummaryReader.h"

#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index/summary/SummaryReader.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, TabletSummaryReader);

TabletSummaryReader::TabletSummaryReader(indexlibv2::index::SummaryReader* summaryReader,
                                         const std::shared_ptr<indexlib::index::PrimaryKeyIndexReader>& pkIndexReader)
    : indexlib::index::SummaryReader(nullptr)
    , _summaryReader(summaryReader)
{
    mPKIndexReader = pkIndexReader;
}

future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
TabletSummaryReader::GetDocument(const std::vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                                 autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption option,
                                 const indexlib::index::SearchSummaryDocVec* docs) const noexcept
{
    co_return co_await _summaryReader->GetDocument(docIds, groupVec, sessionPool, option,
                                                   (indexlibv2::index::SummaryReader::SearchSummaryDocVec*)docs);
}

future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
TabletSummaryReader::GetDocument(const std::vector<docid_t>& docIds, autil::mem_pool::Pool* sessionPool,
                                 indexlib::file_system::ReadOption option,
                                 const indexlib::index::SearchSummaryDocVec* docs) const noexcept
{
    co_return co_await _summaryReader->GetDocument(docIds, sessionPool, option,
                                                   (indexlibv2::index::SummaryReader::SearchSummaryDocVec*)docs);
}

bool TabletSummaryReader::GetDocument(docid_t docId, indexlib::document::SearchSummaryDocument* summaryDoc) const
{
    auto [status, ret] = _summaryReader->GetDocument(docId, summaryDoc);
    THROW_IF_STATUS_ERROR(status);
    return ret;
}

bool TabletSummaryReader::GetDocument(docid_t docId, indexlib::document::SearchSummaryDocument* summaryDoc,
                                      const SummaryGroupIdVec& groupVec) const
{
    auto [status, ret] = _summaryReader->GetDocument(docId, groupVec, summaryDoc);
    THROW_IF_STATUS_ERROR(status);
    return ret;
}

void TabletSummaryReader::AddAttrReader(fieldid_t fieldId,
                                        const std::shared_ptr<indexlib::index::AttributeReader>& attrReader)
{
    AUTIL_LOG(ERROR, "can not add legacy attribute reader interface into tablet summary reader");
    INDEXLIB_THROW(indexlib::util::RuntimeException,
                   "can not add legacy attribute reader interface into tablet summary reader");
}

void TabletSummaryReader::AddAttrReader(fieldid_t fieldId, AttributeReader* attrReader)
{
    _summaryReader->AddAttrReader(fieldId, attrReader);
}

void TabletSummaryReader::SetPrimaryKeyReader(indexlib::index::PrimaryKeyIndexReader* pkIndexReader)
{
    _summaryReader->SetPrimaryKeyReader(pkIndexReader);
    mPKIndexReader = std::shared_ptr<indexlib::index::PrimaryKeyIndexReader>(pkIndexReader);
}

} // namespace indexlibv2::index
