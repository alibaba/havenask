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
#include "indexlib/index/normal/summary/summary_reader_impl.h"

#include "autil/TimeoutTerminator.h"
#include "autil/memory.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/index/normal/summary/local_disk_summary_reader.h"
#include "indexlib/util/Exception.h"

using namespace std;

using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::index_base;
using indexlib::index::ErrorCode;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SummaryReaderImpl);

SummaryReaderImpl::SummaryReaderImpl(const SummarySchemaPtr& summarySchema)
    : SummaryReader(summarySchema)
    , mExecutor(nullptr)
{
}

SummaryReaderImpl::~SummaryReaderImpl() {}

bool SummaryReaderImpl::Open(const PartitionDataPtr& partitionData,
                             const std::shared_ptr<PrimaryKeyIndexReader>& pkIndexReader,
                             const SummaryReader* hintReader)
{
    if (!mSummarySchema) {
        IE_LOG(ERROR, "summary schema is empty");
        return false;
    }

    for (summarygroupid_t groupId = 0; groupId < mSummarySchema->GetSummaryGroupConfigCount(); ++groupId) {
        const SummaryGroupConfigPtr& summaryGroupConfig = mSummarySchema->GetSummaryGroupConfig(groupId);
        LocalDiskSummaryReaderPtr readerImpl(new LocalDiskSummaryReader(mSummarySchema, groupId));
        auto typedReader = dynamic_cast<const SummaryReaderImpl*>(hintReader);
        auto hintGroupReader = GET_IF_NOT_NULL(typedReader, mSummaryGroups[groupId].get());
        if (!readerImpl->Open(partitionData, pkIndexReader, hintGroupReader)) {
            IE_LOG(ERROR, "open summary group reader[%s] failed", summaryGroupConfig->GetGroupName().c_str());
            return false;
        }
        mSummaryGroups.push_back(readerImpl);
        mAllGroupIds.push_back(groupId);
    }
    mExecutor = util::FutureExecutor::GetInternalExecutor();
    if (!mExecutor) {
        IE_LOG(DEBUG, "internal executor not created, summary use serial mode");
    }
    mPKIndexReader = pkIndexReader;
    return true;
}

void SummaryReaderImpl::AddAttrReader(fieldid_t fieldId, const AttributeReaderPtr& attrReader)
{
    summarygroupid_t groupId = mSummarySchema->FieldIdToSummaryGroupId(fieldId);
    assert(groupId >= 0 && groupId < (summarygroupid_t)mSummaryGroups.size());
    mSummaryGroups[groupId]->AddAttrReader(fieldId, attrReader);
}

void SummaryReaderImpl::AddPackAttrReader(fieldid_t fieldId, const PackAttributeReaderPtr& attrReader)
{
    summarygroupid_t groupId = mSummarySchema->FieldIdToSummaryGroupId(fieldId);
    assert(groupId >= 0 && groupId < (summarygroupid_t)mSummaryGroups.size());
    mSummaryGroups[groupId]->AddPackAttrReader(fieldId, attrReader);
}

future_lite::coro::Lazy<index::ErrorCodeVec>
SummaryReaderImpl::GetDocument(const vector<docid_t>& docIds, autil::mem_pool::Pool* sessionPool,
                               file_system::ReadOption option,
                               const vector<SearchSummaryDocument*>* docs) const noexcept
{
    return GetDocument(docIds, mAllGroupIds, sessionPool, option, docs);
}

future_lite::coro::Lazy<index::ErrorCodeVec>
SummaryReaderImpl::InnerGetDocumentAsync(const vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                                         autil::mem_pool::Pool* sessionPool, file_system::ReadOption option,
                                         const vector<SearchSummaryDocument*>* docs) const noexcept
{
    if (docIds.empty()) {
        co_return index::ErrorCodeVec();
    }
    if (std::is_sorted(docIds.begin(), docIds.end()) && docIds[0] > 0) {
        co_return co_await InnerGetDocumentAsyncOrdered(docIds, groupVec, sessionPool, option, docs);
    }
    index::ErrorCodeVec result(docIds.size());
    std::vector<size_t> sortIndex(docIds.size());
    size_t validCnt = 0;
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (docIds[i] < 0) {
            result[i] = index::ErrorCode::BadParameter;
        } else {
            sortIndex[validCnt++] = i;
        }
    }
    sortIndex.resize(validCnt);
    std::sort(sortIndex.begin(), sortIndex.end(),
              [&docIds](size_t lhs, size_t rhs) { return docIds[lhs] < docIds[rhs]; });
    vector<docid_t> orderedDocIds(validCnt);
    vector<SearchSummaryDocument*> orderedDocs(validCnt);
    for (size_t i = 0; i < validCnt; ++i) {
        size_t idx = sortIndex[i];
        orderedDocIds[i] = docIds[idx];
        orderedDocs[i] = (*docs)[idx];
    }
    auto orderedResult =
        co_await InnerGetDocumentAsyncOrdered(orderedDocIds, groupVec, sessionPool, option, &orderedDocs);
    for (size_t i = 0; i < validCnt; ++i) {
        result[sortIndex[i]] = orderedResult[i];
    }
    co_return result;
}

future_lite::coro::Lazy<index::ErrorCodeVec>
SummaryReaderImpl::InnerGetDocumentAsyncOrdered(const vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                                                autil::mem_pool::Pool* sessionPool, file_system::ReadOption option,
                                                const vector<SearchSummaryDocument*>* docs) const noexcept
{
    vector<ErrorCode> ec;
    for (size_t i = 0; i < groupVec.size(); ++i) {
        if (unlikely(groupVec[i] < 0 || groupVec[i] >= (summarygroupid_t)mSummaryGroups.size())) {
            IE_LOG(WARN, "invalid summary group id [%d], max group id [%d]", groupVec[i],
                   (summarygroupid_t)mSummaryGroups.size());
            co_return vector<ErrorCode>(docIds.size(), ErrorCode::Runtime);
        }
    }
    vector<future_lite::coro::Lazy<vector<index::ErrorCode>>> tasks;
    for (size_t i = 0; i < groupVec.size(); ++i) {
        tasks.push_back(mSummaryGroups[groupVec[i]]->GetDocumentAsync(docIds, sessionPool, option, docs));
    }
    auto taskResults = co_await future_lite::coro::collectAll(std::move(tasks));
    for (size_t i = 0; i < docIds.size(); ++i) {
        ec.push_back(ErrorCode::OK);
    }
    for (auto& taskResult : taskResults) {
        assert(!taskResult.hasError());
        auto groupResult = taskResult.value();
        for (size_t docIdx = 0; docIdx < groupResult.size(); ++docIdx) {
            if (groupResult[docIdx] != ErrorCode::OK) {
                ec[docIdx] = groupResult[docIdx];
            }
        }
    }
    co_return ec;
}

future_lite::coro::Lazy<index::ErrorCodeVec>
SummaryReaderImpl::GetDocument(const vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                               autil::mem_pool::Pool* sessionPool, file_system::ReadOption option,
                               const vector<SearchSummaryDocument*>* docs) const noexcept
{
    if (!sessionPool) {
        IE_LOG(ERROR, "GetDocument fail, sessionPool is nullptr");
        co_return index::ErrorCodeVec(docIds.size(), index::ErrorCode::BadParameter);
    }

    assert(docIds.size() == docs->size());
    index::ErrorCodeVec ec;
    auto executor = co_await future_lite::CurrentExecutor();
    if (mExecutor) {
        ec = co_await InnerGetDocumentAsync(docIds, groupVec, sessionPool, option, docs).via(mExecutor);
    } else if (executor) {
        ec = co_await InnerGetDocumentAsync(docIds, groupVec, sessionPool, option, docs);
    } else {
        size_t i = 0;
        for (; i < docIds.size(); ++i) {
            if (option.timeoutTerminator && option.timeoutTerminator->checkRestrictTimeout()) {
                break;
            }
            ec.push_back(GetDocument(docIds[i], (*docs)[i], groupVec) ? ErrorCode::OK : ErrorCode::BadParameter);
        }
        for (; i < docIds.size(); ++i) {
            ec.push_back(ErrorCode::Timeout);
        }
    }
    co_return ec;
}

bool SummaryReaderImpl::GetDocument(docid_t docId, SearchSummaryDocument* summaryDoc,
                                    const SummaryGroupIdVec& groupVec) const
{
    try {
        return DoGetDocument(docId, summaryDoc, groupVec);
    } catch (const util::ExceptionBase& e) {
        IE_LOG(ERROR, "GetDocument exception: %s", e.what());
    } catch (const std::exception& e) {
        IE_LOG(ERROR, "GetDocument exception: %s", e.what());
    } catch (...) {
        IE_LOG(ERROR, "GetDocument exception");
    }
    return false;
}

bool SummaryReaderImpl::DoGetDocument(docid_t docId, SearchSummaryDocument* summaryDoc,
                                      const SummaryGroupIdVec& groupVec) const
{
    for (size_t i = 0; i < groupVec.size(); ++i) {
        if (unlikely(groupVec[i] < 0 || groupVec[i] >= (summarygroupid_t)mSummaryGroups.size())) {
            IE_LOG(WARN, "invalid summary group id [%d], max group id [%d]", groupVec[i],
                   (summarygroupid_t)mSummaryGroups.size());
            return false;
        }
        if (!mSummaryGroups[groupVec[i]]->GetDocument(docId, summaryDoc)) {
            return false;
        }
    }
    return true;
}

bool SummaryReaderImpl::GetDocumentByPkStr(const std::string& pkStr, document::SearchSummaryDocument* summaryDoc) const
{
    AssertPkIndexExist();
    docid_t docId = mPKIndexReader->Lookup(pkStr);
    return GetDocument(docId, summaryDoc);
}

}} // namespace indexlib::index
