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
#include "indexlib/index/normal/summary/local_disk_summary_reader.h"

#include <assert.h>
#include <fcntl.h>
#include <sstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "autil/TimeoutTerminator.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/summary/in_mem_summary_segment_reader_container.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::document;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace index {

IE_LOG_SETUP(index, LocalDiskSummaryReader);

LocalDiskSummaryReader::LocalDiskSummaryReader(const SummarySchemaPtr& summarySchema, summarygroupid_t summaryGroupId)
    : SummaryReader(summarySchema)
    , mSummaryGroupConfig(summarySchema->GetSummaryGroupConfig(summaryGroupId))
    , mBuildingBaseDocId(INVALID_DOCID)
{
    assert(mSummaryGroupConfig);
}

LocalDiskSummaryReader::~LocalDiskSummaryReader() {}

bool LocalDiskSummaryReader::Open(const PartitionDataPtr& partitionData, const PrimaryKeyIndexReaderPtr& pkIndexReader,
                                  const SummaryReader* hintReader)
{
    IE_LOG(DEBUG, "Begin opening summary");
    mPKIndexReader = pkIndexReader;

    PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    SegmentIteratorPtr builtSegIter = segIter->CreateIterator(SIT_BUILT);
    assert(builtSegIter);
    unordered_map<segmentid_t, LocalDiskSummarySegmentReaderPtr> hintReaderMap;
    auto typedHintReader = dynamic_cast<const LocalDiskSummaryReader*>(hintReader);
    if (typedHintReader) {
        for (size_t i = 0; i < typedHintReader->mReaderSegmentIds.size(); ++i) {
            segmentid_t segId = typedHintReader->mReaderSegmentIds[i];
            hintReaderMap[segId] = typedHintReader->mSegmentReaders[i];
        }
    }

    while (builtSegIter->IsValid()) {
        const SegmentData& segData = builtSegIter->GetSegmentData();
        const std::shared_ptr<const SegmentInfo>& segmentInfo = segData.GetSegmentInfo();
        try {
            if (segmentInfo->docCount == 0) {
                builtSegIter->MoveToNext();
                continue;
            }
            segmentid_t segId = segData.GetSegmentId();
            auto iter = hintReaderMap.find(segId);
            if (iter != hintReaderMap.end()) {
                mSegmentReaders.push_back(iter->second);
            } else if (!LoadSegmentReader(segData)) {
                return false;
            }
            mSegmentDocCount.push_back((uint64_t)segmentInfo->docCount);
            mReaderSegmentIds.push_back(segId);
            builtSegIter->MoveToNext();
        } catch (const ExceptionBase& e) {
            const file_system::DirectoryPtr& directory = segData.GetDirectory();
            IE_LOG(ERROR, "LoadSegment[%s] failed,  exception:%s", directory->DebugString().c_str(), e.what());
            throw;
        }
    }

    mBuildingBaseDocId = segIter->GetBuildingBaseDocId();
    SegmentIteratorPtr buildingSegIter = segIter->CreateIterator(SIT_BUILDING);
    InitBuildingSummaryReader(buildingSegIter);
    IE_LOG(DEBUG, "End opening summary");
    return true;
}

bool LocalDiskSummaryReader::GetDocument(docid_t docId, SearchSummaryDocument* summaryDoc) const
{
    if (GetDocumentFromSummary(docId, summaryDoc)) {
        return GetDocumentFromAttributes(docId, summaryDoc);
    }

    return false;
}

future_lite::coro::Lazy<vector<index::ErrorCode>>
LocalDiskSummaryReader::GetDocumentFromSummaryAsync(const std::vector<docid_t>& docIds, Pool* sessionPool,
                                                    file_system::ReadOption readOption,
                                                    const SearchSummaryDocVec* docs) const noexcept
{
    if (readOption.timeoutTerminator && readOption.timeoutTerminator->checkRestrictTimeout()) {
        co_return vector<index::ErrorCode>(docIds.size(), index::ErrorCode::Timeout);
    }

    vector<index::ErrorCode> ret(docIds.size(), index::ErrorCode::OK);
    // get summary failed if any docid is invalid
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (docIds[i] < 0) {
            vector<index::ErrorCode> invalidResult(docIds.size(), index::ErrorCode::Runtime);
            co_return invalidResult;
        }
    }

    if (!mSummaryGroupConfig->NeedStoreSummary()) {
        co_return ret;
    }

    // fill result for built segment
    auto segmentResults = co_await GetBuiltSegmentTasks(docIds, sessionPool, readOption, docs);
    size_t docIdx = 0;
    for (size_t i = 0; i < segmentResults.size(); ++i) {
        assert(!segmentResults[i].hasError());
        auto segmentResult = segmentResults[i].value();
        for (auto& ec : segmentResult) {
            ret[docIdx++] = ec;
        }
    }

    // fill result for building segment
    while (docIdx < docIds.size()) {
        docid_t docId = docIds[docIdx];
        assert(docId >= mBuildingBaseDocId);
        if (!mBuildingSummaryReader || !mBuildingSummaryReader->GetDocument(docId, (*docs)[docIdx])) {
            ret[docIdx] = index::ErrorCode::BadParameter;
        }
        docIdx++;
    }
    co_return ret;
}

future_lite::coro::Lazy<vector<future_lite::Try<vector<index::ErrorCode>>>>
LocalDiskSummaryReader::GetBuiltSegmentTasks(const vector<docid_t>& docIds, Pool* sessionPool,
                                             file_system::ReadOption readOption,
                                             const SearchSummaryDocVec* docs) const noexcept
{
    vector<future_lite::coro::Lazy<vector<index::ErrorCode>>> segmentTasks;

    // get value from built segment async

    docid_t currentSegDocIdEnd = 0;
    size_t docIdIdx = 0;

    deque<std::vector<docid_t>> segmentDocIds;
    deque<SearchSummaryDocVec> segmentDocs;
    for (uint32_t i = 0; i < mSegmentDocCount.size() && docIdIdx < docIds.size(); ++i) {
        std::vector<docid_t> currentSegmentDocId;
        SearchSummaryDocVec currentSegmentDocs;
        docid_t baseDocId = currentSegDocIdEnd;
        currentSegDocIdEnd += mSegmentDocCount[i];
        while (docIdIdx < docIds.size() && docIds[docIdIdx] < currentSegDocIdEnd) {
            currentSegmentDocId.push_back(docIds[docIdIdx] - baseDocId);
            currentSegmentDocs.push_back((*docs)[docIdIdx]);
            docIdIdx++;
        }
        if (!currentSegmentDocId.empty()) {
            size_t idx = segmentDocIds.size();
            segmentDocIds.push_back(move(currentSegmentDocId));
            segmentDocs.push_back(move(currentSegmentDocs));
            segmentTasks.push_back(
                mSegmentReaders[i]->GetDocument(segmentDocIds[idx], sessionPool, readOption, &segmentDocs[idx]));
        }
    }
    co_return co_await future_lite::coro::collectAll(move(segmentTasks));
}

future_lite::coro::Lazy<vector<index::ErrorCode>>
LocalDiskSummaryReader::GetDocumentAsync(const std::vector<docid_t>& docIds, Pool* sessionPool,
                                         file_system::ReadOption readOption,
                                         const SearchSummaryDocVec* docs) const noexcept
{
    if (readOption.timeoutTerminator && readOption.timeoutTerminator->checkRestrictTimeout()) {
        co_return vector<index::ErrorCode>(docIds.size(), index::ErrorCode::Timeout);
    }
    // TODO(makuo.mnb) support pack attribute
    if (!mPackAttrReaders.empty()) {
        IE_LOG(ERROR, "summary batch read does not support reuse of pack attribute");
        co_return vector<index::ErrorCode>(docIds.size(), index::ErrorCode::Runtime);
    }
    vector<index::ErrorCode> result(docIds.size(), index::ErrorCode::OK);
    vector<future_lite::coro::Lazy<vector<index::ErrorCode>>> subTasks;
    subTasks.reserve(mAttrReaders.size() + 1);
    subTasks.push_back(GetDocumentFromSummaryAsync(docIds, sessionPool, readOption, docs));
    vector<vector<string>> attributeValues(mAttrReaders.size()), packAttributeValues(mPackAttrReaders.size());

    vector<AttributeIteratorBase*> attributeIterators;
    attributeIterators.reserve(mAttrReaders.size() + mPackAttrReaders.size());
    for (size_t i = 0; i < mAttrReaders.size(); ++i) {
        attributeValues[i].resize(docIds.size());
        auto iter = mAttrReaders[i].second->CreateIterator(sessionPool);
        attributeIterators.push_back(iter);
        subTasks.push_back(iter->BatchSeek(docIds, readOption, &attributeValues[i]));
    }

    auto taskResult = co_await future_lite::coro::collectAll(move(subTasks));

    for (size_t docIdx = 0; docIdx < docIds.size(); ++docIdx) {
        assert(!taskResult[0].hasError());
        vector<index::ErrorCode> summaryEcs = taskResult[0].value();
        if (summaryEcs[docIdx] != index::ErrorCode::OK) {
            result[docIdx] = summaryEcs[docIdx];
        }
    }
    size_t fieldIdx = 1;

#define FILL_ATTRIBUTE_HELPER(readers, values)                                                                         \
    do {                                                                                                               \
        for (size_t i = 0; i < readers.size(); ++i, ++fieldIdx) {                                                      \
            assert(!taskResult[fieldIdx].hasError());                                                                  \
            auto& attributeEc = taskResult[fieldIdx].value();                                                          \
            for (size_t docIdx = 0; docIdx < docIds.size(); ++docIdx) {                                                \
                if (result[docIdx] != index::ErrorCode::OK) {                                                          \
                    continue;                                                                                          \
                }                                                                                                      \
                                                                                                                       \
                if (attributeEc[docIdx] != index::ErrorCode::OK) {                                                     \
                    result[docIdx] = attributeEc[docIdx];                                                              \
                } else if (!SetSummaryDocField((*docs)[docIdx], readers[i].first, values[i][docIdx])) {                \
                    result[docIdx] = index::ErrorCode::Runtime;                                                        \
                }                                                                                                      \
            }                                                                                                          \
        }                                                                                                              \
    } while (0);

    FILL_ATTRIBUTE_HELPER(mAttrReaders, attributeValues);
    FILL_ATTRIBUTE_HELPER(mPackAttrReaders, packAttributeValues);
#undef FILL_ATTRIBUTE_HELPER
    for (auto iter : attributeIterators) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter);
    }
    co_return result;
}

bool LocalDiskSummaryReader::GetDocumentFromSummary(docid_t docId, document::SearchSummaryDocument* summaryDoc) const
{
    if (docId < 0) {
        return false;
    }

    if (!mSummaryGroupConfig->NeedStoreSummary()) {
        return true;
    }

    if (docId >= mBuildingBaseDocId) {
        return mBuildingSummaryReader && mBuildingSummaryReader->GetDocument(docId, summaryDoc);
    }
    docid_t baseDocId = 0;
    for (uint32_t i = 0; i < mSegmentDocCount.size(); i++) {
        if (docId < baseDocId + (docid_t)mSegmentDocCount[i]) {
            return mSegmentReaders[i]->GetDocument(docId - baseDocId, summaryDoc);
        }
        baseDocId += mSegmentDocCount[i];
    }
    return false;
}

bool LocalDiskSummaryReader::SetSummaryDocField(SearchSummaryDocument* summaryDoc, fieldid_t fieldId,
                                                const string& value) const
{
    assert(summaryDoc);
    int32_t summaryFieldId = mSummarySchema->GetSummaryFieldId(fieldId);
    assert(summaryFieldId != -1);
    return summaryDoc->SetFieldValue(summaryFieldId, value.data(), value.length());
}

bool LocalDiskSummaryReader::GetDocumentFromAttributes(docid_t docId, SearchSummaryDocument* summaryDoc) const
{
    Pool* pool = dynamic_cast<Pool*>(summaryDoc->getPool());
    string attrValue;
    for (size_t i = 0; i < mAttrReaders.size(); ++i) {
        if (!(mAttrReaders[i].second)->Read(docId, attrValue, pool)) {
            return false;
        }
        if (!SetSummaryDocField(summaryDoc, mAttrReaders[i].first, attrValue)) {
            return false;
        }
    }
    for (size_t i = 0; i < mPackAttrReaders.size(); ++i) {
        std::shared_ptr<SummaryConfig> summaryConfig = mSummarySchema->GetSummaryConfig(mPackAttrReaders[i].first);
        assert(summaryConfig);
        const string& fieldName = summaryConfig->GetSummaryName();
        if (!(mPackAttrReaders[i].second)->Read(docId, fieldName, attrValue, pool)) {
            return false;
        }
        if (!SetSummaryDocField(summaryDoc, mPackAttrReaders[i].first, attrValue)) {
            return false;
        }
    }
    return true;
}

void LocalDiskSummaryReader::AddAttrReader(fieldid_t fieldId, const AttributeReaderPtr& attrReader)
{
    if (attrReader) {
        mAttrReaders.push_back(make_pair(fieldId, attrReader));
    }
}

void LocalDiskSummaryReader::AddPackAttrReader(fieldid_t fieldId, const PackAttributeReaderPtr& attrReader)
{
    if (attrReader) {
        mPackAttrReaders.push_back(make_pair(fieldId, attrReader));
    }
}

bool LocalDiskSummaryReader::LoadSegmentReader(const SegmentData& segmentData)
{
    if (!mSummaryGroupConfig->NeedStoreSummary()) {
        mSegmentReaders.push_back(LocalDiskSummarySegmentReaderPtr());
        return true;
    }

    LocalDiskSummarySegmentReaderPtr onDiskSegReader(new LocalDiskSummarySegmentReader(mSummaryGroupConfig));

    if (!onDiskSegReader->Open(segmentData)) {
        return false;
    }
    mSegmentReaders.push_back(onDiskSegReader);
    return true;
}

void LocalDiskSummaryReader::InitBuildingSummaryReader(const SegmentIteratorPtr& buildingIter)
{
    if (!mSummaryGroupConfig->NeedStoreSummary() || !buildingIter) {
        return;
    }

    while (buildingIter->IsValid()) {
        index_base::InMemorySegmentPtr inMemorySegment = buildingIter->GetInMemSegment();
        index_base::InMemorySegmentReaderPtr segmentReader = inMemorySegment->GetSegmentReader();
        SummarySegmentReaderPtr summarySegReader = segmentReader->GetSummaryReader();
        assert(summarySegReader);
        InMemSummarySegmentReaderContainerPtr inMemSummarySegmentReaderContainer =
            DYNAMIC_POINTER_CAST(InMemSummarySegmentReaderContainer, summarySegReader);
        assert(inMemSummarySegmentReaderContainer);

        InMemSummarySegmentReaderPtr inMemSegReader =
            inMemSummarySegmentReaderContainer->GetInMemSummarySegmentReader(mSummaryGroupConfig->GetGroupId());
        if (!mBuildingSummaryReader) {
            mBuildingSummaryReader.reset(new BuildingSummaryReader);
        }
        mBuildingSummaryReader->AddSegmentReader(buildingIter->GetBaseDocId(), inMemSegReader);
        buildingIter->MoveToNext();
    }
}
}} // namespace indexlib::index
