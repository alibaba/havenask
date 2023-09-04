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
#include "indexlib/index/merger_util/truncate/single_truncate_index_writer.h"

#include "autil/ConstStringUtil.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryTypedFactory.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"
#include "indexlib/index/merger_util/truncate/truncate_posting_iterator.h"
#include "indexlib/util/MMapAllocator.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlibv2::index;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, SingleTruncateIndexWriter);

SingleTruncateIndexWriter::SingleTruncateIndexWriter(const config::IndexConfigPtr& indexConfig,
                                                     const ReclaimMapPtr& reclaimMap)
    : mIndexConfig(indexConfig)
    , mHasNullTerm(false)
    , mAllocator(NULL)
    , mByteSlicePool(NULL)
    , mBufferPool(NULL)
    , mSortFieldRef(NULL)
    , mReclaimMap(reclaimMap)
    , mDesc(false)
{
    mIndexFormatOption.Init(indexConfig);
}

SingleTruncateIndexWriter::~SingleTruncateIndexWriter()
{
    mMetaFileWriter.reset();
    // mFileWriter.reset();
    ReleasePoolResource();
}

void SingleTruncateIndexWriter::Init(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    mIndexOutputSegmentMergeInfos = outputSegmentMergeInfos;
}

void SingleTruncateIndexWriter::Init(const EvaluatorPtr& evaluator, const DocCollectorPtr& collector,
                                     const TruncateTriggerPtr& truncTrigger, const string& truncateIndexName,
                                     const DocInfoAllocatorPtr& docInfoAllocator, const config::MergeIOConfig& ioConfig)
{
    mEvaluator = evaluator;
    mCollector = collector;
    mTruncTrigger = truncTrigger;
    mDocInfoAllocator = docInfoAllocator;
    mIOConfig = ioConfig;
    mTruncateIndexName = truncateIndexName;
    mAllocator = new MMapAllocator();
    mByteSlicePool = new Pool(mAllocator, DEFAULT_CHUNK_SIZE * 1024 * 1024);
    // TODO, if in memory segment use truncate, align size should be 8
    mBufferPool = new RecyclePool(mAllocator, DEFAULT_CHUNK_SIZE * 1024 * 1024);
    mPostingWriterResource.reset(new PostingWriterResource(&mSimplePool, mByteSlicePool, mBufferPool,
                                                           mIndexFormatOption.GetPostingFormatOption()));
    mTruncateDictKeySet.clear();
    mHasNullTerm = false;
}

void SingleTruncateIndexWriter::SetTruncateIndexMetaInfo(const file_system::FileWriterPtr& metaFile,
                                                         const string& firstDimenSortFieldName, bool desc)
{
    if (!metaFile) {
        return;
    }
    mMetaFileWriter = metaFile;

    mSortFieldRef = mDocInfoAllocator->GetReference(firstDimenSortFieldName);
    assert(mSortFieldRef != NULL);
    mDesc = desc;
}

void SingleTruncateIndexWriter::SetReTruncateInfo(EvaluatorPtr reTruncateEvaluator,
                                                  std::unique_ptr<DocCollector> reTruncateCollector, bool desc)
{
    mReTruncateEvaluator = std::move(reTruncateEvaluator);
    mReTruncateCollector = std::move(reTruncateCollector);
    mDesc = desc;
}

void SingleTruncateIndexWriter::AddPosting(const index::DictKeyInfo& dictKey,
                                           const std::shared_ptr<PostingIterator>& postingIt, df_t docFreq)
{
    PrepareResource();
    if (HasTruncated(dictKey)) {
        return;
    }

    mCollector->CollectDocIds(dictKey, postingIt, docFreq);
    if (!mCollector->Empty()) {
        std::string metaValue = WriteTruncateMeta(dictKey, postingIt);
        if (mReTruncateCollector) {
            mCollector->Reset(); // release memory
            assert(!metaValue.empty());
            metaValue = metaValue.substr(0, metaValue.size() - 1); // trim last '\n'
            std::vector<std::string> strVec = autil::StringUtil::split(metaValue, "\t");
            assert(strVec.size() == 2);
            int64_t value = 0;
            int64_t otherValue = 0;
            if (!autil::StringUtil::fromString(strVec[1], value)) {
                std::cerr << "LENG: " << strVec[1].size() << std::endl;
                INDEXLIB_FATAL_ERROR(IndexCollapsed, "invalid value [%s]", strVec[1].c_str());
            }
            postingIt->Reset();
            if (mDesc) {
                otherValue = std::numeric_limits<int64_t>::max();
                mReTruncateCollector->CollectDocIds(dictKey, postingIt, docFreq, &value, &otherValue);
            } else {
                otherValue = std::numeric_limits<int64_t>::min();
                mReTruncateCollector->CollectDocIds(dictKey, postingIt, docFreq, &otherValue, &value);
            }
            WriteTruncateIndex(*mReTruncateCollector, dictKey, postingIt);
        } else {
            WriteTruncateIndex(*mCollector, dictKey, postingIt);
        }
    }

    ResetResource();
}

bool SingleTruncateIndexWriter::BuildTruncateIndex(
    const DocCollector& collector, const std::shared_ptr<PostingIterator>& postingIt,
    const std::shared_ptr<legacy::MultiSegmentPostingWriter>& postingWriter)
{
    const DocCollector::DocIdVectorPtr& docIdVec = collector.GetTruncateDocIds();
    for (size_t i = 0; i < docIdVec->size(); ++i) {
        docid_t docId = (*docIdVec)[i];
        docid_t seekedDocId = postingIt->SeekDoc(docId);
        assert(seekedDocId == docId);
        (void)seekedDocId;
        pair<segmentindex_t, docid_t> docidInfo = mReclaimMap->GetLocalId(seekedDocId);
        auto postingWriterImpl = postingWriter->GetSegmentPostingWriter(docidInfo.first);

        TruncatePostingIteratorPtr truncateIter = DYNAMIC_POINTER_CAST(TruncatePostingIterator, postingIt);
        std::shared_ptr<MultiSegmentPostingIterator> multiIter;
        if (truncateIter) {
            multiIter = DYNAMIC_POINTER_CAST(MultiSegmentPostingIterator, truncateIter->GetCurrentIterator());
        }
        assert(multiIter);
        TermMatchData termMatchData;
        multiIter->Unpack(termMatchData);
        AddPosition(postingWriterImpl, termMatchData, multiIter);
        EndDocument(postingWriterImpl, multiIter, termMatchData, docidInfo.second);
    }
    postingWriter->EndSegment();
    return true;
}

void SingleTruncateIndexWriter::AddPosition(const shared_ptr<PostingWriter>& postingWriter,
                                            TermMatchData& termMatchData,
                                            const std::shared_ptr<PostingIterator>& postingIt)
{
    InDocPositionState* inDocPosState = termMatchData.GetInDocPositionState();
    if (inDocPosState == NULL) {
        return;
    }
    InDocPositionIterator* posIter = inDocPosState->CreateInDocIterator();
    if (postingIt->HasPosition()) {
        pos_t pos = 0;
        while ((pos = posIter->SeekPosition(pos)) != INVALID_POSITION) {
            postingWriter->AddPosition(pos, posIter->GetPosPayload(), 0);
        }
    } else if (mIndexConfig->GetOptionFlag() & of_position_list) {
        // add fake pos (0) for bitmap truncate
        postingWriter->AddPosition(0, posIter->GetPosPayload(), 0);
    }
    delete posIter;
    termMatchData.FreeInDocPositionState();
}

void SingleTruncateIndexWriter::EndDocument(const shared_ptr<PostingWriter>& postingWriter,
                                            const std::shared_ptr<MultiSegmentPostingIterator>& postingIt,
                                            const TermMatchData& termMatchData, docid_t docId)
{
    assert(mIndexConfig);
    if (mIndexConfig->GetOptionFlag() & of_term_payload) {
        postingWriter->SetTermPayload(postingIt->GetTermPayLoad());
    }
    if (termMatchData.HasFieldMap()) {
        fieldmap_t fieldMap = termMatchData.GetFieldMap();
        postingWriter->EndDocument(docId, postingIt->GetDocPayload(), fieldMap);
    } else {
        postingWriter->EndDocument(docId, postingIt->GetDocPayload());
    }
}

void SingleTruncateIndexWriter::DumpPosting(const index::DictKeyInfo& dictKey,
                                            const std::shared_ptr<PostingIterator>& postingIt,
                                            const std::shared_ptr<legacy::MultiSegmentPostingWriter>& postingWriter)
{
    TruncatePostingIteratorPtr truncateIter = DYNAMIC_POINTER_CAST(TruncatePostingIterator, postingIt);
    std::shared_ptr<MultiSegmentPostingIterator> multiIter;
    if (truncateIter) {
        multiIter = DYNAMIC_POINTER_CAST(MultiSegmentPostingIterator, truncateIter->GetCurrentIterator());
    }
    assert(multiIter);
    postingWriter->Dump(dictKey, mOutputSegmentResources, multiIter->GetTermPayLoad());
}

void SingleTruncateIndexWriter::SaveDictKey(const index::DictKeyInfo& dictKey, int64_t fileOffset)
{
    if (dictKey.IsNull()) {
        mHasNullTerm = true;
    } else {
        mTruncateDictKeySet.insert(dictKey.GetKey());
    }
}

std::shared_ptr<legacy::MultiSegmentPostingWriter>
SingleTruncateIndexWriter::CreatePostingWriter(InvertedIndexType indexType)
{
    std::shared_ptr<legacy::MultiSegmentPostingWriter> writer;
    switch (indexType) {
    case it_primarykey64:
    case it_primarykey128:
    case it_trie:
        break;
    default:
        PostingFormatOption formatOption = mIndexFormatOption.GetPostingFormatOption();
        writer.reset(new legacy::MultiSegmentPostingWriter(mPostingWriterResource.get(), mIndexOutputSegmentMergeInfos,
                                                           formatOption));
        break;
    }
    return writer;
}

void SingleTruncateIndexWriter::EndPosting()
{
    ReleaseMetaResource();
    ReleaseTruncateIndexResource();
    ReleasePoolResource();
}

bool SingleTruncateIndexWriter::NeedTruncate(const TruncateTriggerInfo& info) const
{
    return mTruncTrigger->NeedTruncate(info);
}

void SingleTruncateIndexWriter::PrepareResource()
{
    if (mOutputSegmentResources.size() > 0) {
        return;
    }
    mOutputSegmentResources.reserve(mIndexOutputSegmentMergeInfos.size());
    bool needCreateBitmapIndex = mIndexConfig->GetHighFreqVocabulary() != nullptr;
    for (size_t i = 0; i < mIndexOutputSegmentMergeInfos.size(); ++i) {
        const file_system::DirectoryPtr& indexDir = mIndexOutputSegmentMergeInfos[i].directory;
        indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        indexDir->RemoveDirectory(mTruncateIndexName, removeOption);
        const file_system::DirectoryPtr& truncateIndexDir = indexDir->MakeDirectory(mTruncateIndexName);
        IndexOutputSegmentResourcePtr indexOutputSegmentResource(new IndexOutputSegmentResource);
        indexOutputSegmentResource->Init(truncateIndexDir, mIndexConfig, mIOConfig,
                                         mIndexOutputSegmentMergeInfos[i].temperatureLayer, &mSimplePool,
                                         needCreateBitmapIndex);
        mOutputSegmentResources.push_back(indexOutputSegmentResource);
        string optionString = LegacyIndexFormatOption::ToString(mIndexFormatOption);
        truncateIndexDir->Store(INDEX_FORMAT_OPTION_FILE_NAME, optionString);
    }
}

bool SingleTruncateIndexWriter::HasTruncated(const index::DictKeyInfo& dictKey)
{
    if (dictKey.IsNull()) {
        return mHasNullTerm;
    }
    return mTruncateDictKeySet.find(dictKey.GetKey()) != mTruncateDictKeySet.end();
}

void SingleTruncateIndexWriter::WriteTruncateIndex(const DocCollector& collector, const index::DictKeyInfo& dictKey,
                                                   const std::shared_ptr<PostingIterator>& postingIt)
{
    std::shared_ptr<legacy::MultiSegmentPostingWriter> postingWriter =
        CreatePostingWriter(mIndexConfig->GetInvertedIndexType());
    postingIt->Reset();

    if (BuildTruncateIndex(collector, postingIt, postingWriter)) {
        // int64_t offset = mFileWriter->GetLength();
        int64_t offset = 0;
        DumpPosting(dictKey, postingIt, postingWriter);
        SaveDictKey(dictKey, offset);
    }

    IE_LOG(DEBUG, "index writer allocator use bytes : %luM", mAllocator->getUsedBytes() / (1024 * 1024));
    postingWriter.reset();
}

std::string SingleTruncateIndexWriter::WriteTruncateMeta(const index::DictKeyInfo& dictKey,
                                                         const std::shared_ptr<PostingIterator>& postingIt)
{
    if (!mMetaFileWriter) {
        return "";
    }

    string metaValue;
    GenerateMetaValue(postingIt, dictKey, metaValue);
    if (!metaValue.empty()) {
        mMetaFileWriter->Write(metaValue.c_str(), metaValue.size()).GetOrThrow();
    }
    return metaValue;
}

void SingleTruncateIndexWriter::GenerateMetaValue(const std::shared_ptr<PostingIterator>& postingIt,
                                                  const index::DictKeyInfo& dictKey, string& metaValue)
{
    string value;
    AcquireLastDocValue(postingIt, value);
    string key = dictKey.ToString();
    metaValue = key + "\t" + value + "\n";
    IE_LOG(DEBUG, "truncate meta: %s", metaValue.c_str());
}

void SingleTruncateIndexWriter::AcquireLastDocValue(const PostingIteratorPtr& postingIt, string& value)
{
    docid_t docId = mCollector->GetMinValueDocId();
    // assert(docId != INVALID_DOCID);
    DocInfo* docInfo = mDocInfoAllocator->Allocate();
    docInfo->SetDocId(docId);
    postingIt->Reset();
    postingIt->SeekDoc(docId);
    mEvaluator->Evaluate(docId, postingIt, docInfo);
    value = mSortFieldRef->GetStringValue(docInfo);
    int64_t int64Value = 0;
    if (!autil::StringUtil::fromString(value, int64Value)) {
        double doubleValue = 0;
        if (autil::StringUtil::fromString(value, doubleValue)) {
            if (mDesc) {
                value = autil::StringUtil::toString(floor(doubleValue));
            } else {
                value = autil::StringUtil::toString(ceil(doubleValue));
            }
        }
    }
}

void SingleTruncateIndexWriter::ResetResource()
{
    mBufferPool->reset();
    mByteSlicePool->reset();
    mCollector->Reset();
    if (mReTruncateCollector) {
        mReTruncateCollector->Reset();
    }
}

void SingleTruncateIndexWriter::ReleasePoolResource()
{
    if (mByteSlicePool) {
        mByteSlicePool->release();
        delete mByteSlicePool;
        mByteSlicePool = NULL;
    }
    if (mBufferPool) {
        mBufferPool->release();
        delete mBufferPool;
        mBufferPool = NULL;
    }
    if (mAllocator) {
        delete mAllocator;
        mAllocator = NULL;
    }
}

void SingleTruncateIndexWriter::ReleaseMetaResource()
{
    if (mDocInfoAllocator) {
        mDocInfoAllocator->Release();
    }
    if (mMetaFileWriter) {
        mMetaFileWriter->Close().GetOrThrow();
        mMetaFileWriter.reset();
    }
}

void SingleTruncateIndexWriter::ReleaseTruncateIndexResource()
{
    for (auto& indexOutputSegmentResource : mOutputSegmentResources) {
        indexOutputSegmentResource->Reset();
    }

    mOutputSegmentResources.clear();

    mTruncateDictKeySet.clear();
    mHasNullTerm = false;

    if (mCollector) {
        mCollector.reset();
        IE_LOG(DEBUG, "clear collector");
    }
}

int64_t SingleTruncateIndexWriter::EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount,
                                                     size_t outputSegmentCount) const
{
    int64_t size = sizeof(*this);
    size += maxPostingLen;                                                             // for build truncate index
    int64_t writeBufferSize = mIOConfig.GetWriteBufferSize() * 2 * outputSegmentCount; // dict and data
    IE_LOG(INFO, "SingleTruncateIndexWriter: write buffer [%ld MB]", writeBufferSize / 1024 / 1024);
    size += writeBufferSize;

    int64_t collectorMemUse = mCollector->EstimateMemoryUse(totalDocCount);
    IE_LOG(INFO, "SingleTruncateIndexWriter: collectorMemUse [%ld MB]", collectorMemUse / 1024 / 1024);
    size += collectorMemUse;

    return size;
}
} // namespace indexlib::index::legacy
