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
#include "indexlib/index/inverted_index/truncate/SingleTruncateIndexWriter.h"

#include "autil/StringUtil.h"
#include "indexlib/base/Status.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryTypedFactory.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"
#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"
#include "indexlib/index/inverted_index/truncate/TruncatePostingIterator.h"
#include "indexlib/util/MMapAllocator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, SingleTruncateIndexWriter);

SingleTruncateIndexWriter::SingleTruncateIndexWriter(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
    const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper)
    : _hasNullTerm(false)
    , _allocator(NULL)
    , _byteSlicePool(NULL)
    , _bufferPool(NULL)
    , _sortFieldRef(NULL)
    , _docMapper(docMapper)
    , _desc(false)
{
    assert(indexConfig != nullptr);
    _indexFormatOption.Init(indexConfig);
    _invertedIndexConfig = indexConfig;
}

SingleTruncateIndexWriter::~SingleTruncateIndexWriter()
{
    _metaFileWriter.reset();
    ReleasePoolResource();
}

void SingleTruncateIndexWriter::Init(
    const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegmentMetas)
{
    _targetSegmentMetas = targetSegmentMetas;
}

void SingleTruncateIndexWriter::SetParam(const std::shared_ptr<IEvaluator>& evaluator,
                                         const std::shared_ptr<DocCollector>& collector,
                                         const std::shared_ptr<TruncateTrigger>& truncTrigger,
                                         const std::string& truncateIndexName,
                                         const std::shared_ptr<DocInfoAllocator>& docInfoAllocator,
                                         const file_system::IOConfig& ioConfig)
{
    _evaluator = evaluator;
    _collector = collector;
    _truncateTrigger = truncTrigger;
    _docInfoAllocator = docInfoAllocator;
    _ioConfig = ioConfig;
    _truncateIndexName = truncateIndexName;
    _allocator = new util::MMapAllocator();
    _byteSlicePool = new autil::mem_pool::Pool(_allocator, DEFAULT_CHUNK_SIZE * 1024 * 1024);
    // TODO, if in memory segment use truncate, align size should be 8
    _bufferPool = new autil::mem_pool::RecyclePool(_allocator, DEFAULT_CHUNK_SIZE * 1024 * 1024);
    _postingWriterResource.reset(new PostingWriterResource(&_simplePool, _byteSlicePool, _bufferPool,
                                                           _indexFormatOption.GetPostingFormatOption()));
    _truncateDictKeySet.clear();
    _hasNullTerm = false;
}

void SingleTruncateIndexWriter::SetTruncateIndexMetaInfo(const std::shared_ptr<file_system::FileWriter>& metaFile,
                                                         const std::string& firstDimenSortFieldName, bool desc)
{
    if (!metaFile) {
        return;
    }
    _metaFileWriter = metaFile;

    _sortFieldRef = _docInfoAllocator->GetReference(firstDimenSortFieldName);
    assert(_sortFieldRef != NULL);
    _desc = desc;
}

Status SingleTruncateIndexWriter::AddPosting(const DictKeyInfo& dictKey,
                                             const std::shared_ptr<PostingIterator>& postingIt, df_t docFreq)
{
    auto st = PrepareResource();
    RETURN_IF_STATUS_ERROR(st, "prepare single truncate writer resource failed.");
    if (HasTruncated(dictKey)) {
        return Status::OK();
    }

    _collector->CollectDocIds(dictKey, postingIt, docFreq);
    if (!_collector->Empty()) {
        RETURN_IF_STATUS_ERROR(WriteTruncateMeta(dictKey, postingIt), "writer truncate meta failed.");
        WriteTruncateIndex(dictKey, postingIt);
    }

    ResetResource();
    return Status::OK();
}

bool SingleTruncateIndexWriter::BuildTruncateIndex(const std::shared_ptr<PostingIterator>& postingIt,
                                                   const std::shared_ptr<MultiSegmentPostingWriter>& postingWriter)
{
    const std::shared_ptr<DocCollector::DocIdVector>& docIdVec = _collector->GetTruncateDocIds();
    for (size_t i = 0; i < docIdVec->size(); ++i) {
        docid_t docId = (*docIdVec)[i];
        [[maybe_unused]] docid_t seekedDocId = postingIt->SeekDoc(docId);
        assert(seekedDocId == docId);
        auto targetSegmentId = _docMapper->GetLocalId(docId);
        auto postingWriterImpl = postingWriter->GetSegmentPostingWriterBySegId(targetSegmentId);
        if (postingWriterImpl == nullptr) {
            continue;
        }

        std::shared_ptr<TruncatePostingIterator> truncateIter =
            std::dynamic_pointer_cast<TruncatePostingIterator>(postingIt);
        std::shared_ptr<MultiSegmentPostingIterator> multiIter;
        if (truncateIter != nullptr) {
            multiIter = std::dynamic_pointer_cast<MultiSegmentPostingIterator>(truncateIter->GetCurrentIterator());
        }
        assert(multiIter);
        TermMatchData termMatchData;
        multiIter->Unpack(termMatchData);
        AddPosition(postingWriterImpl, termMatchData, multiIter);
        EndDocument(postingWriterImpl, multiIter, termMatchData, docId);
    }
    postingWriter->EndSegment();
    return true;
}

void SingleTruncateIndexWriter::AddPosition(const std::shared_ptr<PostingWriter>& postingWriter,
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
    } else if (_invertedIndexConfig->GetOptionFlag() & of_position_list) {
        // add fake pos (0) for bitmap truncate
        postingWriter->AddPosition(0, posIter->GetPosPayload(), 0);
    }
    delete posIter;
    termMatchData.FreeInDocPositionState();
}

void SingleTruncateIndexWriter::EndDocument(const std::shared_ptr<PostingWriter>& postingWriter,
                                            const std::shared_ptr<MultiSegmentPostingIterator>& postingIt,
                                            const TermMatchData& termMatchData, docid_t docId)
{
    assert(_invertedIndexConfig);
    if (_invertedIndexConfig->GetOptionFlag() & of_term_payload) {
        postingWriter->SetTermPayload(postingIt->GetTermPayLoad());
    }
    if (termMatchData.HasFieldMap()) {
        fieldmap_t fieldMap = termMatchData.GetFieldMap();
        postingWriter->EndDocument(docId, postingIt->GetDocPayload(), fieldMap);
    } else {
        postingWriter->EndDocument(docId, postingIt->GetDocPayload());
    }
}

void SingleTruncateIndexWriter::DumpPosting(const DictKeyInfo& dictKey,
                                            const std::shared_ptr<PostingIterator>& postingIt,
                                            const std::shared_ptr<MultiSegmentPostingWriter>& postingWriter)
{
    auto truncateIter = std::dynamic_pointer_cast<TruncatePostingIterator>(postingIt);
    std::shared_ptr<MultiSegmentPostingIterator> multiIter;
    if (truncateIter) {
        multiIter = std::dynamic_pointer_cast<MultiSegmentPostingIterator>(truncateIter->GetCurrentIterator());
    }
    assert(multiIter != nullptr);
    postingWriter->Dump(dictKey, _outputSegmentResources, multiIter->GetTermPayLoad());
}

void SingleTruncateIndexWriter::SaveDictKey(const DictKeyInfo& dictKey)
{
    if (dictKey.IsNull()) {
        _hasNullTerm = true;
    } else {
        _truncateDictKeySet.insert(dictKey.GetKey());
    }
}

std::shared_ptr<MultiSegmentPostingWriter> SingleTruncateIndexWriter::CreatePostingWriter(InvertedIndexType indexType)
{
    std::shared_ptr<MultiSegmentPostingWriter> writer;
    switch (indexType) {
    case it_primarykey64:
    case it_primarykey128:
    case it_trie:
        break;
    default:
        PostingFormatOption formatOption = _indexFormatOption.GetPostingFormatOption();
        writer.reset(new MultiSegmentPostingWriter(_postingWriterResource.get(), _targetSegmentMetas, formatOption));
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
    return _truncateTrigger->NeedTruncate(info);
}

Status SingleTruncateIndexWriter::PrepareResource()
{
    if (_outputSegmentResources.size() > 0) {
        return Status::OK();
    }
    _outputSegmentResources.reserve(_targetSegmentMetas.size());
    for (const auto& segmentMeta : _targetSegmentMetas) {
        auto indexDir = segmentMeta->segmentDir->GetDirectory(indexlib::index::INVERTED_INDEX_PATH,
                                                              /*throwExceptionIfNotExist*/ false);
        indexDir->RemoveDirectory(_truncateIndexName, file_system::RemoveOption::MayNonExist());
        const std::shared_ptr<file_system::Directory>& truncateIndexDir = indexDir->MakeDirectory(_truncateIndexName);
        auto [st, statistics] = segmentMeta->segmentInfo->GetSegmentStatistics();
        auto segmentStatistics = std::make_shared<indexlibv2::framework::SegmentStatistics>(statistics);
        RETURN_IF_STATUS_ERROR(st, "get segment statistics failed.");
        auto indexOutputSegmentResource = std::make_shared<IndexOutputSegmentResource>();
        indexOutputSegmentResource->Init(truncateIndexDir, _invertedIndexConfig, _ioConfig, segmentStatistics,
                                         &_simplePool,
                                         /*hasAdaptiveBitMap*/ false);
        _outputSegmentResources.push_back(indexOutputSegmentResource);
        std::string optionString = IndexFormatOption::ToString(_indexFormatOption);
        truncateIndexDir->Store(INDEX_FORMAT_OPTION_FILE_NAME, optionString);
    }
    return Status::OK();
}

bool SingleTruncateIndexWriter::HasTruncated(const DictKeyInfo& dictKey)
{
    if (dictKey.IsNull()) {
        return _hasNullTerm;
    }
    return _truncateDictKeySet.find(dictKey.GetKey()) != _truncateDictKeySet.end();
}

void SingleTruncateIndexWriter::WriteTruncateIndex(const DictKeyInfo& dictKey,
                                                   const std::shared_ptr<PostingIterator>& postingIt)
{
    std::shared_ptr<MultiSegmentPostingWriter> postingWriter =
        CreatePostingWriter(_invertedIndexConfig->GetInvertedIndexType());
    postingIt->Reset();

    if (BuildTruncateIndex(postingIt, postingWriter)) {
        DumpPosting(dictKey, postingIt, postingWriter);
        SaveDictKey(dictKey);
    }

    AUTIL_LOG(DEBUG, "index writer allocator use bytes : %luM", _allocator->getUsedBytes() / (1024 * 1024));
    postingWriter.reset();
}

Status SingleTruncateIndexWriter::WriteTruncateMeta(const DictKeyInfo& dictKey,
                                                    const std::shared_ptr<PostingIterator>& postingIt)
{
    if (!_metaFileWriter) {
        return Status::OK();
    }

    std::string metaValue;
    GenerateMetaValue(postingIt, dictKey, metaValue);
    if (!metaValue.empty()) {
        auto [st, _] = _metaFileWriter->Write(metaValue.c_str(), metaValue.size()).StatusWith();
        RETURN_IF_STATUS_ERROR(st, "write truncate meta failed.");
    }
    return Status::OK();
}

void SingleTruncateIndexWriter::GenerateMetaValue(const std::shared_ptr<PostingIterator>& postingIt,
                                                  const DictKeyInfo& dictKey, std::string& metaValue)
{
    std::string value;
    AcquireLastDocValue(postingIt, value);
    std::string key = dictKey.ToString();
    metaValue = key + "\t" + value + "\n";
}

void SingleTruncateIndexWriter::AcquireLastDocValue(const std::shared_ptr<PostingIterator>& postingIt,
                                                    std::string& value)
{
    docid_t docId = _collector->GetMinValueDocId();
    DocInfo* docInfo = _docInfoAllocator->Allocate();
    docInfo->SetDocId(docId);
    postingIt->Reset();
    postingIt->SeekDoc(docId);
    _evaluator->Evaluate(docId, postingIt, docInfo);
    value = _sortFieldRef->GetStringValue(docInfo);
    int64_t int64Value = 0;
    if (!autil::StringUtil::fromString(value, int64Value)) {
        double doubleValue = 0;
        if (autil::StringUtil::fromString(value, doubleValue)) {
            if (_desc) {
                value = autil::StringUtil::toString(floor(doubleValue));
            } else {
                value = autil::StringUtil::toString(ceil(doubleValue));
            }
        }
    }
}

void SingleTruncateIndexWriter::ResetResource()
{
    _bufferPool->reset();
    _byteSlicePool->reset();
    _collector->Reset();
}

void SingleTruncateIndexWriter::ReleasePoolResource()
{
    if (_byteSlicePool) {
        _byteSlicePool->release();
        delete _byteSlicePool;
        _byteSlicePool = NULL;
    }
    if (_bufferPool) {
        _bufferPool->release();
        delete _bufferPool;
        _bufferPool = NULL;
    }
    if (_allocator) {
        delete _allocator;
        _allocator = NULL;
    }
}

void SingleTruncateIndexWriter::ReleaseMetaResource()
{
    if (_docInfoAllocator) {
        _docInfoAllocator->Release();
    }
    if (_metaFileWriter) {
        _metaFileWriter->Close().GetOrThrow();
        _metaFileWriter.reset();
    }
}

void SingleTruncateIndexWriter::ReleaseTruncateIndexResource()
{
    for (auto& indexOutputSegmentResource : _outputSegmentResources) {
        indexOutputSegmentResource->Reset();
    }

    _outputSegmentResources.clear();

    _truncateDictKeySet.clear();
    _hasNullTerm = false;

    if (_collector) {
        _collector.reset();
        AUTIL_LOG(DEBUG, "clear collector");
    }
}

int64_t SingleTruncateIndexWriter::EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount,
                                                     size_t outputSegmentCount) const
{
    int64_t size = sizeof(*this);
    size += maxPostingLen;                                                             // for build truncate index
    int64_t writeBufferSize = _ioConfig.GetWriteBufferSize() * 2 * outputSegmentCount; // dict and data
    AUTIL_LOG(INFO, "SingleTruncateIndexWriter: write buffer [%ld MB]", writeBufferSize / 1024 / 1024);
    size += writeBufferSize;

    int64_t collectorMemUse = _collector->EstimateMemoryUse(totalDocCount);
    AUTIL_LOG(INFO, "SingleTruncateIndexWriter: collectorMemUse [%ld MB]", collectorMemUse / 1024 / 1024);
    size += collectorMemUse;

    return size;
}
} // namespace indexlib::index
