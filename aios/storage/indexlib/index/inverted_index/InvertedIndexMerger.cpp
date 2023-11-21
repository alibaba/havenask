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
#include "indexlib/index/inverted_index/InvertedIndexMerger.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/relocatable/RelocatableFolder.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/common/PlainDocMapper.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/IndexTermExtender.h"
#include "indexlib/index/inverted_index/OneDocMerger.h"
#include "indexlib/index/inverted_index/PostingMerger.h"
#include "indexlib/index/inverted_index/PostingMergerImpl.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/SegmentTermInfoQueue.h"
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/AdaptiveBitmapIndexWriterCreator.h"
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/MultiAdaptiveBitmapIndexWriter.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingMerger.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/config/TruncateOptionConfig.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/index/inverted_index/patch/InvertedIndexDedupPatchFileMerger.h"
#include "indexlib/index/inverted_index/truncate/BucketMap.h"
#include "indexlib/index/inverted_index/truncate/BucketMapCreator.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReaderCreator.h"
#include "indexlib/index/inverted_index/truncate/TruncateIndexWriterCreator.h"
#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::InvertedIndexConfig;
using indexlibv2::framework::IndexTaskResourceManager;
using indexlibv2::framework::SegmentInfo;
using indexlibv2::framework::SegmentMeta;
using indexlibv2::framework::SegmentStatistics;
using indexlibv2::index::DocMapper;
using indexlibv2::index::IIndexMerger;
using indexlibv2::index::PlainDocMapper;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, InvertedIndexMerger);

InvertedIndexMerger::~InvertedIndexMerger()
{
    _byteSlicePool.reset();
    _bufferPool.reset();
    _allocator.reset();
}

Status InvertedIndexMerger::Init(const std::shared_ptr<IIndexConfig>& indexConfig,
                                 const std::map<std::string, std::any>& params)
{
    RETURN_IF_STATUS_ERROR(InitWithoutParam(indexConfig), "InvertedIndexMerger init failed");
    auto iter = params.find(DocMapper::GetDocMapperType());
    if (iter == params.end()) {
        AUTIL_LOG(ERROR, "not find doc mapper name by type [%s]", DocMapper::GetDocMapperType().c_str());
        return Status::Corruption("not find doc mapper name by type");
    }
    _docMapperName = std::any_cast<std::string>(iter->second);

    iter = params.find(OPTIMIZE_MERGE);
    if (iter == params.end() ||
        !autil::StringUtil::fromString(std::any_cast<std::string>(iter->second), _isOptimizeMerge)) {
        _isOptimizeMerge = false;
    }
    _params = params;
    return Status::OK();
}

Status InvertedIndexMerger::InitWithoutParam(const std::shared_ptr<IIndexConfig>& indexConfig)
{
    _indexName = indexConfig->GetIndexName();
    _indexConfig = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
    if (_indexConfig == nullptr) {
        AUTIL_LOG(ERROR, "InvertedIndexConfig failed to dynamic cast to OrcConfig [%s][%s]",
                  indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str());
        return Status::InvalidArgs();
    }
    _indexFormatOption.Init(_indexConfig);

    assert(!_postingFormat);
    _postingFormat = std::make_unique<PostingFormat>(_indexFormatOption.GetPostingFormatOption());

    assert(!_allocator);
    _allocator = std::make_unique<util::MMapAllocator>();

    assert(!_byteSlicePool);
    assert(!_bufferPool);
    _byteSlicePool = std::make_unique<autil::mem_pool::Pool>(_allocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024);
    _bufferPool = std::make_unique<autil::mem_pool::RecyclePool>(_allocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024);
    _postingWriterResource = std::make_shared<PostingWriterResource>(
        &_simplePool, _byteSlicePool.get(), _bufferPool.get(), _indexFormatOption.GetPostingFormatOption());
    return Status::OK();
}

Status InvertedIndexMerger::Merge(const SegmentMergeInfos& segMergeInfos,
                                  const std::shared_ptr<IndexTaskResourceManager>& taskResourceManager)
{
    // TODO(makuo.mnb) remove try catch, use iDirectory
    try {
        return DoMerge(segMergeInfos, taskResourceManager);
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "inverted index merge failed, indexName[%s] exception[%s]", _indexName.c_str(), e.what());
    } catch (...) {
        AUTIL_LOG(ERROR, "inverted index merge failed, indexName[%s] unknown exception", _indexName.c_str());
    }
    return Status::IOError("merge inverted inedx failed");
}

Status InvertedIndexMerger::DoMerge(const SegmentMergeInfos& segMergeInfos,
                                    const std::shared_ptr<IndexTaskResourceManager>& taskResourceManager)
{
    if (segMergeInfos.targetSegments.size() == 0) {
        AUTIL_LOG(ERROR, "target segments size is empty, cannot merge");
        return Status::InvalidArgs();
    }
    if (_indexConfig->GetShardingType() == InvertedIndexConfig::IST_NEED_SHARDING) {
        return Status::OK();
    }
    std::shared_ptr<DocMapper> docMapper;
    uint64_t docCountLimit = MAX_SEGMENT_DOC_COUNT;
    if (_docMapperName == PlainDocMapper::GetDocMapperName()) {
        docMapper.reset(new PlainDocMapper(segMergeInfos));
        for (const auto& targetSegment : segMergeInfos.targetSegments) {
            if (docMapper->GetTargetSegmentDocCount(targetSegment->segmentId) > docCountLimit) {
                AUTIL_LOG(ERROR, "inverted index does not support merged segment doc count [%lu], which > [%lu]",
                          docMapper->GetTargetSegmentDocCount(targetSegment->segmentId), docCountLimit);
                return Status::InvalidArgs();
            }
        }
    } else {
        auto status = taskResourceManager->LoadResource(_docMapperName, DocMapper::GetDocMapperType(), docMapper);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "load doc mapper fail");
            return status;
        }
    }

    RETURN_IF_STATUS_ERROR(
        InitAdaptiveBitmapIndexWriter(segMergeInfos.relocatableGlobalRoot, docMapper, segMergeInfos.targetSegments),
        "%s", "init adaptive bitmap index writer failed");
    RETURN_IF_STATUS_ERROR(InitTruncateIndexWriter(segMergeInfos, docMapper, taskResourceManager),
                           "init trunacate index writer failed.");
    PrepareIndexOutputSegmentResource(segMergeInfos.srcSegments, segMergeInfos.targetSegments);
    // TODO(yonghao.fyh) : confirm bitmap writer and truncate writer
    if (_adaptiveBitmapIndexWriter || _truncateIndexWriter) {
        _termExtender.reset(new IndexTermExtender(_indexConfig, _truncateIndexWriter, _adaptiveBitmapIndexWriter));
        _termExtender->Init(segMergeInfos.targetSegments, _indexOutputSegmentResources);
    }

    // Init term queue
    auto onDiskIndexIterCreator = CreateOnDiskIndexIteratorCreator();
    SegmentTermInfoQueue termInfoQueue(_indexConfig, onDiskIndexIterCreator);
    auto status = termInfoQueue.Init(segMergeInfos.srcSegments, _patchInfos);
    RETURN_IF_STATUS_ERROR(status, "init term info queue for index [%s] failed", _indexConfig->GetIndexName().c_str());

    DictKeyInfo key;
    while (!termInfoQueue.Empty()) {
        SegmentTermInfo::TermIndexMode termMode;
        const auto& segTermInfos = termInfoQueue.CurrentTermInfos(key, termMode);
        status = MergeTerm(key, segTermInfos, termMode, docMapper, segMergeInfos.targetSegments);
        RETURN_IF_STATUS_ERROR(status, "merge term failed.");
        termInfoQueue.MoveToNextTerm();
    }
    if (_termExtender) {
        _termExtender->Destroy();
        _termExtender.reset();
    }
    EndMerge();
    status = MergePatches(segMergeInfos);
    RETURN_IF_STATUS_ERROR(status, "merge patches failed");
    return Status::OK();
}

Status InvertedIndexMerger::MergeTerm(DictKeyInfo key, const SegmentTermInfos& segTermInfos,
                                      SegmentTermInfo::TermIndexMode mode, const std::shared_ptr<DocMapper>& docMapper,
                                      const std::vector<std::shared_ptr<SegmentMeta>>& targetSegments)
{
    if (mode == SegmentTermInfo::TM_BITMAP) {
        // no high frequency term in repartition case, drop bitmap posting
        auto vol = _indexConfig->GetHighFreqVocabulary();
        if (!vol) {
            return Status::OK();
        }
        if (!vol->Lookup(key)) {
            return Status::OK();
        }
    }

    std::shared_ptr<PostingMerger> postingMerger = MergeTermPosting(segTermInfos, mode, docMapper, targetSegments);
    df_t df = postingMerger->GetDocFreq();
    if (df <= 0) {
        return Status::OK();
    }
    IndexTermExtender::TermOperation termOperation = IndexTermExtender::TO_REMAIN;
    if (_termExtender) {
        auto [st, operation] = _termExtender->ExtendTerm(key, postingMerger, mode);
        RETURN_IF_STATUS_ERROR(st, "extend term failed.");
        termOperation = operation;
    }
    if (termOperation != IndexTermExtender::TO_DISCARD) {
        postingMerger->Dump(key, _indexOutputSegmentResources);
    }

    _byteSlicePool->reset();
    _bufferPool->reset();
    return Status::OK();
}

std::shared_ptr<PostingMerger>
InvertedIndexMerger::MergeTermPosting(const SegmentTermInfos& segTermInfos, SegmentTermInfo::TermIndexMode mode,
                                      const std::shared_ptr<DocMapper>& docMapper,
                                      const std::vector<std::shared_ptr<SegmentMeta>>& targetSegments)
{
    std::shared_ptr<PostingMerger> postingMerger;
    if (mode == SegmentTermInfo::TM_BITMAP) {
        postingMerger.reset(CreateBitmapPostingMerger(targetSegments));
    } else {
        postingMerger.reset(CreatePostingMerger(targetSegments));
    }
    postingMerger->Merge(segTermInfos, docMapper);
    return postingMerger;
}

void InvertedIndexMerger::EndMerge()
{
    for (auto& indexOutputSegmentResource : _indexOutputSegmentResources) {
        indexOutputSegmentResource->Reset();
    }

    _indexOutputSegmentResources.clear();

    if (_byteSlicePool) {
        _byteSlicePool->release();
    }

    if (_bufferPool) {
        _bufferPool->release();
    }
}

Status InvertedIndexMerger::MergePatches(const SegmentMergeInfos& segmentMergeInfos)
{
    if (_indexConfig->GetShardingType() == InvertedIndexConfig::IST_NEED_SHARDING) {
        return Status::OK();
    }
    if (not _indexConfig->IsIndexUpdatable()) {
        return Status::OK();
    }
    InvertedIndexDedupPatchFileMerger merger(_indexConfig, _patchInfos);
    return merger.Merge(segmentMergeInfos);
}

bool InvertedIndexMerger::NeedPreloadMaxDictCount(size_t targetSegmentCount) const
{
    if (targetSegmentCount > 1) {
        // can not allocate total key count to each output
        return false;
    }

    bool preloadKeyCount = autil::EnvUtil::getEnv("INDEXLIB_PRELOAD_DICTKEY_COUNT", false);
    return preloadKeyCount && _indexConfig->IsHashTypedDictionary();
}

size_t InvertedIndexMerger::GetDictKeyCount(const std::vector<SourceSegment>& srcSegments) const
{
    assert(_indexConfig->GetShardingType() != InvertedIndexConfig::IST_NEED_SHARDING);
    AUTIL_LOG(INFO, "merge index [%s], Begin GetDictKeyCount.", _indexName.c_str());
    std::unordered_set<dictkey_t> dictKeySet;
    for (size_t i = 0; i < srcSegments.size(); i++) {
        const auto& segment = srcSegments[i].segment;
        segmentid_t segId = segment->GetSegmentId();
        uint32_t maxDocCount = segment->GetSegmentInfo()->docCount;
        if (maxDocCount == 0) {
            continue;
        }

        AUTIL_LOG(INFO, "read dictionary file for index [%s] in segment [%d]", _indexName.c_str(), segId);
        std::shared_ptr<file_system::Directory> segDir = segment->GetSegmentDirectory();
        auto indexDirectory = GetIndexDirectory(segDir);
        if (!indexDirectory || !indexDirectory->IsExist(DICTIONARY_FILE_NAME)) {
            continue;
        }

        std::unique_ptr<DictionaryReader> dictReaderPtr(DictionaryCreator::CreateDiskReader(_indexConfig));
        auto status = dictReaderPtr->Open(indexDirectory, DICTIONARY_FILE_NAME, true);
        THROW_IF_STATUS_ERROR(status);
        std::shared_ptr<DictionaryIterator> dictIter = dictReaderPtr->CreateIterator();
        DictKeyInfo key;
        dictvalue_t value;
        while (dictIter->HasNext()) {
            dictIter->Next(key, value);
            if (!key.IsNull()) {
                dictKeySet.insert(key.GetKey());
            }
        }
    }

    AUTIL_LOG(INFO, "merge index [%s], total dictKeyCount [%lu].", _indexName.c_str(), dictKeySet.size());
    return dictKeySet.size();
}

void InvertedIndexMerger::PrepareIndexOutputSegmentResource(
    const std::vector<SourceSegment>& srcSegments, const std::vector<std::shared_ptr<SegmentMeta>>& targetSegments)
{
    size_t dictKeyCount = 0;
    if (NeedPreloadMaxDictCount(targetSegments.size())) {
        dictKeyCount = GetDictKeyCount(srcSegments);
    }
    bool needCreateBitmapIndex = (_indexConfig->GetHighFreqVocabulary() || _adaptiveBitmapIndexWriter != nullptr);
    for (size_t i = 0; i < targetSegments.size(); i++) {
        std::shared_ptr<file_system::Directory> segDir = targetSegments[i]->segmentDir;
        auto subDir = segDir->MakeDirectory(INVERTED_INDEX_PATH);
        // TODO(yonghao.fyh) : support parallel merge dir
        file_system::RemoveOption removeOption = file_system::RemoveOption::MayNonExist();
        subDir->RemoveDirectory(_indexName, /*mayNonExist=*/removeOption);
        auto mergeDir = subDir->MakeDirectory(_indexName);
        std::string optionString = IndexFormatOption::ToString(_indexFormatOption);
        mergeDir->Store(INDEX_FORMAT_OPTION_FILE_NAME, optionString);
        auto outputResource = std::make_shared<IndexOutputSegmentResource>(dictKeyCount);
        auto segmentInfo = targetSegments[i]->segmentInfo;
        assert(segmentInfo);
        auto [status, segmentStatistics] = segmentInfo->GetSegmentStatistics();
        auto segmentStatisticsPtr = std::make_shared<SegmentStatistics>(segmentStatistics);
        if (!status.IsOK()) {
            AUTIL_LOG(WARN, "segment statistics jsonize failed");
            segmentStatisticsPtr = nullptr;
        }
        outputResource->Init(mergeDir, _indexConfig, _ioConfig, /*SegmentStatistics*/ segmentStatisticsPtr,
                             &_simplePool, needCreateBitmapIndex);
        _indexOutputSegmentResources.push_back(outputResource);
    }
}

std::pair<Status, int64_t>
InvertedIndexMerger::GetMaxLengthOfPosting(std::shared_ptr<file_system::Directory> indexDirectory,
                                           const SegmentInfo& segInfo) const
{
    if (!indexDirectory) {
        return std::make_pair(Status::Corruption(), 0);
    }
    DictionaryReader* dictReader = DictionaryCreator::CreateDiskReader(_indexConfig);
    std::unique_ptr<DictionaryReader> dictReaderPtr(dictReader);
    auto status = dictReaderPtr->Open(indexDirectory, DICTIONARY_FILE_NAME, true);
    RETURN2_IF_STATUS_ERROR(status, 0, "dict reader open fail");
    std::shared_ptr<DictionaryIterator> dictIter = dictReader->CreateIterator();

    int64_t lastOffset = 0;
    int64_t maxLen = 0;
    DictKeyInfo key;
    dictvalue_t value;
    while (dictIter->HasNext()) {
        dictIter->Next(key, value);
        int64_t offset = 0;
        if (!ShortListOptimizeUtil::GetOffset(value, offset)) {
            continue;
        }
        maxLen = std::max(maxLen, offset - lastOffset);
        lastOffset = offset;
    }

    size_t indexFileLength = 0;
    std::shared_ptr<file_system::CompressFileInfo> compressInfo =
        indexDirectory->GetCompressFileInfo(POSTING_FILE_NAME);
    if (compressInfo) {
        indexFileLength = compressInfo->deCompressFileLen;
    } else {
        indexFileLength = indexDirectory->GetFileLength(POSTING_FILE_NAME);
    }
    maxLen = std::max(maxLen, (int64_t)indexFileLength - lastOffset);
    return std::make_pair(Status::OK(), maxLen);
}

std::pair<Status, int64_t>
InvertedIndexMerger::GetMaxLengthOfPosting(const std::vector<IIndexMerger::SourceSegment>& srcSegments) const
{
    uint32_t maxDocCount = srcSegments[0].segment->GetSegmentInfo()->docCount;
    uint32_t idx = 0;
    for (uint32_t i = 1; i < srcSegments.size(); i++) {
        const auto& segment = srcSegments[i].segment;
        if (segment->GetSegmentInfo()->docCount > maxDocCount) {
            maxDocCount = segment->GetSegmentInfo()->docCount;
            idx = i;
        }
    }

    if (maxDocCount == 0) {
        return {Status::OK(), 0};
    }
    auto segDir = srcSegments[idx].segment->GetSegmentDirectory();
    auto indexDir = GetIndexDirectory(segDir);
    if (!indexDir) {
        auto segmentSchema = srcSegments[idx].segment->GetSegmentSchema();
        if (segmentSchema && segmentSchema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, _indexName) == nullptr) {
            return std::make_pair(Status::OK(), 0);
        }
        AUTIL_LOG(ERROR, "get max length of posting failed for get index dir [%s, %s] no exist ",
                  segDir->DebugString().c_str(), INVERTED_INDEX_PATH.c_str());
        return std::make_pair(Status::Corruption(), 0);
    }
    return GetMaxLengthOfPosting(indexDir, *(srcSegments[idx].segment->GetSegmentInfo()));
}

std::shared_ptr<file_system::Directory>
InvertedIndexMerger::GetIndexDirectory(const std::shared_ptr<file_system::Directory>& segDir) const
{
    if (!segDir) {
        return nullptr;
    }
    auto subDir = segDir->GetDirectory(INVERTED_INDEX_PATH, /*throwExceptionIfNotExist*/ false);
    if (!subDir) {
        return nullptr;
    }
    auto indexDirectory = subDir->GetDirectory(_indexName, /*throwExceptionIfNotExist*/ false);
    if (!indexDirectory) {
        return nullptr;
    }
    return indexDirectory;
}

int64_t InvertedIndexMerger::GetHashDictMaxMemoryUse(const std::vector<IIndexMerger::SourceSegment>& srcSegments) const
{
    int64_t size = 0;
    if (srcSegments.size() == 1) {
        // buffer read dictionary file, to get max dict count
        return size;
    }

    for (uint32_t i = 0; i < srcSegments.size(); i++) {
        auto segDir = srcSegments[i].segment->GetSegmentDirectory();
        auto indexDirectory = GetIndexDirectory(segDir);
        if (!indexDirectory || !indexDirectory->IsExist(DICTIONARY_FILE_NAME)) {
            continue;
        }
        size += indexDirectory->GetFileLength(DICTIONARY_FILE_NAME);
    }
    return size;
}

PostingMerger*
InvertedIndexMerger::CreateBitmapPostingMerger(const std::vector<std::shared_ptr<SegmentMeta>>& targetSegments)
{
    BitmapPostingMerger* bitmapPostingMerger =
        new BitmapPostingMerger(_byteSlicePool.get(), targetSegments, _indexConfig->GetOptionFlag());
    return bitmapPostingMerger;
}

PostingMerger* InvertedIndexMerger::CreatePostingMerger(const std::vector<std::shared_ptr<SegmentMeta>>& targetSegments)
{
    PostingMergerImpl* postingMergerImpl = new PostingMergerImpl(_postingWriterResource.get(), targetSegments);
    return postingMergerImpl;
}

Status InvertedIndexMerger::InitAdaptiveBitmapIndexWriter(
    const std::shared_ptr<file_system::RelocatableFolder>& relocatableGlobalRoot,
    const std::shared_ptr<DocMapper>& docMapper, const std::vector<std::shared_ptr<SegmentMeta>>& targetSegments)
{
    if (!_isOptimizeMerge || _indexConfig->GetAdaptiveDictionaryConfig() == nullptr ||
        _adaptiveBitmapIndexWriter != nullptr) {
        return Status::OK();
    }

    AUTIL_LOG(INFO, "create adaptive bitmap writer for index [%s]", _indexName.c_str());
    auto adaptiveDir = relocatableGlobalRoot->MakeRelocatableFolder(ADAPTIVE_DICT_DIR_NAME);
    if (!adaptiveDir) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "make relocatable global dir[%s] failed",
                               ADAPTIVE_DICT_DIR_NAME);
    }
    assert(adaptiveDir);
    AdaptiveBitmapIndexWriterCreator creator(adaptiveDir, docMapper, targetSegments);
    _adaptiveBitmapIndexWriter = creator.Create(_indexConfig, _ioConfig);
    return Status::OK();
}

void InvertedIndexMerger::SetPatchInfos(const indexlibv2::index::PatchInfos& patchInfos) { _patchInfos = patchInfos; }
Status InvertedIndexMerger::LoadBucketMaps(
    const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager,
    const std::shared_ptr<indexlibv2::config::TruncateOptionConfig>& truncateOptionConfig,
    std::map<std::string, std::shared_ptr<BucketMap>>& bucketMaps)
{
    // truncate attribute reader creator
    auto iter = _params.find(SEGMENT_MERGE_PLAN_INDEX);
    if (iter == _params.end()) {
        AUTIL_LOG(ERROR, "cannot find merge plan index");
        return Status::InternalError("cannot find merge plan index");
    }
    int64_t segmentMergePlanIdx = -1;
    auto str = std::any_cast<std::string>(iter->second);
    if (!autil::StringUtil::fromString(str, segmentMergePlanIdx)) {
        AUTIL_LOG(ERROR, "cannot convert merge plan idx[%s]", str.c_str());
        return Status::InternalError("cannot convert merge plan idx");
    }
    for (const auto& truncateProfile : truncateOptionConfig->GetTruncateProfiles()) {
        const auto& truncateProfileName = truncateProfile->truncateProfileName;
        const std::string bucketMapName = BucketMapCreator::GetBucketMapName(segmentMergePlanIdx, truncateProfileName);
        std::shared_ptr<index::BucketMap> bucketMap;
        // in case of failover, try load resource first
        auto status = taskResourceManager->LoadResource(bucketMapName, index::BucketMap::GetBucketMapType(), bucketMap);
        if (!status.IsOK() && !status.IsNotFound()) {
            AUTIL_LOG(ERROR, "load bucket map failed.");
            return status;
        }
        if (status.IsOK()) {
            bucketMaps[truncateProfileName] = std::move(bucketMap);
        }
    }
    return Status::OK();
}

Status
InvertedIndexMerger::InitTruncateIndexWriter(const SegmentMergeInfos& segmentMergeInfos,
                                             const std::shared_ptr<DocMapper>& docMapper,
                                             const std::shared_ptr<IndexTaskResourceManager>& taskResourceManager)
{
    if (!_indexConfig->HasTruncate()) {
        return Status::OK();
    }

    AUTIL_LOG(INFO, "create truncate index writer for index [%s]", _indexName.c_str());
    // truncate attribute reader creator
    auto iter = _params.find(TRUNCATE_ATTRIBUTE_READER_CREATOR);
    if (iter == _params.end()) {
        AUTIL_LOG(ERROR, "cannot find truncate attribute reader creator");
        return Status::InternalError("cannot find truncate attribute reader creator");
    }
    auto truncateAttributeReaderCreator = std::any_cast<std::shared_ptr<TruncateAttributeReaderCreator>>(iter->second);
    assert(truncateAttributeReaderCreator != nullptr);

    iter = _params.find(TRUNCATE_OPTION_CONFIG);
    if (iter == _params.end()) {
        AUTIL_LOG(ERROR, "cannot find truncate option config");
        return Status::InternalError("cannot find truncate option config");
    }
    auto truncateOptionConfig = std::any_cast<std::shared_ptr<indexlibv2::config::TruncateOptionConfig>>(iter->second);
    assert(truncateOptionConfig != nullptr);

    iter = _params.find(SOURCE_VERSION_TIMESTAMP);
    if (iter == _params.end()) {
        AUTIL_LOG(ERROR, "cannot find source version timestamp");
        return Status::InternalError("cannot find source version timestamp");
    }
    auto srcVersionTimestamp = std::any_cast<int64_t>(iter->second);

    iter = _params.find(TRUNCATE_META_FILE_READER_CREATOR);
    if (iter == _params.end()) {
        AUTIL_LOG(ERROR, "cannot find truncate meta file reader");
        return Status::InternalError("cannot find truncate meta file reader");
    }

    auto truncateMetaFileReaderCreator = std::any_cast<std::shared_ptr<TruncateMetaFileReaderCreator>>(iter->second);
    assert(truncateMetaFileReaderCreator != nullptr);

    BucketMaps bucketMaps;
    auto status = LoadBucketMaps(taskResourceManager, truncateOptionConfig, bucketMaps);
    RETURN_IF_STATUS_ERROR(status, "load bucket map failed.");

    TruncateIndexWriterCreator creator(segmentMergeInfos, _isOptimizeMerge);
    creator.Init(truncateOptionConfig, docMapper, truncateAttributeReaderCreator, bucketMaps,
                 truncateMetaFileReaderCreator, srcVersionTimestamp);
    _truncateIndexWriter = creator.Create(_indexConfig, _ioConfig);
    return Status::OK();
}

} // namespace indexlib::index
