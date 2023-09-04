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
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"

#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/InvertedIndexUtil.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapDiskIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicMemIndexer.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/config/SectionAttributeConfig.h"
#include "indexlib/index/inverted_index/format/dictionary/DefaultTermDictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeIndexFactory.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::index::IndexerParameter;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, InvertedDiskIndexer);

InvertedDiskIndexer::InvertedDiskIndexer(const IndexerParameter& indexerParam) : _indexerParam(indexerParam) {}

Status InvertedDiskIndexer::Open(const std::shared_ptr<IIndexConfig>& indexConfig,
                                 const std::shared_ptr<file_system::IDirectory>& indexDirectory)
{
    if (0 == _indexerParam.docCount) {
        // there is no document here, so do nothing
        AUTIL_LOG(INFO, "doc count is zero in index [%s] for segment [%d], just do nothing",
                  indexConfig->GetIndexName().c_str(), _indexerParam.segmentId);
        return Status::OK();
    }
    try {
        _indexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
        if (_indexConfig == nullptr) {
            AUTIL_LOG(ERROR, "fail to cast to legacy index config, indexName[%s] indexType[%s]",
                      indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str());
            return Status::InvalidArgs("fail to cast to legacy index config");
        }
        _indexFormatOption.Init(_indexConfig);
        auto status = InitDynamicIndexer(indexDirectory);
        RETURN_IF_STATUS_ERROR(status, "init dynamic indexer failed");
        auto [status1, isExist] = indexDirectory->IsExist(_indexConfig->GetIndexName()).StatusWith();
        RETURN_IF_STATUS_ERROR(status1, "IsExist [%s] failed",
                               indexDirectory->DebugString(_indexConfig->GetIndexName()).c_str());
        if (!isExist) {
            if (_indexerParam.readerOpenType == IndexerParameter::READER_DEFAULT_VALUE) {
                auto dictReader = std::make_shared<DefaultTermDictionaryReader>(_indexConfig, _indexerParam.docCount);
                auto status = dictReader->Open(nullptr, "", false);
                RETURN_IF_STATUS_ERROR(status, "dictionary reader open fail");
                IndexFormatOption formatOption;
                formatOption.Init(_indexConfig);
                formatOption.SetIsCompressedPostingHeader(false);
                _leafReader = std::make_shared<InvertedLeafReader>(_indexConfig, formatOption, _indexerParam.docCount,
                                                                   dictReader, nullptr);
                // not support HighFreqVocabulary in default index
                assert(!_indexConfig->GetHighFreqVocabulary());
                return Status::OK();
            } else {
                // TODO(xc & lc) maybe remove this if block, the truncate index indeed not exist
                if (!_indexConfig->GetNonTruncateIndexName().empty()) {
                    return Status::OK();
                }
                AUTIL_LOG(ERROR, "get index field dir [%s] failed", _indexConfig->GetIndexName().c_str());
                return Status::InternalError();
            }
        }
        auto [status2, subIDir] = indexDirectory->GetDirectory(_indexConfig->GetIndexName()).StatusWith();
        if (!status2.IsOK() || !subIDir) {
            AUTIL_LOG(ERROR, "fail to get subDir, indexName[%s] indexType[%s]", indexConfig->GetIndexName().c_str(),
                      indexConfig->GetIndexType().c_str());
            return Status::InternalError("directory for inverted index[%s] does not exist",
                                         _indexConfig->GetIndexName().c_str());
        }
        auto subDir = file_system::IDirectory::ToLegacyDirectory(subIDir);
        if (_indexConfig->GetHighFreqVocabulary()) {
            _bitmapDiskIndexer = std::make_shared<BitmapDiskIndexer>(_indexerParam.docCount);
            auto status = _bitmapDiskIndexer->Open(indexConfig, indexDirectory);
            RETURN_IF_STATUS_ERROR(status, "open bitmap disk indexer fail, indexName[%s] indexType[%s]",
                                   indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str());
        }

        std::string dictFilePath = DICTIONARY_FILE_NAME;
        std::string postingFilePath = POSTING_FILE_NAME;
        std::string formatFilePath = INDEX_FORMAT_OPTION_FILE_NAME;
        bool dictExist = subDir->IsExist(dictFilePath);
        bool postingExist = subDir->IsExist(postingFilePath);

        IndexFormatOption formatOption;
        if (subDir->IsExist(formatFilePath)) {
            std::string indexFormatStr;
            subDir->Load(formatFilePath, indexFormatStr);
            formatOption = IndexFormatOption::FromString(indexFormatStr);
            formatOption.SetNumberIndex(InvertedIndexUtil::IsNumberIndex(_indexConfig));
        } else {
            formatOption.Init(_indexConfig);
            formatOption.SetIsCompressedPostingHeader(false);
        }

        if (!dictExist && !postingExist) {
            return OpenSectionAttrDiskIndexer(_indexConfig, indexDirectory, formatOption);
        }
        if (!dictExist || !postingExist) {
            AUTIL_LOG(ERROR, "index[%s]:missing dictionary or posting file", _indexConfig->GetIndexName().c_str());
            return Status::Corruption();
        }
        std::shared_ptr<DictionaryReader> dictReader(DictionaryCreator::CreateReader(_indexConfig));
        status = dictReader->Open(subDir, dictFilePath, true);
        RETURN_IF_STATUS_ERROR(status, "dictionary reader open fail");

        file_system::ReaderOption option(file_system::FSOT_LOAD_CONFIG);
        option.supportCompress = true;
        auto postingReader = subDir->CreateFileReader(postingFilePath, option);
        _leafReader = std::make_shared<InvertedLeafReader>(_indexConfig, formatOption, _indexerParam.docCount,
                                                           dictReader, postingReader);
        return OpenSectionAttrDiskIndexer(_indexConfig, indexDirectory, formatOption);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "inverted disk indexer open failed, exception[%s]", e.what());
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "inverted disk indexer open failed, exception[%s]", e.what());
    } catch (...) {
        AUTIL_LOG(ERROR, "inverted disk indexer open failed, unknown exception");
    }
    AUTIL_LOG(ERROR, "inverted disk indexer open failed");
    return Status::Corruption("inverted disk indexer open failed");
}

Status InvertedDiskIndexer::InitDynamicIndexer(const std::shared_ptr<file_system::IDirectory>& indexDir)
{
    if (_indexerParam.docCount == 0) {
        return Status::OK();
    }
    if (not _indexConfig->IsIndexUpdatable()) {
        return Status::OK();
    }
    if (not _indexConfig->GetNonTruncateIndexName().empty()) {
        return Status::OK();
    }
    std::string dynamicTreeFileName = DynamicMemIndexer::GetDynamicTreeFileName(_indexConfig);
    auto [status, resourceFile] = indexDir->GetResourceFile(dynamicTreeFileName).StatusWith();

    if (not status.IsNotFound()) {
        RETURN_IF_STATUS_ERROR(status, "get [%s] failed", dynamicTreeFileName.c_str());
    }
    if (status.IsNotFound() or not resourceFile or resourceFile->Empty()) {
        assert(!indexlibv2::framework::Segment::IsRtSegmentId(_indexerParam.segmentId));
        auto postingTable = new DynamicMemIndexer::DynamicPostingTable(HASHMAP_INIT_SIZE);
        if (_indexConfig->SupportNull()) {
            postingTable->nullTermPosting = DynamicMemIndexer::CreatePostingWriter(postingTable);
        }
        std::tie(status, resourceFile) = indexDir->CreateResourceFile(dynamicTreeFileName).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "create [%s] failed", dynamicTreeFileName.c_str());
        assert(resourceFile);
        resourceFile->Reset(postingTable);
    }
    _dynamicPostingResource = resourceFile;
    return Status::OK();
}

Status
InvertedDiskIndexer::OpenSectionAttrDiskIndexer(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& config,
                                                const std::shared_ptr<file_system::IDirectory>& indexDir,
                                                const IndexFormatOption& formatOption)
{
    const indexlibv2::config::InvertedIndexConfig::IndexShardingType shardingType = config->GetShardingType();

    if (shardingType == indexlibv2::config::InvertedIndexConfig::IST_NO_SHARDING &&
        config->GetNonTruncateIndexName().empty()) {
        if (formatOption.HasSectionAttribute()) {
            auto packageIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(config);
            assert(packageIndexConfig);
            auto sectionAttrConf = packageIndexConfig->GetSectionAttributeConfig();
            assert(sectionAttrConf);
            auto attrConfig = sectionAttrConf->CreateAttributeConfig(packageIndexConfig->GetIndexName());
            if (attrConfig == nullptr) {
                auto status = Status::InternalError("create attribute config failed, index name[%s]",
                                                    config->GetIndexName().c_str());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                assert(false);
                return status;
            }
            auto indexFactory = std::make_unique<SectionAttributeIndexFactory>();
            auto attrDiskIndexer = indexFactory->CreateDiskIndexer(attrConfig, _indexerParam);
            auto status = attrDiskIndexer->Open(attrConfig, indexDir);
            RETURN_IF_STATUS_ERROR(status, "section attribute disk indexer init failed, indexName[%s]",
                                   config->GetIndexName().c_str());
            _sectionAttrDiskIndexer = attrDiskIndexer;
        }
    }
    return Status::OK();
}

size_t InvertedDiskIndexer::EstimateMemUsed(const std::shared_ptr<IIndexConfig>& indexConfig,
                                            const std::shared_ptr<file_system::IDirectory>& indexDirectory)
{
    if (indexDirectory == nullptr) {
        AUTIL_LOG(DEBUG, "Inverted index [%s] try EstimateMemUsed failed. Input Directory is null.",
                  indexConfig->GetIndexName().c_str());
        return 0;
    }
    auto subDir = indexlib::file_system::IDirectory::ToLegacyDirectory(indexDirectory)
                      ->GetDirectory(indexConfig->GetIndexName(), /*throwExceptionIfNotExist=*/false);
    if (subDir == nullptr) {
        if (_indexerParam.readerOpenType == IndexerParameter::READER_DEFAULT_VALUE) {
            return 0;
        }
        if (_indexerParam.docCount != 0) {
            AUTIL_LOG(ERROR, "get inverted index field dir [%s] failed", indexConfig->GetIndexName().c_str());
        }
        return 0;
    }

    size_t totalMemUse = subDir->EstimateFileMemoryUse(DICTIONARY_FILE_NAME, file_system::FSOT_LOAD_CONFIG) +
                         subDir->EstimateFileMemoryUse(POSTING_FILE_NAME, file_system::FSOT_LOAD_CONFIG);
    const auto& invertedConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
    assert(invertedConfig);
    if (invertedConfig->GetHighFreqVocabulary()) {
        auto bitmapDiskIndexer = std::make_unique<BitmapDiskIndexer>(_indexerParam.docCount);
        totalMemUse += bitmapDiskIndexer->EstimateMemUsed(indexConfig, indexDirectory);
    }
    totalMemUse += EstimateSectionAttributeMemUse(invertedConfig, indexDirectory);
    return totalMemUse;
}

size_t InvertedDiskIndexer::EstimateSectionAttributeMemUse(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& invertedConfig,
    const std::shared_ptr<file_system::IDirectory>& indexDirectory) const
{
    if (invertedConfig->GetShardingType() != indexlibv2::config::InvertedIndexConfig::IST_NO_SHARDING) {
        return 0;
    }
    auto indexFormatOption = std::make_unique<IndexFormatOption>();
    indexFormatOption->Init(invertedConfig);
    if (!indexFormatOption->HasSectionAttribute()) {
        return 0;
    }
    auto packageIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(invertedConfig);
    assert(packageIndexConfig);
    auto sectionAttrConf = packageIndexConfig->GetSectionAttributeConfig();
    assert(sectionAttrConf);
    auto attrConfig = sectionAttrConf->CreateAttributeConfig(packageIndexConfig->GetIndexName());
    if (attrConfig == nullptr) {
        AUTIL_LOG(ERROR, "create attr config failed, index name [%s]", invertedConfig->GetIndexName().c_str());
        assert(false);
        return 0;
    }
    auto indexFactory = std::make_unique<SectionAttributeIndexFactory>();
    auto attrDiskIndexer = indexFactory->CreateDiskIndexer(attrConfig, _indexerParam);
    assert(attrDiskIndexer);
    return attrDiskIndexer->EstimateMemUsed(attrConfig, indexDirectory);
}

size_t InvertedDiskIndexer::EvaluateCurrentMemUsed()
{
    size_t totalMemUse = 0;

    // todo remove try catch
    try {
        if (_leafReader) {
            totalMemUse += _leafReader->EvaluateCurrentMemUsed();
        }
        if (_bitmapDiskIndexer) {
            totalMemUse += _bitmapDiskIndexer->EvaluateCurrentMemUsed();
        }
        if (_sectionAttrDiskIndexer) {
            totalMemUse += _sectionAttrDiskIndexer->EvaluateCurrentMemUsed();
        }
        if (_dynamicPostingResource) {
            totalMemUse += _dynamicPostingResource->GetMemoryUse();
        }
    } catch (util::NonExistException& e) {
        AUTIL_LOG(WARN, "inverted disk index estimate memory use failed, indexName[%s], file is not exist",
                  _indexConfig->GetIndexName().c_str());
        return 0;
    } catch (...) {
        AUTIL_LOG(ERROR, "inverted disk index estimate memory use failed, indexName[%s] ",
                  _indexConfig->GetIndexName().c_str());
        return 0;
    }
    return totalMemUse;
}

void InvertedDiskIndexer::UpdateTokens(docid_t localDocId, const document::ModifiedTokens& modifiedTokens)
{
    if (not _dynamicPostingResource or _dynamicPostingResource->Empty()) {
        AUTIL_LOG(ERROR, "update doc[%d] failed, index [%s] not updatable", localDocId,
                  _indexConfig->GetIndexName().c_str());
        return;
    }
    auto postingTable = _dynamicPostingResource->GetResource<DynamicMemIndexer::DynamicPostingTable>();
    assert(postingTable);
    for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
        auto item = modifiedTokens[i];
        DoUpdateToken(DictKeyInfo(item.first), _indexFormatOption.IsNumberIndex(), postingTable, localDocId,
                      item.second);
    }
    document::ModifiedTokens::Operation nullTermOp;
    if (modifiedTokens.GetNullTermOperation(&nullTermOp)) {
        DoUpdateToken(DictKeyInfo::NULL_TERM, _indexFormatOption.IsNumberIndex(), postingTable, localDocId, nullTermOp);
    }

    _dynamicPostingResource->UpdateMemoryUse(postingTable->nodeManager.TotalMemory());
}

void InvertedDiskIndexer::UpdateOneTerm(docid_t localDocId, const DictKeyInfo& dictKeyInfo,
                                        document::ModifiedTokens::Operation op)
{
    if (not _dynamicPostingResource or _dynamicPostingResource->Empty()) {
        AUTIL_LOG(ERROR, "update terms failed, indexId [%s] not updatable", _indexConfig->GetIndexName().c_str());
        return;
    }
    auto postingTable = _dynamicPostingResource->GetResource<DynamicMemIndexer::DynamicPostingTable>();
    assert(postingTable);
    DoUpdateToken(dictKeyInfo, _indexFormatOption.IsNumberIndex(), postingTable, localDocId, op);
}

void InvertedDiskIndexer::UpdateTerms(IndexUpdateTermIterator* termIter)
{
    if (not _dynamicPostingResource or _dynamicPostingResource->Empty()) {
        AUTIL_LOG(ERROR, "update terms failed, indexId [%s] not updatable", _indexConfig->GetIndexName().c_str());
        return;
    }
    auto postingTable = _dynamicPostingResource->GetResource<DynamicMemIndexer::DynamicPostingTable>();
    assert(postingTable);

    ComplexDocId complexDocId;
    while (termIter->Next(&complexDocId)) {
        document::ModifiedTokens::Operation op = complexDocId.IsDelete() ? document::ModifiedTokens::Operation::REMOVE
                                                                         : document::ModifiedTokens::Operation::ADD;
        DoUpdateToken(termIter->GetTermKey(), _indexFormatOption.IsNumberIndex(), postingTable, complexDocId.DocId(),
                      op);
    }
}

void InvertedDiskIndexer::DoUpdateToken(const DictKeyInfo& termKey, bool isNumberIndex,
                                        DynamicMemIndexer::DynamicPostingTable* postingTable, docid_t docId,
                                        document::ModifiedTokens::Operation op)
{
    HighFrequencyTermPostingType highFreqType = _indexConfig->GetHighFrequencyTermPostingType();
    bool isHighFreqTerm =
        _indexConfig->GetHighFreqVocabulary() && _indexConfig->GetHighFreqVocabulary()->Lookup(termKey);
    if (isHighFreqTerm and _bitmapDiskIndexer) {
        _bitmapDiskIndexer->Update(docId, termKey, op == document::ModifiedTokens::Operation::REMOVE);
    }
    if (not isHighFreqTerm or highFreqType == hp_both) {
        DynamicMemIndexer::UpdateToken(termKey, _indexFormatOption.IsNumberIndex(), postingTable, docId, op);
    }
}

void InvertedDiskIndexer::UpdateBuildResourceMetrics(size_t& poolMemory, size_t& totalMemory,
                                                     size_t& totalRetiredMemory, size_t& totalDocCount,
                                                     size_t& totalAlloc, size_t& totalFree,
                                                     size_t& totalTreeCount) const
{
    if (_dynamicPostingResource and !_dynamicPostingResource->Empty()) {
        auto postingTable = _dynamicPostingResource->GetResource<DynamicMemIndexer::DynamicPostingTable>();
        assert(postingTable);
        poolMemory += postingTable->pool.getUsedBytes();
        totalMemory += postingTable->nodeManager.TotalMemory();
        totalRetiredMemory += postingTable->nodeManager.RetiredMemory();
        totalAlloc += postingTable->nodeManager.TotalAllocatedMemory();
        totalFree += postingTable->nodeManager.TotalFreedMemory();
        totalTreeCount += postingTable->table.Size();
        totalDocCount += postingTable->stat.totalDocCount;

        if (postingTable->nullTermPosting) {
            ++totalTreeCount;
        }
    }
}

std::shared_ptr<indexlibv2::index::IDiskIndexer> InvertedDiskIndexer::GetSectionAttributeDiskIndexer() const
{
    return _sectionAttrDiskIndexer;
}

} // namespace indexlib::index
