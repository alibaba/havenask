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
#include "indexlib/index/inverted_index/InvertedMemIndexer.h"

#include <any>

#include "autil/Scope.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/Field.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/document/normal/IndexTokenizeField.h"
#include "indexlib/document/normal/Section.h"
#include "indexlib/document/normal/Token.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/common/FileCompressParamHelper.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/common/patch/PatchFileInfo.h"
#include "indexlib/index/inverted_index/IndexFormatWriterCreator.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/inverted_index/InvertedIndexMerger.h"
#include "indexlib/index/inverted_index/InvertedIndexUtil.h"
#include "indexlib/index/inverted_index/InvertedLeafMemReader.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/SectionAttributeMemIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexWriter.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicMemIndexer.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryWriter.h"
#include "indexlib/index/inverted_index/merge/SimpleInvertedIndexMerger.h"
#include "indexlib/index/inverted_index/patch/InvertedIndexSegmentUpdater.h"
#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::document::IDocument;
using indexlibv2::document::IDocumentBatch;
using indexlibv2::document::IIndexFields;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, InvertedMemIndexer);

InvertedMemIndexer::InvertedMemIndexer(const indexlibv2::index::IndexerParameter& indexerParam,
                                       const std::shared_ptr<InvertedIndexMetrics>& metrics)
    : _indexerParam(indexerParam)
    , _modifiedPosting(autil::mem_pool::pool_allocator<PostingPair>(&_simplePool))
    , _hashKeyVector(autil::mem_pool::pool_allocator<dictkey_t>(&_simplePool))
    , _metrics(metrics)
{
    // for 2 pool
    _estimateDumpTempMemSize = NeedEstimateDumpTempMemSize() ? indexlibv2::DEFAULT_CHUNK_SIZE * 1024 * 1024 * 2 : 0;
}

InvertedMemIndexer::~InvertedMemIndexer()
{
    if (_postingTable) {
        PostingTable::Iterator it = _postingTable->CreateIterator();
        while (it.HasNext()) {
            PostingTable::KeyValuePair& kv = it.Next();
            PostingWriter* pWriter = kv.second;
            IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool.get(), pWriter);
        }
        _postingTable->Clear();
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool.get(), _nullTermPosting);
    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool.get(), _postingTable);
    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool.get(), _bitmapIndexWriter);
    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool.get(), _postingWriterResource);
}

Status InvertedMemIndexer::Init(const std::shared_ptr<IIndexConfig>& indexConfig,
                                indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    std::any emptyFieldHint;
    assert(docInfoExtractorFactory);
    _docInfoExtractor = docInfoExtractorFactory->CreateDocumentInfoExtractor(
        indexConfig, indexlibv2::document::extractor::DocumentInfoExtractorType::INVERTED_INDEX_DOC,
        /*fieldHint=*/emptyFieldHint);
    if (_docInfoExtractor == nullptr) {
        AUTIL_LOG(ERROR, "create document info extractor failed");
        return Status::InternalError("create document info extractor failed.");
    }
    _indexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
    if (_indexConfig == nullptr) {
        AUTIL_LOG(ERROR,
                  "IndexType[%s] IndexName[%s] invalid index config, "
                  "dynamic cast to legacy index config failed.",
                  indexConfig->GetIndexType().c_str(), indexConfig->GetIndexName().c_str());
        return Status::InvalidArgs("invalid index config, dynamic cast to legacy index config failed.");
    }
    size_t lastSegmentDistinctTermCount = HASHMAP_INIT_SIZE;
    if (_indexerParam.lastSegmentMetrics &&
        _indexerParam.lastSegmentMetrics->GetDistinctTermCount(_indexConfig->GetIndexName()) > 0) {
        lastSegmentDistinctTermCount =
            _indexerParam.lastSegmentMetrics->GetDistinctTermCount(_indexConfig->GetIndexName());
    }
    _hashMapInitSize = (size_t)(lastSegmentDistinctTermCount * HASHMAP_INIT_SIZE_FACTOR);

    indexlibv2::config::InvertedIndexConfig::Iterator iter = _indexConfig->CreateIterator();
    while (iter.HasNext()) {
        const auto& fieldConfig = iter.Next();
        fieldid_t fieldId = fieldConfig->GetFieldId();
        _fieldIds.push_back(fieldId);
    }
    if (_fieldIds.empty()) {
        assert(false);
        AUTIL_LOG(ERROR, "IndexType[%s] IndexName[%s] invalid index config, has no field id.",
                  _indexConfig->GetIndexType().c_str(), _indexConfig->GetIndexName().c_str());
        return Status::InvalidArgs("invalid index config, has no field id.");
    }
    _allocator.reset(new util::MMapAllocator);
    _byteSlicePool.reset(new autil::mem_pool::Pool(_allocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024));
    _bufferPool.reset(new (std::nothrow)
                          autil::mem_pool::RecyclePool(_allocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024, 8));
    _indexFormatOption = std::make_shared<IndexFormatOption>();
    const indexlibv2::config::InvertedIndexConfig::IndexShardingType shardingType = _indexConfig->GetShardingType();
    assert(shardingType == indexlibv2::config::InvertedIndexConfig::IST_IS_SHARDING or
           shardingType == indexlibv2::config::InvertedIndexConfig::IST_NO_SHARDING);
    _indexFormatOption->Init(_indexConfig);
    if (shardingType == indexlibv2::config::InvertedIndexConfig::IST_NO_SHARDING) {
        if (_indexFormatOption->HasSectionAttribute()) {
            _sectionAttributeMemIndexer = std::make_unique<SectionAttributeMemIndexer>(_indexerParam);
            auto status = _sectionAttributeMemIndexer->Init(_indexConfig, docInfoExtractorFactory);
            RETURN_IF_STATUS_ERROR(status, "section attribute mem indexer init failed, indexName[%s]",
                                   GetIndexName().c_str());
        }
    }

    _highFreqVol = _indexConfig->GetHighFreqVocabulary();
    _indexSupportNull = _indexConfig->SupportNull();

    _postingTable =
        IE_POOL_COMPATIBLE_NEW_CLASS(_byteSlicePool.get(), PostingTable, _byteSlicePool.get(), _hashMapInitSize);

    _postingWriterResource =
        IE_POOL_COMPATIBLE_NEW_CLASS(_byteSlicePool.get(), PostingWriterResource, &_simplePool, _byteSlicePool.get(),
                                     _bufferPool.get(), _indexFormatOption->GetPostingFormatOption());
    _bitmapIndexWriter = IndexFormatWriterCreator::CreateBitmapIndexWriter(_indexFormatOption, _indexerParam.isOnline,
                                                                           _byteSlicePool.get(), &_simplePool);
    if (_indexConfig->IsIndexUpdatable() and _indexConfig->GetNonTruncateIndexName().empty()) {
        if (_indexerParam.isOnline) {
            bool isNumberIndex = InvertedIndexUtil::IsNumberIndex(_indexConfig);
            _dynamicMemIndexer = std::make_unique<DynamicMemIndexer>(HASHMAP_INIT_SIZE, isNumberIndex);
            _dynamicMemIndexer->Init(_indexConfig);
        }
        // TODO: @qingran 应改为，在需要Dump刷盘时，才初始化该PatchWriter
        _invertedIndexSegmentUpdater = std::make_unique<InvertedIndexSegmentUpdater>(0, _indexConfig);
    }
    _sealed = false;
    return Status::OK();
}

bool InvertedMemIndexer::IsDirty() const { return _docCount > 0; }

Status InvertedMemIndexer::DocBatchToDocs(IDocumentBatch* docBatch, std::vector<document::IndexDocument*>* docs) const
{
    docs->reserve(docBatch->GetBatchSize());
    for (size_t i = 0; i < docBatch->GetBatchSize(); i++) {
        if (!docBatch->IsDropped(i)) {
            if ((*docBatch)[i]->GetDocOperateType() != ADD_DOC) {
                continue;
            }

            document::IndexDocument* doc = nullptr;
            Status s = _docInfoExtractor->ExtractField((*docBatch)[i].get(), (void**)&doc);
            if (!s.IsOK()) {
                return s;
            }
            (*docs).emplace_back(doc);
        }
    }
    return Status::OK();
}
Status InvertedMemIndexer::Build(const IIndexFields* indexFields, size_t n)
{
    assert(0);
    return {};
}
Status InvertedMemIndexer::Build(IDocumentBatch* docBatch)
{
    // TODO(makuo.mnb) remove this try catch when v2 migration completed
    try {
        return DoBuild(docBatch);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "inverted mem indexer build failed, indexName[%s] exception[%s]",
                  _indexConfig->GetIndexName().c_str(), e.what());
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "inverted mem indexer build failed, indexName[%s] exception[%s]",
                  _indexConfig->GetIndexName().c_str(), e.what());
    } catch (...) {
        AUTIL_LOG(ERROR, "inverted mem indexer build failed, indexName[%s] unknown exception",
                  _indexConfig->GetIndexName().c_str());
    }
    return Status::IOError("inverted mem indexer build failed");
}

Status InvertedMemIndexer::DoBuild(IDocumentBatch* docBatch)
{
    std::vector<document::IndexDocument*> docs;
    auto status = DocBatchToDocs(docBatch, &docs);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "convert doc batch to docs failed");
        return status;
    }
    for (const auto& doc : docs) {
        auto status = AddDocument(doc);
        RETURN_IF_STATUS_ERROR(status, "add document failed");
    }
    return Status::OK();
}

Status InvertedMemIndexer::AddDocument(document::IndexDocument* doc)
{
    for (auto fieldId : _fieldIds) {
        const document::Field* field = doc->GetField(fieldId);
        auto status = AddField(field);
        RETURN_IF_STATUS_ERROR(status, "add field fail, fieldId[%d]", fieldId);
    }
    EndDocument(*doc);
    _docCount++;
    return Status::OK();
}

void InvertedMemIndexer::EndDocument(const document::IndexDocument& indexDocument)
{
    if (_bitmapIndexWriter) {
        _bitmapIndexWriter->EndDocument(indexDocument);
        _estimateDumpTempMemSize =
            NeedEstimateDumpTempMemSize()
                ? std::max(_estimateDumpTempMemSize, _bitmapIndexWriter->GetEstimateDumpTempMemSize())
                : 0;
    }
    if (!_modifiedPosting.empty()) {
        for (PostingVector::iterator it = _modifiedPosting.begin(); it != _modifiedPosting.end(); it++) {
            if (_indexFormatOption->HasTermPayload()) {
                termpayload_t termpayload = indexDocument.GetTermPayload(it->first);
                it->second->SetTermPayload(termpayload);
            }

            docpayload_t docPayload = _indexFormatOption->HasDocPayload() ? indexDocument.GetDocPayload(it->first) : 0;
            it->second->EndDocument(indexDocument.GetDocId(), docPayload);
            UpdateToCompressShortListCount(it->second->GetDF());
            _estimateDumpTempMemSize =
                NeedEstimateDumpTempMemSize()
                    ? std::max(_estimateDumpTempMemSize, it->second->GetEstimateDumpTempMemSize())
                    : 0;
        }
    }

    if (_modifyNullTermPosting) {
        assert(_nullTermPosting);
        _nullTermPosting->EndDocument(indexDocument.GetDocId(), 0);
        UpdateToCompressShortListCount(_nullTermPosting->GetDF());
        _estimateDumpTempMemSize =
            NeedEstimateDumpTempMemSize()
                ? std::max(_estimateDumpTempMemSize, _nullTermPosting->GetEstimateDumpTempMemSize())
                : 0;
    }

    _modifiedPosting.clear();
    _modifyNullTermPosting = false;
    if (_sectionAttributeMemIndexer) {
        _sectionAttributeMemIndexer->EndDocument(indexDocument);
    }
    _basePos = 0;
}

void InvertedMemIndexer::UpdateToCompressShortListCount(uint32_t df)
{
    if (df == UNCOMPRESS_SHORT_LIST_MIN_LEN) {
        _toCompressShortListCount++;
    } else if (df == UNCOMPRESS_SHORT_LIST_MAX_LEN + 1) {
        _toCompressShortListCount--;
    }
}

Status InvertedMemIndexer::DoAddNullToken(fieldid_t fieldId)
{
    if (!_indexSupportNull) {
        AUTIL_LOG(WARN, "index [%s] not support Add Null Token, ignore", GetIndexName().c_str());
        return Status::OK();
    }

    fieldid_t fieldIdxInPack = _indexConfig->GetFieldIdxInPack(fieldId);
    if (fieldIdxInPack < 0) {
        std::stringstream ss;
        ss << "get field idx in pack failed, fieldIdxInPack = " << fieldIdxInPack << " ,fieldId = " << fieldId;
        AUTIL_LOG(ERROR, "%s", ss.str().c_str());
        return Status::InvalidArgs("get field idx in pack failed");
    }

    bool isHighFreqTerm = false;
    if (_highFreqVol) {
        isHighFreqTerm = _highFreqVol->Lookup(DictKeyInfo::NULL_TERM);
    }

    // bitmap index writer should add first for realtime
    if (isHighFreqTerm and _bitmapIndexWriter) {
        _bitmapIndexWriter->AddToken(DictKeyInfo::NULL_TERM, 0);
    }

    if (!isHighFreqTerm || _indexConfig->GetHighFrequencyTermPostingType() == indexlib::index::hp_both ||
        _indexConfig->IsTruncateTerm(DictKeyInfo::NULL_TERM)) {
        if (!_nullTermPosting) {
            _nullTermPosting = IndexFormatWriterCreator::CreatePostingWriter(_indexConfig->GetInvertedIndexType(),
                                                                             _postingWriterResource);
        }
        assert(_nullTermPosting);
        if (_nullTermPosting->NotExistInCurrentDoc()) {
            _modifyNullTermPosting = true;
        }
        _nullTermPosting->AddPosition(_basePos, 0, fieldIdxInPack);
    }
    return Status::OK();
}

Status InvertedMemIndexer::AddToken(const document::Token* token, fieldid_t fieldId, pos_t tokenBasePos)
{
    DictKeyInfo hashKey(token->GetHashKey());
    fieldid_t fieldIdxInPack = _indexConfig->GetFieldIdxInPack(fieldId);
    if (fieldIdxInPack < 0) {
        std::stringstream ss;
        ss << "invalid fieldIdxInPack = " << fieldIdxInPack << " ,fieldId = " << fieldId;
        AUTIL_LOG(ERROR, "%s", ss.str().c_str());
        return Status::ConfigError("get field in pack fail");
    }

    bool isHighFreqTerm = false;
    if (_highFreqVol) {
        isHighFreqTerm = _highFreqVol->Lookup(hashKey);
    }

    // bitmap index writer should add first for realtime
    if (isHighFreqTerm and _bitmapIndexWriter) {
        _bitmapIndexWriter->AddToken(hashKey, token->GetPosPayload());
    }

    if (!isHighFreqTerm || _indexConfig->GetHighFrequencyTermPostingType() == indexlib::index::hp_both ||
        _indexConfig->IsTruncateTerm(hashKey)) {
        DoAddHashToken(hashKey, token, fieldIdxInPack, tokenBasePos);
    }
    return Status::OK();
}

void InvertedMemIndexer::DoAddHashToken(DictKeyInfo termKey, const document::Token* token, fieldid_t fieldIdxInPack,
                                        pos_t tokenBasePos)
{
    assert(!termKey.IsNull());
    dictkey_t retrievalHashKey =
        InvertedIndexUtil::GetRetrievalHashKey(_indexFormatOption->IsNumberIndex(), termKey.GetKey());
    // AUTIL_LOG(ERROR, "thuhujin index name:%s, key:%s, retrievalHashKey:%lu", _indexConfig->GetIndexName().c_str(),
    //           termKey.ToString().c_str(), retrievalHashKey);
    PostingWriter** pWriter = _postingTable->Find(retrievalHashKey);
    if (pWriter == nullptr) {
        // not found, add a new <term,posting>
        PostingWriter* postingWriter =
            IndexFormatWriterCreator::CreatePostingWriter(_indexConfig->GetInvertedIndexType(), _postingWriterResource);
        _postingTable->Insert(retrievalHashKey, postingWriter);
        _hashKeyVector.push_back(termKey.GetKey());

        postingWriter->AddPosition(tokenBasePos, token->GetPosPayload(), fieldIdxInPack);
        _modifiedPosting.push_back(std::make_pair(termKey.GetKey(), postingWriter));
    } else {
        // found, append to the end
        auto writer = *pWriter;
        if (writer->NotExistInCurrentDoc()) {
            _modifiedPosting.push_back(std::make_pair(termKey.GetKey(), writer));
        }
        writer->AddPosition(tokenBasePos, token->GetPosPayload(), fieldIdxInPack);
    }
}

Status InvertedMemIndexer::AddField(const document::Field* field)
{
    if (!field) {
        return Status::OK();
    }
    document::Field::FieldTag tag = field->GetFieldTag();
    if (tag == document::Field::FieldTag::NULL_FIELD) {
        auto nullField = dynamic_cast<const document::NullField*>(field);
        fieldid_t fieldId = nullField->GetFieldId();
        return DoAddNullToken(fieldId);
    }

    assert(tag == document::Field::FieldTag::TOKEN_FIELD);
    auto tokenizeField = dynamic_cast<const document::IndexTokenizeField*>(field);

    if (!tokenizeField) {
        AUTIL_LOG(ERROR, "fieldTag [%d] is not IndexTokenizeField", static_cast<int8_t>(field->GetFieldTag()));
        return Status::ConfigError("field tag is not IndexTokenizeField");
    }
    if (tokenizeField->GetSectionCount() <= 0) {
        return Status::OK();
    }

    pos_t tokenBasePos = _basePos;
    uint32_t fieldLen = 0;
    fieldid_t fieldId = tokenizeField->GetFieldId();

    for (auto iterField = tokenizeField->Begin(); iterField != tokenizeField->End(); ++iterField) {
        const document::Section* section = *iterField;
        fieldLen += section->GetLength();

        for (size_t i = 0; i < section->GetTokenCount(); i++) {
            const document::Token* token = section->GetToken(i);
            tokenBasePos += token->GetPosIncrement();
            auto status = AddToken(token, fieldId, tokenBasePos);
            RETURN_IF_STATUS_ERROR(status, "add token failed, fieldId[%d]", fieldId);
        }
    }
    _basePos += fieldLen;
    return Status::OK();
}

size_t InvertedMemIndexer::CalPostingLength(PostingWriter* postingWriter) const
{
    uint64_t inlinePostingValue;
    bool isDocList = false;
    if (!postingWriter || postingWriter->GetDictInlinePostingValue(inlinePostingValue, isDocList)) {
        return 0;
    }

    PostingFormatOption formatOption = _indexFormatOption->GetPostingFormatOption();
    TermMeta termMeta(postingWriter->GetDF(), postingWriter->GetTotalTF());
    TermMetaDumper tmDumper(formatOption);
    if (_indexFormatOption->HasTermPayload()) {
        termMeta.SetPayload(postingWriter->GetTermPayload());
    }
    uint32_t curLength = tmDumper.CalculateStoreSize(termMeta) + postingWriter->GetDumpLength();
    if (formatOption.IsCompressedPostingHeader()) {
        return VByteCompressor::GetVInt32Length(curLength) + curLength;
    }
    return sizeof(uint32_t) + curLength;
}

size_t InvertedMemIndexer::CalPostingFileLength() const
{
    size_t totalLen = 0;
    PostingTable::Iterator iter = _postingTable->CreateIterator();
    while (iter.HasNext()) {
        PostingTable::KeyValuePair& kv = iter.Next();
        PostingWriter* postingWriter = kv.second;
        totalLen += CalPostingLength(postingWriter);
    }

    if (_indexSupportNull && _nullTermPosting) {
        totalLen += CalPostingLength(_nullTermPosting);
    }
    return totalLen;
}

Status InvertedMemIndexer::Dump(autil::mem_pool::PoolBase* dumpPool,
                                const std::shared_ptr<file_system::Directory>& directory,
                                const std::shared_ptr<indexlibv2::framework::DumpParams>& dumpParams)
{
    // TODO(makuo.mnb) remove this try catch when v2 migration completed
    try {
        FlushBuffer();
        return DoDump(dumpPool, directory, dumpParams);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "inverted mem indexer dump failed, dir[%s] autil::legacy::exception[%s]",
                  directory->GetLogicalPath().c_str(), e.what());
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "inverted mem indexer dump failed, dir[%s] std::exception[%s]",
                  directory->GetLogicalPath().c_str(), e.what());
    } catch (...) {
        AUTIL_LOG(ERROR, "inverted mem indexer dump failed, dir[%s] unknown exception",
                  directory->GetLogicalPath().c_str());
    }
    return Status::IOError("inverted mem indexer dump failed");
}

Status InvertedMemIndexer::DoDump(autil::mem_pool::PoolBase* dumpPool,
                                  const std::shared_ptr<file_system::Directory>& directory,
                                  const std::shared_ptr<indexlibv2::framework::DumpParams>& dumpParams)
{
    AUTIL_LOG(DEBUG, "Dumping index : [%s]", GetIndexName().c_str());
    if (!_sealed) {
        AUTIL_LOG(ERROR, "dump failed, need call Seal() before dump, indexName[%s]", GetIndexName().c_str());
        return Status::InternalError();
    }
    {
        /*
            For alter field custom merge task recover
            To be removed
        */
        if (directory->IsExist(GetIndexName())) {
            AUTIL_LOG(WARN, "directory [%s] already exist, will be removed.", GetIndexName().c_str());
            directory->RemoveDirectory(GetIndexName());
        }
    }

    // TODO(makuo.mnb) change to IDirectory
    std::shared_ptr<file_system::Directory> indexDirectory = directory->MakeDirectory(GetIndexName());
    std::shared_ptr<file_system::Directory> tmpDirectory = indexDirectory;
    if (_invertedIndexSegmentUpdater) {
        // 该 tmp 临时目录后面需要删除掉（或者保证不刷出去），
        // 而 PackageMemStorage 不允许随意删除某个子目录，以避免与 Flush 过程冲突。
        // 此处指定 Package 本意是期望指定该目录是一个 独立的、不会落盘的 目录，但 FS 暂时没有合适的语义，
        // 另一方面，由于 Dump 过程中涉及许多 Factory 和文件，也不方便将各个输出文件逐个指定为 MemFile，
        // 所以目前使用该写法，未来有更多场景时，再做相关概念抽象
        tmpDirectory = indexDirectory->MakeDirectory("tmp", file_system::DirectoryOption::Package());
    }
    auto params = std::dynamic_pointer_cast<indexlibv2::index::DocMapDumpParams>(dumpParams);
    Status status =
        DoDumpFiles(dumpPool, /*outputDirectory=*/tmpDirectory, /*sectionAttributeOutputDirectory=*/directory, params);
    RETURN_IF_STATUS_ERROR(status, "dump files to [%s] failed", tmpDirectory->GetLogicalPath().c_str());
    if (_invertedIndexSegmentUpdater) {
        const std::vector<docid_t>* old2NewDocId = params ? &params->old2new : nullptr;
        auto [status, patchFileInfo] =
            _invertedIndexSegmentUpdater->DumpToIndexDir(tmpDirectory->GetIDirectory(), /*srcSegment=*/0, old2NewDocId);
        RETURN_IF_STATUS_ERROR(status, "dump patch to [%s] failed", tmpDirectory->GetLogicalPath().c_str());
        status = MergeModifiedTokens(tmpDirectory, patchFileInfo, indexDirectory);
        RETURN_IF_STATUS_ERROR(status, "merge posting and patch from [%s] to [%s] failed",
                               tmpDirectory->GetLogicalPath().c_str(), indexDirectory->GetLogicalPath().c_str());
        indexDirectory->RemoveDirectory("tmp", file_system::RemoveOption());
    }
    if (_dynamicMemIndexer) {
        _dynamicMemIndexer->Dump(directory);
    }
    return Status::OK();
}

Status InvertedMemIndexer::DoDumpFiles(autil::mem_pool::PoolBase* dumpPool,
                                       const std::shared_ptr<file_system::Directory>& outputDirectory,
                                       const std::shared_ptr<file_system::Directory>& sectionAttributeOutputDirectory,
                                       const std::shared_ptr<indexlibv2::index::DocMapDumpParams>& dumpParams)
{
    std::shared_ptr<DictionaryWriter> dictWriter(
        IndexFormatWriterCreator::CreateDictionaryWriter(_indexConfig, /*SegmentStatistics*/ nullptr, dumpPool));
    dictWriter->Open(outputDirectory, DICTIONARY_FILE_NAME, _hashKeyVector.size());
    file_system::WriterOption writerOption;
    indexlibv2::index::FileCompressParamHelper::SyncParam(_indexConfig->GetFileCompressConfigV2(),
                                                          /*SegmentStatistics=*/nullptr, writerOption);
    auto postingFile = outputDirectory->CreateFileWriter(POSTING_FILE_NAME, writerOption);
    std::sort(_hashKeyVector.begin(), _hashKeyVector.end());
    size_t postingFileLen = CalPostingFileLength();
    if (postingFileLen > 0 && !postingFile->ReserveFile(postingFileLen).OK()) {
        AUTIL_LOG(ERROR, "file writer reserve file fail, reserveSize[%lu] file[%s]", postingFileLen,
                  postingFile->GetLogicalPath().c_str());
        return Status::IOError();
    }

    const std::vector<docid_t>* old2NewDocId = dumpParams ? &dumpParams->old2new : nullptr;
    autil::mem_pool::Pool dumpTempPool(_allocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024);
    autil::mem_pool::RecyclePool recycleTempPool(_allocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024, 8);
    PostingWriterResource dumpResource(_postingWriterResource->simplePool, &dumpTempPool, &recycleTempPool,
                                       _postingWriterResource->postingFormatOption);

    InvertedIndexMetrics::SortDumpMetrics* dumpMetric =
        (old2NewDocId && _metrics) ? _metrics->AddSingleFieldSortDumpMetric(GetIndexName()) : nullptr;

    auto sortDumpAction = [&]() {
        if (dumpMetric) {
            dumpMetric->maxMemUse =
                std::max(dumpMetric->maxMemUse, dumpTempPool.getTotalBytes() + recycleTempPool.getTotalBytes());
        }
        dumpTempPool.reset();
        recycleTempPool.reset();
    };

    for (auto it = _hashKeyVector.begin(); it != _hashKeyVector.end(); ++it) {
        PostingWriter** pWriter =
            _postingTable->Find(InvertedIndexUtil::GetRetrievalHashKey(_indexFormatOption->IsNumberIndex(), *it));
        assert(pWriter != NULL);
        auto [status, dictValue] = DumpPosting(*pWriter, postingFile, old2NewDocId, &dumpResource, dumpMetric);
        RETURN_IF_STATUS_ERROR(status, "dump posting error");
        sortDumpAction();
        dictWriter->AddItem(DictKeyInfo(*it), dictValue);
    }
    if (_nullTermPosting) {
        auto [status, dictValue] = DumpPosting(_nullTermPosting, postingFile, old2NewDocId, &dumpResource, dumpMetric);
        RETURN_IF_STATUS_ERROR(status, "null term dump posting error");
        sortDumpAction();
        dictWriter->AddItem(DictKeyInfo::NULL_TERM, dictValue);
    }
    assert(postingFileLen == 0 || old2NewDocId || postingFileLen == postingFile->GetLogicLength());

    if (!postingFile->Close().OK()) {
        AUTIL_LOG(ERROR, "file writer close fail, file[%s]", postingFile->GetLogicalPath().c_str());
        return Status::IOError();
    }

    dictWriter->Close();
    dictWriter.reset();

    _indexFormatOption->Store(outputDirectory);

    // TODO support sort dump metrics for bitmap move to indexlibv2
    if (_bitmapIndexWriter) {
        _bitmapIndexWriter->Dump(outputDirectory, &dumpTempPool, old2NewDocId);
    }

    if (_sectionAttributeMemIndexer) {
        auto status = _sectionAttributeMemIndexer->Dump(dumpPool, sectionAttributeOutputDirectory, dumpParams);
        RETURN_IF_STATUS_ERROR(status, "section attribute mem indexer dump failed, indexName[%s]",
                               GetIndexName().c_str());
    }
    return Status::OK();
}

Status
InvertedMemIndexer::MergeModifiedTokens(const std::shared_ptr<file_system::Directory>& inputIndexDir,
                                        const std::shared_ptr<indexlibv2::index::PatchFileInfo>& inputPatchFileInfo,
                                        const std::shared_ptr<file_system::Directory>& outputIndexDir)
{
    if (not _invertedIndexSegmentUpdater) {
        return Status::OK();
    }
    auto merger = std::make_unique<SimpleInvertedIndexMerger>(_indexConfig);
    size_t dictKeyCount = _postingTable->Size();
    return merger->Merge(inputIndexDir, inputPatchFileInfo, outputIndexDir, dictKeyCount, /*docCount=*/0);
}

std::pair<Status, dictvalue_t>
InvertedMemIndexer::DumpPosting(PostingWriter* writer, const std::shared_ptr<file_system::FileWriter>& fileWriter,
                                const std::vector<docid_t>* old2NewDocId, PostingWriterResource* resource,
                                InvertedIndexMetrics::SortDumpMetrics* metrics)
{
    uint64_t inlinePostingValue;
    bool isDocList = false;

    std::shared_ptr<PostingWriter> scopedWriter;
    PostingWriter* dumpWriter = writer;
    if (old2NewDocId) {
        int64_t beginTime = autil::TimeUtility::currentTimeInMicroSeconds();
        autil::ScopeGuard guard([&]() {
            if (metrics) {
                metrics->sortDumpTime += autil::TimeUtility::currentTimeInMicroSeconds() - beginTime;
            }
        });
        auto reorderWriter =
            IndexFormatWriterCreator::CreatePostingWriter(_indexConfig->GetInvertedIndexType(), resource);
        auto dumpPool = resource->byteSlicePool;
        assert(dumpPool);
        scopedWriter = autil::shared_ptr_pool(dumpPool, reorderWriter);
        try {
            if (!writer->CreateReorderPostingWriter(dumpPool, old2NewDocId, reorderWriter)) {
                AUTIL_LOG(ERROR, "create dump recorder posting writer failed");
                return {Status::IOError("create dump reorder posting writer failed"), INVALID_DICT_VALUE};
            }
            dumpWriter = reorderWriter;
        } catch (autil::legacy::ExceptionBase& e) {
            AUTIL_LOG(ERROR, "create dump recorder posting writer failed, exception [%s]", e.what());
            return {Status::IOError("create dump reorder posting failed", e.what()), INVALID_DICT_VALUE};
        }
    }

    if (dumpWriter->GetDictInlinePostingValue(inlinePostingValue, isDocList)) {
        return {Status::OK(),
                ShortListOptimizeUtil::CreateDictInlineValue(inlinePostingValue, isDocList, true /*dfFirst*/)};
    } else {
        return DumpNormalPosting(dumpWriter, fileWriter);
    }
}

std::pair<Status, dictvalue_t>
InvertedMemIndexer::DumpNormalPosting(PostingWriter* writer, const std::shared_ptr<file_system::FileWriter>& fileWriter)
{
    TermMeta termMeta(writer->GetDF(), writer->GetTotalTF());
    if (_indexFormatOption->HasTermPayload()) {
        termMeta.SetPayload(writer->GetTermPayload());
    }
    PostingFormatOption formatOption = _indexFormatOption->GetPostingFormatOption();
    TermMetaDumper tmDumper(formatOption);
    uint64_t offset = fileWriter->GetLogicLength();

    uint32_t postingLen = tmDumper.CalculateStoreSize(termMeta) + writer->GetDumpLength();
    if (formatOption.IsCompressedPostingHeader()) {
        auto ret = fileWriter->WriteVUInt32(postingLen);
        if (!ret.OK()) {
            AUTIL_LOG(ERROR, "write file fail, len[%u] file[%s]", postingLen, fileWriter->GetLogicalPath().c_str());
            return {Status::IOError("write file failed"), 0};
        }
    } else {
        auto ret = fileWriter->Write((void*)(&postingLen), sizeof(uint32_t));
        if (!ret.OK()) {
            AUTIL_LOG(ERROR, "write file fail, len[%lu] file[%s]", sizeof(uint32_t),
                      fileWriter->GetLogicalPath().c_str());
            return {Status::IOError("write file failed"), 0};
        }
    }

    tmDumper.Dump(fileWriter, termMeta);
    writer->Dump(fileWriter);

    uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(
        writer->GetDF(), writer->GetTotalTF(), _indexFormatOption->GetPostingFormatOption().GetDocListCompressMode());
    auto dictValue = ShortListOptimizeUtil::CreateDictValue(compressMode, (int64_t)offset);
    return {Status::OK(), dictValue};
}

bool InvertedMemIndexer::IsValidDocument(IDocument* doc) { return _docInfoExtractor->IsValidDocument(doc); }
bool InvertedMemIndexer::IsValidField(const IIndexFields* fields)
{
    assert(0);
    return false;
}

void InvertedMemIndexer::ValidateDocumentBatch(IDocumentBatch* docBatch)
{
    for (size_t i = 0; i < docBatch->GetBatchSize(); i++) {
        if (!docBatch->IsDropped(i)) {
            if (!_docInfoExtractor->IsValidDocument((*docBatch)[i].get())) {
                docBatch->DropDoc(i);
            }
        }
    }
}

void InvertedMemIndexer::UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* memUpdater)
{
    int64_t attrCurrentMemUse = 0, attrDumpTmpMemUse = 0, attrDumpExpandMemUse = 0, attrDumpFileSize = 0;
    if (_sectionAttributeMemIndexer) {
        _sectionAttributeMemIndexer->UpdateMemUse(memUpdater);
        memUpdater->GetMemInfo(attrCurrentMemUse, attrDumpTmpMemUse, attrDumpExpandMemUse, attrDumpFileSize);
    }

    int64_t currentMemUse = _byteSlicePool->getUsedBytes() + _simplePool.getUsedBytes() + _bufferPool->getUsedBytes();
    int64_t dumpTempBufferSize = TieredDictionaryWriter<dictkey_t>::GetInitialMemUse() + _estimateDumpTempMemSize;
    int64_t dumpExpandBufferSize =
        _bufferPool->getUsedBytes() + _toCompressShortListCount * UNCOMPRESS_SHORT_LIST_DUMP_EXPAND_FACTOR;
    int64_t dumpFileSize = currentMemUse * 0.2;
    memUpdater->UpdateCurrentMemUse(currentMemUse + attrCurrentMemUse);
    memUpdater->EstimateDumpTmpMemUse(std::max(dumpTempBufferSize, attrDumpTmpMemUse));
    memUpdater->EstimateDumpExpandMemUse(dumpExpandBufferSize + attrDumpExpandMemUse);
    memUpdater->EstimateDumpedFileSize(dumpFileSize + attrDumpFileSize);
    // TODO: @qingran updateMemUse
}

bool InvertedMemIndexer::NeedEstimateDumpTempMemSize() const { return _indexerParam.sortDescriptions.size() > 0; }

void InvertedMemIndexer::Seal() {}

void InvertedMemIndexer::FlushBuffer()
{
    PostingTable::Iterator it = _postingTable->CreateIterator();
    while (it.HasNext()) {
        PostingTable::KeyValuePair& kv = it.Next();
        PostingWriter* pWriter = kv.second;
        if (pWriter) {
            pWriter->EndSegment();
        }
    }
    if (_nullTermPosting) {
        _nullTermPosting->EndSegment();
    }
    _sealed = true;
}

const PostingWriter* InvertedMemIndexer::GetPostingListWriter(const DictKeyInfo& key) const
{
    if (key.IsNull()) {
        return _nullTermPosting;
    }
    PostingWriter** pWriter =
        _postingTable->Find(InvertedIndexUtil::GetRetrievalHashKey(_indexFormatOption->IsNumberIndex(), key.GetKey()));
    if (pWriter != nullptr) {
        return *pWriter;
    }
    return nullptr;
}

std::shared_ptr<IndexSegmentReader> InvertedMemIndexer::CreateInMemReader()
{
    std::shared_ptr<indexlibv2::index::AttributeMemReader> sectionReader;
    if (_sectionAttributeMemIndexer) {
        sectionReader = _sectionAttributeMemIndexer->CreateInMemReader();
    }

    std::shared_ptr<InMemBitmapIndexSegmentReader> bitmapReader;
    if (_bitmapIndexWriter) {
        bitmapReader = _bitmapIndexWriter->CreateInMemReader();
    }

    std::shared_ptr<DynamicIndexSegmentReader> dynamicReader;
    if (_dynamicMemIndexer) {
        dynamicReader = _dynamicMemIndexer->CreateInMemReader();
    }
    return std::make_shared<InvertedLeafMemReader>(_postingTable, sectionReader, bitmapReader, dynamicReader,
                                                   *_indexFormatOption, &_nullTermPosting, &_hashKeyVector);
}

std::shared_ptr<indexlibv2::index::AttributeMemReader> InvertedMemIndexer::CreateSectionAttributeMemReader() const
{
    std::shared_ptr<indexlibv2::index::AttributeMemReader> sectionReader;
    if (_sectionAttributeMemIndexer) {
        sectionReader = _sectionAttributeMemIndexer->CreateInMemReader();
    }
    return sectionReader;
}

void InvertedMemIndexer::FillStatistics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics)
{
    segmentMetrics->SetDistinctTermCount(_indexConfig->GetIndexName(), GetDistinctTermCount());
}

uint32_t InvertedMemIndexer::GetDistinctTermCount() const
{
    uint32_t termCount = _postingTable->Size();
    if (_bitmapIndexWriter) {
        termCount += _bitmapIndexWriter->GetDistinctTermCount();
    }

    if (_nullTermPosting) {
        ++termCount;
    }
    return termCount;
}

void InvertedMemIndexer::UpdateTokens(docid_t docId, const document::ModifiedTokens& modifiedTokens)
{
    for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
        auto [termKey, op] = modifiedTokens[i];
        UpdateOneTerm(docId, DictKeyInfo(termKey), op);
    }
    if (modifiedTokens.NullTermExists()) {
        document::ModifiedTokens::Operation op;
        if (modifiedTokens.GetNullTermOperation(&op)) {
            UpdateOneTerm(docId, DictKeyInfo::NULL_TERM, op);
        }
    }
}

void InvertedMemIndexer::UpdateOneTerm(docid_t docId, const DictKeyInfo& dictKeyInfo,
                                       document::ModifiedTokens::Operation op)
{
    if (_invertedIndexSegmentUpdater) {
        _invertedIndexSegmentUpdater->Update(docId, dictKeyInfo, op == document::ModifiedTokens::Operation::REMOVE);
    }
    if (_dynamicMemIndexer) {
        HighFrequencyTermPostingType highFreqType = _indexConfig->GetHighFrequencyTermPostingType();
        bool isHighFreqTerm = _highFreqVol && _highFreqVol->Lookup(dictKeyInfo);
        if (isHighFreqTerm and _bitmapIndexWriter) {
            _bitmapIndexWriter->UpdateTerm(docId, dictKeyInfo, op);
        }
        if (not isHighFreqTerm or highFreqType == hp_both) {
            _dynamicMemIndexer->UpdateTerm(docId, dictKeyInfo, op);
        }
    }
}

indexlibv2::document::extractor::IDocumentInfoExtractor* InvertedMemIndexer::GetDocInfoExtractor() const
{
    return _docInfoExtractor.get();
}

} // namespace indexlib::index
