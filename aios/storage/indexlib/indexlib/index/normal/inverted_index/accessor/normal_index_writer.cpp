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
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"

#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexWriter.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryTypedFactory.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"
#include "indexlib/index/inverted_index/format/skiplist/BufferedSkipListWriter.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_format_writer_creator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/in_mem_dynamic_index_segment_reader.h"
#include "indexlib/index/util/file_compress_param_helper.h"
#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil::mem_pool;

using namespace indexlib::index;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index {
namespace {
using indexlib::index::legacy::IndexFormatWriterCreator;
}

IE_LOG_SETUP(index, NormalIndexWriter);

size_t NormalIndexWriter::UNCOMPRESS_SHORT_LIST_MIN_LEN = MAX_UNCOMPRESSED_DOC_LIST_SIZE + 1;
size_t NormalIndexWriter::UNCOMPRESS_SHORT_LIST_MAX_LEN = MAX_DOC_PER_RECORD - 1;
size_t NormalIndexWriter::UNCOMPRESS_SHORT_LIST_DUMP_EXPAND_FACTOR = BufferedSkipListWriter::ESTIMATE_INIT_MEMORY_USE;

NormalIndexWriter::NormalIndexWriter(segmentid_t segmentId, size_t lastSegmentDistinctTermCount,
                                     const config::IndexPartitionOptions& options)
    : mSegmentId(segmentId)
    , mBasePos(0)
    , mPostingTable(nullptr)
    , mModifyNullTermPosting(false)
    , mModifiedPosting(pool_allocator<PostingPair>(&mSimplePool))
    , mHashKeyVector(pool_allocator<dictkey_t>(&mSimplePool))
    , mNullTermPosting(nullptr)
    , mBitmapIndexWriter(NULL)
    , mDynamicIndexWriter(nullptr)
    , mPostingWriterResource(NULL)
    , mOptions(options)
    , mToCompressShortListCount(0)
    , mIndexSupportNull(false)
{
    if (lastSegmentDistinctTermCount == 0) {
        lastSegmentDistinctTermCount = HASHMAP_INIT_SIZE;
    }
    mHashMapInitSize = (size_t)(lastSegmentDistinctTermCount * HASHMAP_INIT_SIZE_FACTOR);
}

NormalIndexWriter::~NormalIndexWriter()
{
    if (mPostingTable) {
        PostingTable::Iterator it = mPostingTable->CreateIterator();
        while (it.HasNext()) {
            PostingTable::KeyValuePair& kv = it.Next();
            PostingWriter* pWriter = kv.second;
            IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool.get(), pWriter);
        }
        mPostingTable->Clear();
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool.get(), mNullTermPosting);
    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool.get(), mPostingTable);
    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool.get(), mBitmapIndexWriter);
    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool.get(), mDynamicIndexWriter);
    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool.get(), mPostingWriterResource);
}

void NormalIndexWriter::InitMemoryPool()
{
    IndexWriter::InitMemoryPool();
    // set align size = 8, make sure operation of 64bits variable is atomic
    mBufferPool.reset(new (nothrow) RecyclePool(_allocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024, 8));
}

void NormalIndexWriter::Init(const config::IndexConfigPtr& indexConfig, BuildResourceMetrics* buildResourceMetrics)
{
    IndexWriter::Init(indexConfig, buildResourceMetrics);

    mIndexFormatOption.reset(new LegacyIndexFormatOption);
    const config::IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
    assert(shardingType == config::IndexConfig::IST_IS_SHARDING or
           shardingType == config::IndexConfig::IST_NO_SHARDING);
    mIndexFormatOption->Init(indexConfig);
    if (shardingType == config::IndexConfig::IST_NO_SHARDING) {
        mSectionAttributeWriter = IndexFormatWriterCreator::CreateSectionAttributeWriter(
            indexConfig, mIndexFormatOption, buildResourceMetrics);
    }
    mHighFreqVol = indexConfig->GetHighFreqVocabulary();

    mIndexSupportNull = indexConfig->SupportNull();

    mPostingTable =
        IE_POOL_COMPATIBLE_NEW_CLASS(_byteSlicePool.get(), PostingTable, _byteSlicePool.get(), mHashMapInitSize);

    mPostingWriterResource =
        IE_POOL_COMPATIBLE_NEW_CLASS(_byteSlicePool.get(), PostingWriterResource, &mSimplePool, _byteSlicePool.get(),
                                     mBufferPool.get(), mIndexFormatOption->GetPostingFormatOption());

    mBitmapIndexWriter = IndexFormatWriterCreator::CreateBitmapIndexWriter(mIndexFormatOption, mOptions.IsOnline(),
                                                                           _byteSlicePool.get(), &mSimplePool);

    if (indexConfig->IsIndexUpdatable()) {
        mDynamicIndexWriter =
            IE_POOL_COMPATIBLE_NEW_CLASS(_byteSlicePool.get(), DynamicIndexWriter, mSegmentId, mHashMapInitSize,
                                         mIndexFormatOption->IsNumberIndex(), mOptions);
        mDynamicIndexWriter->Init(_indexConfig, buildResourceMetrics);
    }

    UpdateBuildResourceMetrics();
}

size_t NormalIndexWriter::EstimateInitMemUse(const config::IndexConfigPtr& indexConfig,
                                             const index_base::PartitionSegmentIteratorPtr& segIter)
{
    size_t initSize = PostingTable::EstimateFirstBucketSize(mHashMapInitSize);
    // pool allocat one block size when allocate at init time
    // when pool allocate more than one block size mem, original block left mem useless
    initSize += autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE;
    return initSize;
}

void NormalIndexWriter::UpdateBuildResourceMetrics()
{
    if (!_buildResourceMetricsNode) {
        return;
    }
    int64_t currentMemUse = _byteSlicePool->getUsedBytes() + mSimplePool.getUsedBytes() + mBufferPool->getUsedBytes();

    int64_t dumpTempBufferSize = TieredDictionaryWriter<dictkey_t>::GetInitialMemUse();
    int64_t dumpExpandBufferSize =
        mBufferPool->getUsedBytes() + mToCompressShortListCount * UNCOMPRESS_SHORT_LIST_DUMP_EXPAND_FACTOR;
    int64_t dumpFileSize = currentMemUse * 0.2;

    _buildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, currentMemUse);
    _buildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempBufferSize);
    _buildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, dumpExpandBufferSize);
    _buildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
    if (mDynamicIndexWriter) {
        mDynamicIndexWriter->UpdateBuildResourceMetrics();
    }
}

void NormalIndexWriter::AddField(const Field* field)
{
    if (!field) {
        return;
    }

    Field::FieldTag tag = field->GetFieldTag();
    if (tag == Field::FieldTag::NULL_FIELD) {
        auto nullField = dynamic_cast<const NullField*>(field);
        fieldid_t fieldId = nullField->GetFieldId();
        DoAddNullToken(fieldId);
        return;
    }

    assert(tag == Field::FieldTag::TOKEN_FIELD);
    auto tokenizeField = dynamic_cast<const IndexTokenizeField*>(field);

    if (!tokenizeField) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "fieldTag [%d] is not IndexTokenizeField",
                             static_cast<int8_t>(field->GetFieldTag()));
    }
    if (tokenizeField->GetSectionCount() <= 0) {
        return;
    }

    pos_t tokenBasePos = mBasePos;
    uint32_t fieldLen = 0;
    fieldid_t fieldId = tokenizeField->GetFieldId();

    for (auto iterField = tokenizeField->Begin(); iterField != tokenizeField->End(); ++iterField) {
        const Section* section = *iterField;
        fieldLen += section->GetLength();

        for (size_t i = 0; i < section->GetTokenCount(); i++) {
            const Token* token = section->GetToken(i);
            tokenBasePos += token->GetPosIncrement();
            AddToken(token, fieldId, tokenBasePos);
        }
    }
    mBasePos += fieldLen;
}

void NormalIndexWriter::AddToken(const Token* token, fieldid_t fieldId, pos_t tokenBasePos)
{
    index::DictKeyInfo hashKey(token->GetHashKey());
    fieldid_t fieldIdxInPack = _indexConfig->GetFieldIdxInPack(fieldId);
    if (fieldIdxInPack < 0) {
        std::stringstream ss;
        ss << "fieldIdxInPack = " << fieldIdxInPack << " ,fieldId = " << fieldId;
        INDEXLIB_THROW(util::OutOfRangeException, "%s", ss.str().c_str());
    }

    bool isHighFreqTerm = false;
    if (mHighFreqVol) {
        isHighFreqTerm = mHighFreqVol->Lookup(hashKey);
    }

    // bitmap index writer should add first for realtime
    if (isHighFreqTerm and mBitmapIndexWriter) {
        mBitmapIndexWriter->AddToken(hashKey, token->GetPosPayload());
    }

    if (!isHighFreqTerm || _indexConfig->GetHighFrequencyTermPostingType() == hp_both ||
        _indexConfig->IsTruncateTerm(hashKey)) {
        DoAddHashToken(hashKey, token, fieldIdxInPack, tokenBasePos);
    }
}

void NormalIndexWriter::DoAddNullToken(fieldid_t fieldId)
{
    if (!mIndexSupportNull) {
        IE_LOG(ERROR, "index [%s] not support Add Null Token, ignore", _indexName.c_str());
        return;
    }

    fieldid_t fieldIdxInPack = _indexConfig->GetFieldIdxInPack(fieldId);
    if (fieldIdxInPack < 0) {
        std::stringstream ss;
        ss << "fieldIdxInPack = " << fieldIdxInPack << " ,fieldId = " << fieldId;
        INDEXLIB_THROW(util::OutOfRangeException, "%s", ss.str().c_str());
    }

    bool isHighFreqTerm = false;
    if (mHighFreqVol) {
        isHighFreqTerm = mHighFreqVol->Lookup(index::DictKeyInfo::NULL_TERM);
    }

    // bitmap index writer should add first for realtime
    if (isHighFreqTerm and mBitmapIndexWriter) {
        mBitmapIndexWriter->AddToken(index::DictKeyInfo::NULL_TERM, 0);
    }

    if (!isHighFreqTerm || _indexConfig->GetHighFrequencyTermPostingType() == hp_both ||
        _indexConfig->IsTruncateTerm(index::DictKeyInfo::NULL_TERM)) {
        if (!mNullTermPosting) {
            mNullTermPosting = IndexFormatWriterCreator::CreatePostingWriter(_indexConfig->GetInvertedIndexType(),
                                                                             mPostingWriterResource);
        }
        assert(mNullTermPosting);
        if (mNullTermPosting->NotExistInCurrentDoc()) {
            mModifyNullTermPosting = true;
        }
        mNullTermPosting->AddPosition(mBasePos, 0, fieldIdxInPack);
    }
}

void NormalIndexWriter::DoAddHashToken(index::DictKeyInfo termKey, const Token* token, fieldid_t fieldIdxInPack,
                                       pos_t tokenBasePos)
{
    assert(!termKey.IsNull());
    dictkey_t retrievalHashKey =
        InvertedIndexUtil::GetRetrievalHashKey(mIndexFormatOption->IsNumberIndex(), termKey.GetKey());
    PostingWriter** pWriter = mPostingTable->Find(retrievalHashKey);
    if (pWriter == nullptr) {
        // not found, add a new <term,posting>
        PostingWriter* postingWriter =
            IndexFormatWriterCreator::CreatePostingWriter(_indexConfig->GetInvertedIndexType(), mPostingWriterResource);
        mPostingTable->Insert(retrievalHashKey, postingWriter);
        mHashKeyVector.push_back(termKey.GetKey());

        postingWriter->AddPosition(tokenBasePos, token->GetPosPayload(), fieldIdxInPack);
        mModifiedPosting.push_back(std::make_pair(termKey.GetKey(), postingWriter));
    } else {
        // found, append to the end
        auto writer = *pWriter;
        if (writer->NotExistInCurrentDoc()) {
            mModifiedPosting.push_back(std::make_pair(termKey.GetKey(), writer));
        }
        writer->AddPosition(tokenBasePos, token->GetPosPayload(), fieldIdxInPack);
    }
}

void NormalIndexWriter::UpdateTerm(docid_t docId, index::DictKeyInfo termKey, document::ModifiedTokens::Operation op)
{
    assert(mDynamicIndexWriter);
    mDynamicIndexWriter->UpdateTerm(docId, termKey, op);
}

// TODO: Is null term case included here?
void NormalIndexWriter::UpdateField(docid_t docId, const document::ModifiedTokens& modifiedTokens)
{
    if (!mDynamicIndexWriter) {
        return;
    }
    auto highFreqType = _indexConfig->GetHighFrequencyTermPostingType();
    for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
        auto token = modifiedTokens[i];
        index::DictKeyInfo termKey(token.first);
        bool isHighFreqTerm = false;
        if (mHighFreqVol) {
            isHighFreqTerm = mHighFreqVol->Lookup(termKey);
        }
        if (isHighFreqTerm and mBitmapIndexWriter) {
            mBitmapIndexWriter->UpdateTerm(docId, termKey, token.second);
        }
        if (!isHighFreqTerm or highFreqType == hp_both) {
            UpdateTerm(docId, termKey, token.second);
        }
    }
}

void NormalIndexWriter::UpdateField(docid_t docId, index::DictKeyInfo termKey, bool isDelete)
{
    if (!mDynamicIndexWriter) {
        return;
    }
    bool isHighFreqTerm = mHighFreqVol and mHighFreqVol->Lookup(termKey);
    auto op = isDelete ? document::ModifiedTokens::Operation::REMOVE : document::ModifiedTokens::Operation::ADD;
    if (isHighFreqTerm and mBitmapIndexWriter) {
        mBitmapIndexWriter->UpdateTerm(docId, termKey, op);
    }

    if (!isHighFreqTerm or _indexConfig->GetHighFrequencyTermPostingType() == hp_both) {
        UpdateTerm(docId, termKey, op);
    }
}

uint32_t NormalIndexWriter::GetDistinctTermCount() const
{
    uint32_t termCount = mPostingTable->Size();
    if (mBitmapIndexWriter) {
        termCount += mBitmapIndexWriter->GetDistinctTermCount();
    }

    if (mNullTermPosting) {
        ++termCount;
    }
    return termCount;
}

uint64_t NormalIndexWriter::GetNormalTermDfSum() const
{
    uint64_t count = 0;
    PostingTable::Iterator it = mPostingTable->CreateIterator();
    while (it.HasNext()) {
        PostingTable::KeyValuePair& kv = it.Next();
        count += kv.second->GetDF();
    }

    if (mNullTermPosting) {
        count += mNullTermPosting->GetDF();
    }
    return count;
}

void NormalIndexWriter::EndDocument(const IndexDocument& indexDocument)
{
    if (mBitmapIndexWriter) {
        mBitmapIndexWriter->EndDocument(indexDocument);
    }
    for (PostingVector::iterator it = mModifiedPosting.begin(); it != mModifiedPosting.end(); it++) {
        if (mIndexFormatOption->HasTermPayload()) {
            termpayload_t termpayload = indexDocument.GetTermPayload(it->first);
            it->second->SetTermPayload(termpayload);
        }

        docpayload_t docPayload = mIndexFormatOption->HasDocPayload()
                                      ? indexDocument.GetDocPayload(_indexConfig->GetTruncatePayloadConfig(), it->first)
                                      : 0;
        it->second->EndDocument(indexDocument.GetDocId(), docPayload);
        UpdateToCompressShortListCount(it->second->GetDF());
    }

    if (mModifyNullTermPosting) {
        assert(mNullTermPosting);
        mNullTermPosting->EndDocument(indexDocument.GetDocId(), 0);
        UpdateToCompressShortListCount(mNullTermPosting->GetDF());
    }

    mModifiedPosting.clear();
    mModifyNullTermPosting = false;
    if (mSectionAttributeWriter) {
        mSectionAttributeWriter->EndDocument(indexDocument);
    }
    mBasePos = 0;
    UpdateBuildResourceMetrics();
}

void NormalIndexWriter::EndSegment()
{
    PostingTable::Iterator it = mPostingTable->CreateIterator();
    while (it.HasNext()) {
        PostingTable::KeyValuePair& kv = it.Next();
        PostingWriter* pWriter = kv.second;
        if (pWriter) {
            pWriter->EndSegment();
        }
    }
    if (mNullTermPosting) {
        mNullTermPosting->EndSegment();
    }
    UpdateBuildResourceMetrics();
}

void NormalIndexWriter::Dump(const file_system::DirectoryPtr& directory, PoolBase* dumpPool)
{
    IE_LOG(DEBUG, "Dumping index : [%s]", _indexName.c_str());

    {
        /*
            For alter field custom merge task recover
            To be removed
        */
        if (directory->IsExist(_indexName)) {
            IE_LOG(WARN, "File [%s] already exist, will be removed.", _indexName.c_str());
            directory->RemoveDirectory(_indexName);
        }
    }

    file_system::DirectoryPtr indexDirectory = directory->MakeDirectory(_indexName);

    std::shared_ptr<DictionaryWriter> dictWriter(
        IndexFormatWriterCreator::CreateDictionaryWriter(_indexConfig, _temperatureLayer, dumpPool));
    dictWriter->Open(indexDirectory, DICTIONARY_FILE_NAME, mHashKeyVector.size());
    file_system::WriterOption writerOption;
    FileCompressParamHelper::SyncParam(_indexConfig->GetFileCompressConfig(), _temperatureLayer, writerOption);
    mPostingFile = indexDirectory->CreateFileWriter(POSTING_FILE_NAME, writerOption);
    sort(mHashKeyVector.begin(), mHashKeyVector.end());
    size_t postingFileLen = CalPostingFileLength();
    if (postingFileLen > 0) {
        mPostingFile->ReserveFile(postingFileLen).GetOrThrow();
    }
    for (auto it = mHashKeyVector.begin(); it != mHashKeyVector.end(); ++it) {
        PostingWriter** pWriter =
            mPostingTable->Find(InvertedIndexUtil::GetRetrievalHashKey(mIndexFormatOption->IsNumberIndex(), *it));
        assert(pWriter != NULL);
        DumpPosting(dictWriter, *pWriter, index::DictKeyInfo(*it));
    }
    if (mNullTermPosting) {
        DumpPosting(dictWriter, mNullTermPosting, index::DictKeyInfo::NULL_TERM);
    }
    assert(postingFileLen == 0 or postingFileLen == mPostingFile->GetLogicLength());
    mPostingFile->Close().GetOrThrow();
    dictWriter->Close();
    dictWriter.reset();

    mIndexFormatOption->Store(indexDirectory);

    if (mBitmapIndexWriter) {
        mBitmapIndexWriter->Dump(indexDirectory, dumpPool, nullptr);
    }
    if (mDynamicIndexWriter) {
        mDynamicIndexWriter->Dump(indexDirectory);
    }
    if (mSectionAttributeWriter) {
        mSectionAttributeWriter->Dump(directory, dumpPool);
    }
    UpdateBuildResourceMetrics();
}

void NormalIndexWriter::DumpPosting(const std::shared_ptr<DictionaryWriter>& dictWriter, PostingWriter* writer,
                                    const index::DictKeyInfo& key)
{
    dictvalue_t dictValue = DumpPosting(writer);
    dictWriter->AddItem(key, dictValue);
}

dictvalue_t NormalIndexWriter::DumpPosting(PostingWriter* writer)
{
    uint64_t inlinePostingValue;
    bool isDocList = false;
    if (writer->GetDictInlinePostingValue(inlinePostingValue, isDocList)) {
        return ShortListOptimizeUtil::CreateDictInlineValue(inlinePostingValue, isDocList, true /*dfFirst*/);
    } else {
        return DumpNormalPosting(writer);
    }
}

dictvalue_t NormalIndexWriter::DumpNormalPosting(PostingWriter* writer)
{
    TermMeta termMeta(writer->GetDF(), writer->GetTotalTF());
    if (mIndexFormatOption->HasTermPayload()) {
        termMeta.SetPayload(writer->GetTermPayload());
    }
    PostingFormatOption formatOption = mIndexFormatOption->GetPostingFormatOption();
    TermMetaDumper tmDumper(formatOption);
    uint64_t offset = mPostingFile->GetLogicLength();

    uint32_t postingLen = tmDumper.CalculateStoreSize(termMeta) + writer->GetDumpLength();
    if (formatOption.IsCompressedPostingHeader()) {
        mPostingFile->WriteVUInt32(postingLen).GetOrThrow();
    } else {
        mPostingFile->Write((void*)(&postingLen), sizeof(uint32_t)).GetOrThrow();
    }

    tmDumper.Dump(mPostingFile, termMeta);
    writer->Dump(mPostingFile);

    uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(
        writer->GetDF(), writer->GetTotalTF(), mIndexFormatOption->GetPostingFormatOption().GetDocListCompressMode());
    return ShortListOptimizeUtil::CreateDictValue(compressMode, (int64_t)offset);
}

const PostingWriter* NormalIndexWriter::GetPostingListWriter(const index::DictKeyInfo& key) const
{
    if (key.IsNull()) {
        return mNullTermPosting;
    }
    PostingWriter** pWriter =
        mPostingTable->Find(InvertedIndexUtil::GetRetrievalHashKey(mIndexFormatOption->IsNumberIndex(), key.GetKey()));
    if (pWriter != NULL) {
        return *pWriter;
    }
    return nullptr;
}

std::shared_ptr<IndexSegmentReader> NormalIndexWriter::CreateInMemReader()
{
    std::shared_ptr<AttributeSegmentReader> sectionReader;
    if (mSectionAttributeWriter) {
        sectionReader = mSectionAttributeWriter->CreateInMemAttributeReader();
    }

    std::shared_ptr<InMemBitmapIndexSegmentReader> bitmapReader;
    if (mBitmapIndexWriter) {
        bitmapReader = mBitmapIndexWriter->CreateInMemReader();
    }

    std::shared_ptr<InMemDynamicIndexSegmentReader> dynamicReader;
    if (mDynamicIndexWriter) {
        dynamicReader = mDynamicIndexWriter->CreateInMemReader();
    }

    std::shared_ptr<InMemNormalIndexSegmentReader> indexSegmentReader(
        new InMemNormalIndexSegmentReader(mPostingTable, sectionReader, bitmapReader, dynamicReader,
                                          *mIndexFormatOption, &mNullTermPosting, &mHashKeyVector));
    return indexSegmentReader;
}

BitmapPostingWriter* NormalIndexWriter::GetBitmapPostingWriter(const index::DictKeyInfo& hashKey)
{
    if (!mBitmapIndexWriter) {
        return NULL;
    }
    return mBitmapIndexWriter->GetBitmapPostingWriter(hashKey);
}

// TODO: FIXME:
void NormalIndexWriter::PrintIndexDocument(const IndexDocument& indexDocument)
{
    IndexDocument::Iterator it(indexDocument);
    std::cerr << "doc start, docid = " << indexDocument.GetDocId() << std::endl;
    while (it.HasNext()) {
        const Field* field = it.Next();
        const auto tokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
        if (!tokenizeField) {
            continue;
        }
        fieldid_t fieldId = tokenizeField->GetFieldId();
        if (this->_indexConfig->IsInIndex(fieldId)) {
            std::cerr << "field start, fieldid = " << fieldId << std::endl;

            if (tokenizeField->GetSectionCount() > 0) {
                for (auto iterField = tokenizeField->Begin(); iterField != tokenizeField->End(); ++iterField) {
                    const Section* section = *iterField;

                    std::cerr << "section start, section length = " << section->GetLength() << std::endl;

                    for (size_t i = 0; i < section->GetTokenCount(); i++) {
                        const Token* token = section->GetToken(i);
                        std::cerr << "token PosIncrement = " << token->GetPosIncrement() << std::endl;
                    }
                    std::cerr << "section end" << std::endl;
                }
            }
            std::cerr << "field end, fieldid = " << fieldId << std::endl;
        }
    }
    std::cerr << "doc end, docid = " << indexDocument.GetDocId() << std::endl;
}

size_t NormalIndexWriter::CalPostingFileLength() const
{
    size_t totalLen = 0;
    PostingTable::Iterator iter = mPostingTable->CreateIterator();
    while (iter.HasNext()) {
        PostingTable::KeyValuePair& kv = iter.Next();
        PostingWriter* postingWriter = kv.second;
        totalLen += CalPostingLength(postingWriter);
    }

    if (mIndexSupportNull && mNullTermPosting) {
        totalLen += CalPostingLength(mNullTermPosting);
    }
    return totalLen;
}

size_t NormalIndexWriter::CalPostingLength(PostingWriter* postingWriter) const
{
    uint64_t inlinePostingValue;
    bool isDocList = false;
    if (!postingWriter || postingWriter->GetDictInlinePostingValue(inlinePostingValue, isDocList)) {
        return 0;
    }

    PostingFormatOption formatOption = mIndexFormatOption->GetPostingFormatOption();
    TermMeta termMeta(postingWriter->GetDF(), postingWriter->GetTotalTF());
    TermMetaDumper tmDumper(formatOption);
    if (mIndexFormatOption->HasTermPayload()) {
        termMeta.SetPayload(postingWriter->GetTermPayload());
    }
    uint32_t curLength = tmDumper.CalculateStoreSize(termMeta) + postingWriter->GetDumpLength();
    if (formatOption.IsCompressedPostingHeader()) {
        return VByteCompressor::GetVInt32Length(curLength) + curLength;
    }
    return sizeof(uint32_t) + curLength;
}

void NormalIndexWriter::UpdateToCompressShortListCount(uint32_t df)
{
    if (df == UNCOMPRESS_SHORT_LIST_MIN_LEN) {
        mToCompressShortListCount++;
    } else if (df == UNCOMPRESS_SHORT_LIST_MAX_LEN + 1) {
        mToCompressShortListCount--;
    }
}

void NormalIndexWriter::SetTemperatureLayer(const string& layer)
{
    IndexWriter::SetTemperatureLayer(layer);
    if (mSectionAttributeWriter) {
        mSectionAttributeWriter->SetTemperatureLayer(layer);
    }
}
}} // namespace indexlib::index
