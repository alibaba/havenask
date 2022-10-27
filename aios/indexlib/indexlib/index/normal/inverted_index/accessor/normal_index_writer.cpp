#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_typed_factory.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_format_writer_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/util/key_hasher_factory.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"
#include "indexlib/config/high_frequency_vocabulary.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, NormalIndexWriter);

size_t NormalIndexWriter::UNCOMPRESS_SHORT_LIST_MIN_LEN = MAX_UNCOMPRESSED_DOC_LIST_SIZE + 1;
size_t NormalIndexWriter::UNCOMPRESS_SHORT_LIST_MAX_LEN = MAX_DOC_PER_RECORD - 1;
size_t NormalIndexWriter::UNCOMPRESS_SHORT_LIST_DUMP_EXPAND_FACTOR = BufferedSkipListWriter::ESTIMATE_INIT_MEMORY_USE;

NormalIndexWriter::NormalIndexWriter(
        size_t lastSegmentDistinctTermCount,
        const config::IndexPartitionOptions& options)
    : mBasePos(0)
    , mPostingTable(NULL)
    , mModifiedPosting(pool_allocator<PostingPair>(&mSimplePool))
    , mHashKeyVector(pool_allocator<dictkey_t>(&mSimplePool))
    , mBitmapIndexWriter(NULL)
    , mWriterCreator(NULL)
    , mPostingWriterResource(NULL)
    , mOptions(options)
    , mToCompressShortListCount(0)
{
    if (lastSegmentDistinctTermCount == 0)
    {
        lastSegmentDistinctTermCount = HASHMAP_INIT_SIZE;
    }
    mHashMapInitSize = (size_t)(lastSegmentDistinctTermCount * HASHMAP_INIT_SIZE_FACTOR);
}

NormalIndexWriter::~NormalIndexWriter()
{
    if (mPostingTable)
    {
        PostingTable::Iterator it = mPostingTable->CreateIterator();
        while (it.HasNext())
        {
            PostingTable::KeyValuePair& kv = it.Next();
            PostingWriter* pWriter = kv.second;
            if (pWriter)
            {
                pWriter->~PostingWriter();
                pWriter = NULL;
            }
        }
        mPostingTable->Clear();
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(mByteSlicePool.get(), mPostingTable);
    IE_POOL_COMPATIBLE_DELETE_CLASS(mByteSlicePool.get(), mBitmapIndexWriter);
    IE_POOL_COMPATIBLE_DELETE_CLASS(mByteSlicePool.get(), mWriterCreator);
    IE_POOL_COMPATIBLE_DELETE_CLASS(mByteSlicePool.get(), mPostingWriterResource);
}

void NormalIndexWriter::InitMemoryPool()
{
    IndexWriter::InitMemoryPool();
    // set align size = 8, make sure operation of 64bits variable is atomic
    mBufferPool.reset(new (nothrow) RecyclePool(
                    mAllocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024, 8));
}

void NormalIndexWriter::Init(const config::IndexConfigPtr& indexConfig,
                             BuildResourceMetrics* buildResourceMetrics,
                             const index_base::PartitionSegmentIteratorPtr& segIter)
{
    IndexWriter::Init(indexConfig, buildResourceMetrics, segIter);
    mPostingTable = IE_POOL_COMPATIBLE_NEW_CLASS(mByteSlicePool.get(),
            PostingTable, mByteSlicePool.get(), mHashMapInitSize);
    
    mIndexFormatOption.reset(new IndexFormatOption);
    mIndexFormatOption->Init(indexConfig);
    mWriterCreator = IE_POOL_COMPATIBLE_NEW_CLASS(mByteSlicePool.get(),
            IndexFormatWriterCreator, mIndexFormatOption,
            indexConfig);
    mPostingWriterResource = IE_POOL_COMPATIBLE_NEW_CLASS(mByteSlicePool.get(),
            PostingWriterResource, &mSimplePool, mByteSlicePool.get(),
            mBufferPool.get(), mIndexFormatOption->GetPostingFormatOption());    

    mBitmapIndexWriter = mWriterCreator->CreateBitmapIndexWriter(
            mOptions.IsOnline(), mByteSlicePool.get(), &mSimplePool);
    
    if (indexConfig->GetShardingType() != IndexConfig::IST_IS_SHARDING)
    {
        mSectionAttributeWriter = mWriterCreator->CreateSectionAttributeWriter(
                buildResourceMetrics);
    }
    mHighFreqVol = indexConfig->GetHighFreqVocabulary();
    UpdateBuildResourceMetrics();
}

size_t NormalIndexWriter::EstimateInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionSegmentIteratorPtr& segIter)
{
    size_t initSize = PostingTable::EstimateFirstBucketSize(mHashMapInitSize);
    //pool allocat one block size when allocate at init time
    //when pool allocate more than one block size mem, original block left mem useless
    initSize += autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE; 
    return initSize;
}

void NormalIndexWriter::UpdateBuildResourceMetrics()
{
    if (!mBuildResourceMetricsNode)
    {
        return;
    }
    int64_t poolSize = mByteSlicePool->getUsedBytes() +
                       mSimplePool.getUsedBytes() +
                       mBufferPool->getUsedBytes();

    int64_t dumpTempBufferSize = TieredDictionaryWriter<dictkey_t>::GetInitialMemUse();
    int64_t dumpExpandBufferSize = mBufferPool->getUsedBytes() +
                                   mToCompressShortListCount *
                                   UNCOMPRESS_SHORT_LIST_DUMP_EXPAND_FACTOR;
    int64_t dumpFileSize = poolSize * 0.2;
    mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempBufferSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, dumpExpandBufferSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
}

void NormalIndexWriter::AddField(const Field *field)
{
    auto tokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
    if (!tokenizeField)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "fieldTag [%d] is not IndexTokenizeField",
                             static_cast<int8_t>(field->GetFieldTag()));
    }    
    if (tokenizeField->GetSectionCount() <= 0)
    {
        return;
    }

    pos_t tokenBasePos = mBasePos;
    uint32_t fieldLen = 0;
    fieldid_t fieldId = tokenizeField->GetFieldId();

    for (auto iterField = tokenizeField->Begin(); 
         iterField != tokenizeField->End(); ++iterField)
    {
        const Section* section = *iterField;
        fieldLen += section->GetLength();

        for (size_t i = 0; i < section->GetTokenCount(); i++)
        {
            const Token* token = section->GetToken(i);
            tokenBasePos += token->GetPosIncrement();
            AddToken(token, fieldId, tokenBasePos);
        }
    }
    mBasePos += fieldLen;
}

void NormalIndexWriter::AddToken(const Token* token, 
                                 fieldid_t fieldId, pos_t tokenBasePos)
{
    dictkey_t hashKey = token->GetHashKey();
    fieldid_t fieldIdxInPack = mIndexConfig->GetFieldIdxInPack(fieldId);
    if (fieldIdxInPack < 0)
    {
        std::stringstream ss;
        ss << "fieldIdxInPack = " << fieldIdxInPack << " ,fieldId = " << fieldId;
        INDEXLIB_THROW(misc::OutOfRangeException, "%s", ss.str().c_str());
    }
    
    bool isHighFreqTerm = false;
    if (mHighFreqVol)
    {
        isHighFreqTerm = mHighFreqVol->Lookup(hashKey);
    }

    //bitmap index writer should add first for realtime
    if (isHighFreqTerm)
    {
        mBitmapIndexWriter->AddToken(hashKey, token->GetPosPayload());
    }

    if (!isHighFreqTerm 
        || mIndexConfig->GetHighFrequencyTermPostingType() == hp_both
        || mIndexConfig->IsTruncateTerm(hashKey))
    {
        DoAddHashToken(hashKey, token, fieldIdxInPack, tokenBasePos);
    }
}

void NormalIndexWriter::DoAddHashToken(
        dictkey_t hashKey, const Token* token,
        fieldid_t fieldIdxInPack, pos_t tokenBasePos)
{
    dictkey_t retrievalHashKey = IndexWriter::GetRetrievalHashKey(
            mIndexFormatOption->IsNumberIndex(), hashKey);
    PostingWriter** pWriter = mPostingTable->Find(retrievalHashKey);
    if (pWriter == NULL)
    {
        // not found, add a new <term,posting>
        PostingWriter* postingWriter = CreatePostingWriter();
        
        postingWriter->AddPosition(tokenBasePos, token->GetPosPayload(), fieldIdxInPack);

        mPostingTable->Insert(retrievalHashKey, postingWriter);
        mHashKeyVector.push_back(hashKey);
        mModifiedPosting.push_back(std::make_pair(hashKey, postingWriter));
    }
    else 
    {
        // found, append to the end
        if ((*pWriter)->NotExistInCurrentDoc())
        {
            mModifiedPosting.push_back(std::make_pair(hashKey, *pWriter));
        }
        (*pWriter)->AddPosition(tokenBasePos, token->GetPosPayload(), fieldIdxInPack);
    }
}

uint32_t NormalIndexWriter::GetDistinctTermCount() const
{
    uint32_t termCount = mPostingTable->Size();
    if (mBitmapIndexWriter)
    {    
        termCount += mBitmapIndexWriter->GetDistinctTermCount();
    }
    return termCount;
}


uint64_t NormalIndexWriter::GetNormalTermDfSum() const
{
    uint64_t count = 0;
    PostingTable::Iterator it = mPostingTable->CreateIterator();
    while(it.HasNext())
    {
        PostingTable::KeyValuePair& kv = it.Next();
        count += kv.second->GetDF();
    }
    return count;
}

void NormalIndexWriter::EndDocument(const IndexDocument &indexDocument)
{
    for (PostingVector::iterator it = mModifiedPosting.begin();
         it != mModifiedPosting.end(); it++)
    {
        if (mIndexFormatOption->HasTermPayload())
        {
            termpayload_t termpayload = indexDocument.GetTermPayload(it->first);
            it->second->SetTermPayload(termpayload);
        }

        docpayload_t docPayload = mIndexFormatOption->HasDocPayload() ?
                                  indexDocument.GetDocPayload(it->first) : 0;
        it->second->EndDocument(indexDocument.GetDocId(), docPayload);
        uint32_t df = it->second->GetDF();
        if (df == UNCOMPRESS_SHORT_LIST_MIN_LEN)
        {
            mToCompressShortListCount++;
        }
        else if (df == UNCOMPRESS_SHORT_LIST_MAX_LEN + 1)
        {
            mToCompressShortListCount--;
        }
    }
   
    mModifiedPosting.clear();

    if (mSectionAttributeWriter)
    {
	if (!mSectionAttributeWriter->EndDocument(indexDocument))
	{
	    // TODO: FIXME: 
	    PrintIndexDocument(indexDocument);
	}
    }

    if (mBitmapIndexWriter)
    {
        mBitmapIndexWriter->EndDocument(indexDocument);
    }
    mBasePos = 0;
    UpdateBuildResourceMetrics();
}

void NormalIndexWriter::EndSegment()
{
    PostingTable::Iterator it = mPostingTable->CreateIterator();
    while (it.HasNext())
    {
        PostingTable::KeyValuePair& kv = it.Next();
        PostingWriter* pWriter = kv.second;
        if (pWriter)
        {
            pWriter->EndSegment();
        }
    }
    UpdateBuildResourceMetrics();
}

void NormalIndexWriter::Dump(const file_system::DirectoryPtr& directory,
                             PoolBase* dumpPool)
{
    IE_LOG(DEBUG, "Dumping index : [%s]", 
           mIndexConfig->GetIndexName().c_str());
    
    file_system::DirectoryPtr indexDirectory = directory->MakeDirectory(
            mIndexConfig->GetIndexName());
    
    DictionaryWriterPtr dictWriter(
            mWriterCreator->CreateDictionaryWriter(dumpPool));
    dictWriter->Open(indexDirectory, DICTIONARY_FILE_NAME, mHashKeyVector.size());
    mPostingFile = indexDirectory->CreateFileWriter(POSTING_FILE_NAME);

    sort(mHashKeyVector.begin(), mHashKeyVector.end());

    size_t postingFileLen = CalPostingFileLength();
    mPostingFile->ReserveFileNode(postingFileLen);

    for (HashKeyVector::iterator it = mHashKeyVector.begin(); 
         it != mHashKeyVector.end(); ++it)
    {
        PostingWriter** pWriter = mPostingTable->Find(
                IndexWriter::GetRetrievalHashKey(
                        mIndexFormatOption->IsNumberIndex(), *it));
        assert(pWriter != NULL);

        DumpPosting(dictWriter, *pWriter, *it);
    }

    dictWriter->Close();
    dictWriter.reset();

    assert(postingFileLen == mPostingFile->GetLength());
    mPostingFile->Close();

    mIndexFormatOption->Store(indexDirectory);

    if (mBitmapIndexWriter)
    {
        mBitmapIndexWriter->Dump(indexDirectory, dumpPool);
    }

    if (mSectionAttributeWriter)
    {
        mSectionAttributeWriter->Dump(directory, dumpPool);
    }
    UpdateBuildResourceMetrics();
}

void NormalIndexWriter::DumpPosting(const DictionaryWriterPtr& dictWriter,
                                    PostingWriter* writer, dictkey_t key)
{
    uint64_t inlinePostingValue;
    if (writer->GetDictInlinePostingValue(inlinePostingValue))
    {
        DumpDictInlinePosting(dictWriter, inlinePostingValue, key);
    }
    else
    {
        DumpNormalPosting(dictWriter, writer, key);
    }
}

void NormalIndexWriter::DumpDictInlinePosting(const DictionaryWriterPtr& dictWriter,
        uint64_t postingValue, dictkey_t key)
{
    dictvalue_t dictValue = ShortListOptimizeUtil::CreateDictInlineValue(postingValue);
    dictWriter->AddItem(key, dictValue);
}

void NormalIndexWriter::DumpNormalPosting(const DictionaryWriterPtr& dictWriter,
        PostingWriter* writer, dictkey_t key)
{
    index::TermMeta termMeta(writer->GetDF(), writer->GetTotalTF());
    if (mIndexFormatOption->HasTermPayload())
    {
        termMeta.SetPayload(writer->GetTermPayload());
    }
    PostingFormatOption formatOption  = mIndexFormatOption->GetPostingFormatOption();
    TermMetaDumper tmDumper(formatOption);
    uint64_t offset = mPostingFile->GetLength();
    uint32_t postingLen = tmDumper.CalculateStoreSize(termMeta) + writer->GetDumpLength();
    if (formatOption.IsCompressedPostingHeader())
    {
        mPostingFile->WriteVUInt32(postingLen);
    }
    else
    {
        mPostingFile->Write((void*)(&postingLen), sizeof(uint32_t));
    }

    tmDumper.Dump(mPostingFile, termMeta);
    writer->Dump(mPostingFile);
    
    uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(
            writer->GetDF(), writer->GetTotalTF(), 
            mIndexFormatOption->GetPostingFormatOption().GetDocListCompressMode());
    dictvalue_t dictValue = ShortListOptimizeUtil::CreateDictValue(
            compressMode, (int64_t)offset);
    dictWriter->AddItem(key, dictValue);
}

const PostingWriter* NormalIndexWriter::GetPostingListWriter(
        dictkey_t key) const
{
    PostingWriter **pWriter = mPostingTable->Find(
            IndexWriter::GetRetrievalHashKey(mIndexFormatOption->IsNumberIndex(), key));
    if (pWriter != NULL)
    {
        return *pWriter;
    }
    return NULL;
}

PostingWriter* NormalIndexWriter::CreatePostingWriter()
{
    assert(mWriterCreator);
    return mWriterCreator->CreatePostingWriter(mPostingWriterResource);
}

IndexSegmentReaderPtr NormalIndexWriter::CreateInMemReader()
{
    AttributeSegmentReaderPtr sectionReader;
    if (mSectionAttributeWriter)
    {
        sectionReader = mSectionAttributeWriter->CreateInMemReader();
    }

    InMemBitmapIndexSegmentReaderPtr bitmapReader;
    if (mBitmapIndexWriter)
    {
        bitmapReader = mBitmapIndexWriter->CreateInMemReader();
    }

    InMemNormalIndexSegmentReaderPtr indexSegmentReader(
            new InMemNormalIndexSegmentReader(mPostingTable, sectionReader, 
                    bitmapReader, *mIndexFormatOption));
    return indexSegmentReader;
}

BitmapPostingWriter* NormalIndexWriter::GetBitmapPostingWriter(dictkey_t hashKey)
{
    if (!mBitmapIndexWriter)
    {
        return NULL;
    }
    return mBitmapIndexWriter->GetBitmapPostingWriter(hashKey);
}

void NormalIndexWriter::PrintIndexDocument(const IndexDocument& indexDocument)
{
    IndexDocument::Iterator it(indexDocument);
    std::cerr << "doc start, docid = " << indexDocument.GetDocId() << std::endl;
    while(it.HasNext())
    {
        const Field* field = it.Next();
        const auto tokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
        if (!tokenizeField)
        {
            continue;
        }
        fieldid_t fieldId = tokenizeField->GetFieldId();
        if (this->mIndexConfig->IsInIndex(fieldId))
        {
            std::cerr << "field start, fieldid = " << fieldId << std::endl;
                        
            if (tokenizeField->GetSectionCount() > 0)
            {
                for (auto iterField = tokenizeField->Begin(); iterField != tokenizeField->End(); ++iterField)
                {
                    const Section* section = *iterField;

                    std::cerr << "section start, section length = " << 
                        section->GetLength() << std::endl;
                    
                    for (size_t i = 0; i < section->GetTokenCount(); i++)
                    {
                        const Token* token = section->GetToken(i);
                        std::cerr << "token PosIncrement = " << 
                            token->GetPosIncrement() << std::endl;
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
    while(iter.HasNext())
    {
        PostingTable::KeyValuePair& kv = iter.Next();
        PostingWriter* postingWriter = kv.second;
        uint64_t dictInlineValue;
        if (postingWriter->GetDictInlinePostingValue(dictInlineValue))
        {
            continue;
        }

        PostingFormatOption formatOption = mIndexFormatOption->GetPostingFormatOption();
        
        index::TermMeta termMeta(postingWriter->GetDF(), postingWriter->GetTotalTF());
        TermMetaDumper tmDumper(formatOption);
        if (mIndexFormatOption->HasTermPayload())
        {
            termMeta.SetPayload(postingWriter->GetTermPayload());
        }
        uint32_t curLength = tmDumper.CalculateStoreSize(termMeta) +
                                 postingWriter->GetDumpLength();
        if (formatOption.IsCompressedPostingHeader())
        {
            totalLen = totalLen + VByteCompressor::GetVInt32Length(curLength)
                       + curLength;
        }
        else
        {
            totalLen = totalLen + sizeof(uint32_t) + curLength;
        }
    }
    return totalLen;
}

IE_NAMESPACE_END(index);

