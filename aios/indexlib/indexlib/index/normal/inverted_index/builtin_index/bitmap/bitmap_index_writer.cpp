#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_writer.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BitmapIndexWriter);

BitmapIndexWriter::BitmapIndexWriter(
        Pool* byteSlicePool, SimplePool* simplePool,
        bool isNumberIndex, bool isRealTime) 
    : mByteSlicePool(byteSlicePool)
    , mSimplePool(simplePool)
    , mBitmapPostingTable(byteSlicePool, HASHMAP_INIT_SIZE)
    , mHashKeyVector(pool_allocator<uint64_t>(simplePool))
    , mModifiedPosting(pool_allocator<PostingPair>(simplePool))
    , mIsRealTime(isRealTime)
    , mIsNumberIndex(isNumberIndex)
{
}

BitmapIndexWriter::~BitmapIndexWriter() 
{
    mByteSlicePool = NULL;
    mSimplePool = NULL;
    BitmapPostingTable::Iterator it = mBitmapPostingTable.CreateIterator();
    while (it.HasNext())
    {
        BitmapPostingTable::KeyValuePair& kv = it.Next();
        BitmapPostingWriter* pWriter = kv.second;
        if (pWriter)
        {
            pWriter->~BitmapPostingWriter();
        }
    }
    mBitmapPostingTable.Clear();
}

void BitmapIndexWriter::AddToken(const uint64_t hashKey, pospayload_t posPayload)
{
    dictkey_t retrievalHashKey = IndexWriter::GetRetrievalHashKey(
            mIsNumberIndex, hashKey);
    BitmapPostingWriter** pWriter = mBitmapPostingTable.Find(retrievalHashKey);
    if (pWriter == NULL)
    {
        void* ptr = mByteSlicePool->allocate(sizeof(BitmapPostingWriter));
        PoolBase* pool = mIsRealTime ? (PoolBase* )mByteSlicePool :  (PoolBase* )mSimplePool;
        BitmapPostingWriter* writer = new(ptr) BitmapPostingWriter(pool);
        //pos doesn't make sense for BitmapPostingWriter
        writer->AddPosition(0, posPayload);
        mBitmapPostingTable.Insert(retrievalHashKey, writer);
        mHashKeyVector.push_back(hashKey);
        mModifiedPosting.push_back(make_pair(hashKey, writer));
    }
    else 
    {
        if ((*pWriter)->NotExistInCurrentDoc())
        {
            mModifiedPosting.push_back(make_pair(hashKey, *pWriter));
        }
        //pos doesn't make sense for BitmapPostingWriter
        (*pWriter)->AddPosition(0, posPayload);
    }
}

void BitmapIndexWriter::EndDocument(const IndexDocument& indexDocument)
{
    for (BitmapPostingVector::iterator it = mModifiedPosting.begin();
         it != mModifiedPosting.end(); it++)
    {
        termpayload_t termpayload = indexDocument.GetTermPayload(it->first);
        it->second->SetTermPayload(termpayload);
        it->second->EndDocument(indexDocument.GetDocId(), 
                                indexDocument.GetDocPayload(it->first));
    }
        
    mModifiedPosting.clear();
}

void BitmapIndexWriter::Dump(const file_system::DirectoryPtr& dir,
                             PoolBase* dumpPool)
{
    if (mHashKeyVector.size() == 0)
    {
        return;
    }

    TieredDictionaryWriter<dictkey_t> dictionaryWriter(dumpPool);
    dictionaryWriter.Open(dir, BITMAP_DICTIONARY_FILE_NAME, mHashKeyVector.size());

    file_system::FileWriterPtr postingFile = dir->CreateFileWriter(
            BITMAP_POSTING_FILE_NAME);

    size_t postingFileLen = GetDumpPostingFileLength();
    postingFile->ReserveFileNode(postingFileLen);

    sort(mHashKeyVector.begin(), mHashKeyVector.end());
    for (HashKeyVector::const_iterator it = mHashKeyVector.begin(); 
         it != mHashKeyVector.end(); ++it)
    {
        BitmapPostingWriter** pWriter = mBitmapPostingTable.Find(
                IndexWriter::GetRetrievalHashKey(mIsNumberIndex, *it));
        assert(pWriter != NULL);
        DumpPosting(*pWriter, *it, postingFile, dictionaryWriter);
    }

    assert(postingFile->GetLength() == postingFileLen);
    dictionaryWriter.Close();
    postingFile->Close();
}

void BitmapIndexWriter::DumpPosting(BitmapPostingWriter* writer, 
                                    uint64_t key, 
                                    const file_system::FileWriterPtr& postingFile,
                                    TieredDictionaryWriter<dictkey_t>& dictionaryWriter)
{
    uint64_t offset = postingFile->GetLength();
    dictionaryWriter.AddItem(key, offset);

    uint32_t postingLen = writer->GetDumpLength();
    postingFile->Write((void*)(&postingLen), sizeof(uint32_t));
    writer->Dump(postingFile);
}

InMemBitmapIndexSegmentReaderPtr BitmapIndexWriter::CreateInMemReader()
{
    InMemBitmapIndexSegmentReaderPtr reader(new InMemBitmapIndexSegmentReader(
                    &mBitmapPostingTable, mIsNumberIndex));
    return reader;
}

size_t BitmapIndexWriter::GetDumpPostingFileLength() const
{
    size_t totalLength = 0;
    BitmapPostingTable::Iterator iter = mBitmapPostingTable.CreateIterator();
    while (iter.HasNext())
    {
        BitmapPostingTable::KeyValuePair p = iter.Next();
        totalLength += sizeof(uint32_t); // postingLen
        totalLength += p.second->GetDumpLength();
    }
    return totalLength;
}

IE_NAMESPACE_END(index);

