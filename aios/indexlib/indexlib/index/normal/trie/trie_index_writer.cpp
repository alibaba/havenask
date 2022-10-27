#include "indexlib/index/normal/trie/trie_index_define.h"
#include "indexlib/index/normal/trie/trie_index_writer.h"
#include "indexlib/index/normal/trie/in_mem_trie_segment_reader.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TrieIndexWriter);

TrieIndexWriter::TrieIndexWriter()
    : mKVMap(less<ConstString>(),
             pool_allocator<pair<ConstString, docid_t> >(&mSimplePool))
{
}

void TrieIndexWriter::UpdateBuildResourceMetrics()
{
    if (!mBuildResourceMetricsNode)
    {
        return;
    }
    int64_t poolSize = mByteSlicePool->getUsedBytes() +
                       mSimplePool.getUsedBytes();

    // TODO: adopt more accurate estimation
    int64_t dumpTempBufferSize = (DoubleArrayTrie::EstimateMemoryUseFactor() + 
                                  sizeof(KVPairVector::value_type)) * mKVMap.size();
    int64_t dumpExpandBufferSize = 0;
    int64_t dumpFileSize = poolSize * 3.5;
    mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempBufferSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, dumpExpandBufferSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
}

void TrieIndexWriter::EndDocument(const document::IndexDocument& indexDocument)
{
    ConstString key(indexDocument.GetPrimaryKey(), mByteSlicePool.get());
    docid_t docId = indexDocument.GetDocId();
    {
        ScopedReadWriteLock lock(mDataLock, 'W');
        mKVMap[key] = docId;
    }
    UpdateBuildResourceMetrics();    
}

void TrieIndexWriter::DumpTrieIndex(const KVPairVector& sortedVector, 
                                    const DirectoryPtr& indexDirectory,
                                    PoolBase* dumpPool)
{
    DoubleArrayTrie trie(dumpPool);
    if (!trie.Build(sortedVector))
    {
        IE_LOG(ERROR, "build trie index failed");
        ERROR_COLLECTOR_LOG(ERROR, "build trie index failed");
        return;
    }
    IE_LOG(INFO, "Compression Ratio: %f%%", trie.CompressionRatio() * 100.0);

    const ConstString& data = trie.Data();
    file_system::FileWriterPtr fileWriter = indexDirectory->CreateFileWriter(
            PRIMARY_KEY_DATA_FILE_NAME);
    IE_LOG(INFO, "index pool size [%ld], trie index size [%ld]",
           mByteSlicePool->getUsedBytes(), data.size());
    fileWriter->Write(&TRIE_INDEX_VERSION, sizeof(TRIE_INDEX_VERSION));
    fileWriter->Write(data.data(), data.size());
    fileWriter->Close();
}

void TrieIndexWriter::DumpRawData(const KVPairVector& sortedVector,
                                  const DirectoryPtr& indexDirectory)
{
    file_system::FileWriterPtr fileWriter = indexDirectory->CreateFileWriter(
            TRIE_ORIGINAL_DATA);
    uint32_t dataNum = sortedVector.size();
    fileWriter->Write(&dataNum, sizeof(dataNum));
    for (size_t i = 0; i < dataNum; ++i)
    {
        const ConstString& key = sortedVector[i].first;
        uint32_t keyLength = key.size();
        docid_t docid = sortedVector[i].second;
        fileWriter->Write((void*)&docid, sizeof(docid));
        fileWriter->Write((void*)&keyLength, sizeof(keyLength));
        fileWriter->Write((void*)key.data(), key.size());
    }
    fileWriter->Close();
}

void TrieIndexWriter::Dump(const DirectoryPtr& directory, PoolBase* dumpPool)
{
    IE_LOG(DEBUG, "Start Dumping index: [%s]", mIndexConfig->GetIndexName().c_str());
    KVPairVector::allocator_type allocator(dumpPool);
    KVPairVector sortedVector(allocator);
    {
        ScopedReadWriteLock lock(mDataLock, 'R');
        sortedVector.assign(mKVMap.begin(), mKVMap.end());
    }
    file_system::DirectoryPtr indexDirectory = 
        directory->MakeDirectory(mIndexConfig->GetIndexName());

    DumpRawData(sortedVector, indexDirectory);
    DumpTrieIndex(sortedVector, indexDirectory, dumpPool);

    IE_LOG(DEBUG, "Finish Dumping index: [%s]", mIndexConfig->GetIndexName().c_str());
    UpdateBuildResourceMetrics();
}

uint32_t TrieIndexWriter::GetDistinctTermCount() const
{
    ScopedReadWriteLock lock(mDataLock, 'R');
    return mKVMap.size();
}

index::IndexSegmentReaderPtr TrieIndexWriter::CreateInMemReader()
{
    index::IndexSegmentReaderPtr inMemReader(
            new InMemTrieSegmentReader(&mDataLock, &mKVMap));
    return inMemReader;
}

void TrieIndexWriter::GetDumpEstimateFactor(std::priority_queue<double>& factors,
        std::priority_queue<size_t>& minMemUses) const
{
    factors.push(DoubleArrayTrie::EstimateMemoryUseFactor() + 
                 sizeof(KVPairVector::value_type));
    minMemUses.push(DoubleArrayTrie::EstimateMemoryUse(0));
}

IE_NAMESPACE_END(index);

