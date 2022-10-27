#include "indexlib/index/normal/trie/trie_index_define.h"
#include "indexlib/index/normal/trie/trie_index_merger.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TrieIndexMerger);

TrieIndexMerger::TrieIndexMerger() 
{
}

TrieIndexMerger::~TrieIndexMerger() 
{
}

void TrieIndexMerger::ReadSegment(const SegmentData& segmentData,
                                  const index::MergerResource& resource,
                                  vector<KVPairVector>& kvVectors)
{
    ReclaimMapPtr reclaimMap = resource.reclaimMap;
    if (!reclaimMap)
    {
        IE_LOG(ERROR, "reclaimMap is null");
        return;
    }
    DirectoryPtr directory = segmentData.GetIndexDirectory(
            GetIndexConfig()->GetIndexName(), true);
    assert(directory);
    FileReaderPtr fileReader = directory->CreateFileReader(
            TRIE_ORIGINAL_DATA, FSOT_BUFFERED);

    docid_t baseDocid = segmentData.GetBaseDocId();    
    size_t fileLength = fileReader->GetLength();
    //skip header
    size_t cursor = sizeof(uint32_t);
    while(cursor < fileLength)
    {
        docid_t docid;
        cursor += ReadData(fileReader, cursor, sizeof(docid), (void*)&docid);
        
        uint32_t keyLength;
        cursor += ReadData(fileReader, cursor,
                           sizeof(keyLength), (void*)&keyLength);
        char* buffer = (char*)mPool.allocate(keyLength);
        cursor += ReadData(fileReader, cursor, keyLength, (void*)buffer);
        docid_t newDocid = reclaimMap->GetNewId(baseDocid + docid);
        if (newDocid != INVALID_DOCID)
        {
            auto localDocInfo = reclaimMap->GetLocalId(newDocid);
            auto& kvVector = kvVectors[localDocInfo.first];
            kvVector.push_back(make_pair(ConstString(buffer, keyLength), localDocInfo.second));
        }
    }
}

size_t TrieIndexMerger::ReadData(const FileReaderPtr& fileReader, size_t cursor,
                                 size_t length, void* buffer) const
{
    size_t readLen = fileReader->Read(buffer, length, cursor);
    if (readLen != length)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "read trie raw data file[%s] fail, file collapse!",
                             fileReader->GetPath().c_str());
    }
    return readLen;
}

bool LessFirst(const DoubleArrayTrie::KVPair& left,
               const DoubleArrayTrie::KVPair& right)
{
    return left.first < right.first;
}

void TrieIndexMerger::MergeRawData(
    const index::MergerResource& resource,
    const SegmentMergeInfos& segMergeInfos,
    vector<KVPairVector>& sortedVectors)
{
    // TODO: use multiway merge
    for (auto& sortedVector : sortedVectors)
    {
        sortedVector.clear();
    }
    const SegmentDirectoryBasePtr& segDir = GetSegmentDirectory();
    const PartitionDataPtr& partData = segDir->GetPartitionData();
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        const SegmentData& segmentData =
            partData->GetSegmentData(segMergeInfos[i].segmentId);
        const SegmentInfo& segInfo = segmentData.GetSegmentInfo();
        if (segInfo.docCount == 0)
        {
            continue;
        }
        ReadSegment(segmentData, resource, sortedVectors);
    }
    for(auto& sortedVector : sortedVectors)
    {
        sort(sortedVector.begin(), sortedVector.end(), LessFirst);
    }
}

void TrieIndexMerger::InnerMerge(
    const index::MergerResource& resource,
    const SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) 
{    
    IE_LOG(INFO, "merge trie data begin");

    KVPairVector::allocator_type alocator(&mSimplePool);

    vector<KVPairVector> sortedVectors;
    for (size_t i = 0; i < outputSegMergeInfos.size(); ++i)
    {
        KVPairVector sortedVector(alocator);
        sortedVectors.push_back(sortedVector);
    }

    MergeRawData(resource, segMergeInfos, sortedVectors);

    for (size_t i = 0; i < outputSegMergeInfos.size(); ++i)
    {
        DoubleArrayTrie trie(&mSimplePool);
        if (!trie.Build(sortedVectors[i]))
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "trie build fail");
            return;
        }
        const ConstString& data = trie.Data();
        const DirectoryPtr& indexDir = outputSegMergeInfos[i].directory;
        const DirectoryPtr& mergeDir = GetMergeDir(indexDir, true);
        FSWriterParam fsWriterParam(mIOConfig.writeBufferSize, mIOConfig.enableAsyncWrite);
        const FileWriterPtr& fileWriter= 
            mergeDir->CreateFileWriter(PRIMARY_KEY_DATA_FILE_NAME, fsWriterParam);
        fileWriter->Write(&TRIE_INDEX_VERSION, sizeof(TRIE_INDEX_VERSION));
        fileWriter->Write(data.data(), data.size());
        fileWriter->Close();
    }
    IE_LOG(INFO, "merge trie data end");
    mPool.release();
}

int64_t TrieIndexMerger::EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, 
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        bool isSortedMerge)
{
    const PartitionDataPtr& partData = segDir->GetPartitionData();
    size_t sortedVectMem = 0;
    size_t trieDataMem = 0;
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        const SegmentData& segmentData =
            partData->GetSegmentData(segMergeInfos[i].segmentId);
        const SegmentInfo& segInfo = segmentData.GetSegmentInfo();
        if (segInfo.docCount == 0)
        {
            continue;
        }
        DirectoryPtr directory = segmentData.GetIndexDirectory(
                GetIndexConfig()->GetIndexName(), true);

        //estimate sorted vector
        FileReaderPtr fileReader = directory->CreateFileReader(
                TRIE_ORIGINAL_DATA, FSOT_BUFFERED);
        uint32_t itemCount;
        ReadData(fileReader, 0, sizeof(itemCount), (void*)&itemCount);
        sortedVectMem += fileReader->GetLength() - sizeof(uint32_t) +
                         (sizeof(ConstString) - sizeof(uint32_t)) * itemCount;
        fileReader->Close();
        //estimate trie tree data size
        trieDataMem += directory->GetFileLength(PRIMARY_KEY_DATA_FILE_NAME);
    }

    size_t initTrieDataMem = DoubleArrayTrie::GetFirstReserveSize();
    trieDataMem = trieDataMem > initTrieDataMem ? trieDataMem : initTrieDataMem;
    return trieDataMem + sortedVectMem;
}

IE_NAMESPACE_END(index);

