#include "indexlib/index/normal/trie/trie_index_define.h"
#include "indexlib/index/normal/trie/trie_index_segment_reader.h"
#include "indexlib/file_system/file_system_define.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TrieIndexSegmentReader);

DirectoryPtr TrieIndexSegmentReader::GetDirectory(const DirectoryPtr& segmentDirectory,
        const IndexConfigPtr& indexConfig)
{
    assert(segmentDirectory);
    DirectoryPtr indexDirectory = 
        segmentDirectory->GetDirectory(INDEX_DIR_NAME, true);
    assert(indexDirectory);
    return indexDirectory->GetDirectory(indexConfig->GetIndexName(), true);
}

void TrieIndexSegmentReader::Open(const IndexConfigPtr& indexConfig,
                                  const SegmentData& segmentData)
{
    assert(indexConfig);
    DirectoryPtr directory =
        GetDirectory(segmentData.GetDirectory(), indexConfig);
    assert(directory);
    mDataFile = directory->CreateFileReader(
            PRIMARY_KEY_DATA_FILE_NAME, FSOT_MMAP);
    assert(mDataFile);
    uint8_t* trieIndexVersion = (uint8_t*) mDataFile->GetBaseAddress();
    assert(*trieIndexVersion == TRIE_INDEX_VERSION);
    mData = (char*)(trieIndexVersion + 1);
    assert(mData);
}

IE_NAMESPACE_END(index);

