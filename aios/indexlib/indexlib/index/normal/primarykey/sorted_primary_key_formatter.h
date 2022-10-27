#ifndef __INDEXLIB_SORTED_PRIMARY_KEY_FORMATTER_H
#define __INDEXLIB_SORTED_PRIMARY_KEY_FORMATTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include <autil/LongHashValue.h>
#include "indexlib/common_define.h"
#include "indexlib/file_system/slice_file.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/slice_file_writer.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"

IE_NAMESPACE_BEGIN(index);

template <typename Key>
class SortedPrimaryKeyFormatter : public PrimaryKeyFormatter<Key>
{
public:
    typedef index::PKPair<Key> PKPair;
    typedef util::HashMap<Key, docid_t> HashMapType;
    typedef std::tr1::shared_ptr<HashMapType> HashMapTypePtr;
    typedef std::tr1::shared_ptr<PrimaryKeyIterator<Key> > PrimaryKeyIteratorPtr;
    typedef std::tr1::shared_ptr<OrderedPrimaryKeyIterator<Key> > OrderedPrimaryKeyIteratorPtr;

public:
    SortedPrimaryKeyFormatter() {}
    ~SortedPrimaryKeyFormatter() {}

public:
    static docid_t Find(PKPair* data, size_t count, Key key) __ALWAYS_INLINE;

    void Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
                const file_system::FileWriterPtr& fileWriter) const override;

    void Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
        SegmentOutputMapper<PKMergeOutputData>& outputMapper) const override;    

    void SerializeToFile(const HashMapTypePtr& hashMap,
                                           size_t docCount,
                                           autil::mem_pool::PoolBase* pool,
                         const file_system::FileWriterPtr& file) const override;
    
    size_t CalculatePkSliceFileLen(size_t pkCount, size_t docCount) const override
    {  return pkCount * sizeof(PKPair); }
    void DeserializeToSliceFile(const PrimaryKeyIteratorPtr& pkIterator,
                                               const file_system::SliceFilePtr& sliceFile,
                                               size_t sliceFileLen) override;
    size_t EstimatePkCount(size_t fileLength, uint32_t docCount) const override;
    
    static void ReadPKPair(const file_system::FileReaderPtr& fileReader, PKPair& pkPair);

private:
    void SerializeToBuffer(char* buffer, size_t bufferSize,
                              const HashMapTypePtr& hashMap) const;
private:
    IE_LOG_DECLARE();
    friend class SortedPrimaryKeyFormatterTest;
};

///////////////////////////////////////////////////////////
template <typename Key>
inline docid_t SortedPrimaryKeyFormatter<Key>::Find(
        PKPair* data, size_t count, Key key)
{
    PKPair* begin = data;
    PKPair* end = data + count;
    PKPair* iter = std::lower_bound(begin, end, key);
    if (iter != end && iter->key == key)
    {
        return iter->docid;
    }
    return INVALID_DOCID;
}

template <typename Key>
void SortedPrimaryKeyFormatter<Key>::Format(
    const OrderedPrimaryKeyIteratorPtr& pkIter,
    const file_system::FileWriterPtr& fileWriter) const
{
    assert(pkIter);
    assert(fileWriter);
    PKPair pkPair;
    while (pkIter->HasNext())
    {
        pkIter->Next(pkPair);
        fileWriter->Write((void*)&pkPair, sizeof(pkPair));
    }
}

template <typename Key>
void SortedPrimaryKeyFormatter<Key>::Format(
    const OrderedPrimaryKeyIteratorPtr& pkIter,
    SegmentOutputMapper<PKMergeOutputData>& outputMapper) const
{
    assert(pkIter);

    PKPair pkPair;
    const auto& reclaimMap = outputMapper.GetReclaimMap();
    while (pkIter->HasNext())
    {
        pkIter->Next(pkPair);
        auto localInfo = reclaimMap->GetLocalId(pkPair.docid);
        auto output = outputMapper.GetOutputBySegIdx(localInfo.first);
        if (!output)
        {
            continue;
        }
        pkPair.docid = localInfo.second;
        output->fileWriter->Write((void*)&pkPair, sizeof(pkPair));
    }    
}

template <typename Key> 
inline void SortedPrimaryKeyFormatter<Key>::SerializeToBuffer(
    char* buffer, size_t bufferSize, const HashMapTypePtr& hashMap) const
{
    assert(buffer);
    assert(hashMap);
    PKPair* dataBuffer = (PKPair*)buffer;
    size_t count = hashMap->Size();
    assert(bufferSize == count * sizeof(PKPair));
    typename HashMapType::Iterator it = hashMap->CreateIterator();
    size_t pkBufferIdx = 0;
    while (it.HasNext())
    {
        typename HashMapType::KeyValuePair p = it.Next();
        dataBuffer[pkBufferIdx].key = p.first;
        dataBuffer[pkBufferIdx].docid = p.second;
        pkBufferIdx++;
    }
    std::sort(dataBuffer, dataBuffer + count);
}

template <typename Key>
void SortedPrimaryKeyFormatter<Key>::SerializeToFile(
    const HashMapTypePtr& hashMap,
    size_t docCount,
    autil::mem_pool::PoolBase* pool,
    const file_system::FileWriterPtr& file) const
{
    assert(hashMap);
    assert(file);
    size_t count = hashMap->Size();
    size_t bufLen = count * sizeof(PKPair);
    char* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, char, bufLen);
    SerializeToBuffer(buffer, bufLen, hashMap);
    file->ReserveFileNode(bufLen);
    file->Write((void*)buffer, bufLen);
    assert(file->GetLength() == bufLen);
    IE_POOL_COMPATIBLE_DELETE_VECTOR(pool, buffer, bufLen);
}

template <typename Key>
void SortedPrimaryKeyFormatter<Key>::DeserializeToSliceFile(
    const PrimaryKeyIteratorPtr& pkIterator,
    const file_system::SliceFilePtr& sliceFile,
    size_t sliceFileLen)
{
    file_system::SliceFileWriterPtr sliceFileWriter = 
        sliceFile->CreateSliceFileWriter();
    assert(sliceFileWriter);
    std::string pkFilePath = sliceFileWriter->GetPath();
    IE_LOG(DEBUG, "begin load pk [%s] to sort array",
           pkFilePath.c_str());
    PKPair pkPair;
    while (pkIterator->HasNext())
    {
        pkIterator->Next(pkPair);
        sliceFileWriter->Write((void*)&pkPair, sizeof(PKPair));
    }
    sliceFileWriter->Close();
    IE_LOG(DEBUG, "end load pk [%s] to sort array",
           pkFilePath.c_str());
}

template <typename Key>
inline void SortedPrimaryKeyFormatter<Key>::ReadPKPair(
        const file_system::FileReaderPtr& fileReader, PKPair& pkPair)
{
    fileReader->Read((void*)&pkPair, sizeof(PKPair));
}

template <typename Key>
size_t SortedPrimaryKeyFormatter<Key>::EstimatePkCount(
    size_t fileLength, uint32_t docCount) const
{
    return fileLength / sizeof(PKPair);
}

IE_LOG_SETUP_TEMPLATE(index, SortedPrimaryKeyFormatter);
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SORTED_PRIMARY_KEY_FORMATTER_H
 
