#ifndef __INDEXLIB_PRIMARY_KEY_ITERATOR_H
#define __INDEXLIB_PRIMARY_KEY_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/file_system/directory.h"

DECLARE_REFERENCE_CLASS(file_system, FileReader);

IE_NAMESPACE_BEGIN(index);

template <typename Key>
class PrimaryKeyIterator
{
private:
    typedef PKPair<Key> TypedPKPair;
public:
    PrimaryKeyIterator(const config::IndexConfigPtr& indexConfig)
        : mIndexConfig(indexConfig)
    {}
    virtual ~PrimaryKeyIterator() {}
public:
    virtual void Init(const std::vector<index_base::SegmentData>& segmentDatas) = 0;

    virtual uint64_t GetPkCount() const = 0;
    virtual uint64_t GetDocCount() const = 0;
    
    virtual bool HasNext() const = 0;
    // docid in pkPair is globle docid
    virtual void Next(TypedPKPair& pkPair) = 0;

protected:
    file_system::FileReaderPtr OpenPKDataFile(
        const file_system::DirectoryPtr& segmentDirectory) const;
    
protected:
    config::IndexConfigPtr mIndexConfig;
};

template<typename Key>
file_system::FileReaderPtr PrimaryKeyIterator<Key>::OpenPKDataFile(
    const file_system::DirectoryPtr& segmentDirectory) const
{
    assert(segmentDirectory);
    file_system::DirectoryPtr indexDirectory =
        segmentDirectory->GetDirectory(INDEX_DIR_NAME, true);
    assert(indexDirectory);
    file_system::DirectoryPtr pkDirectory = 
        indexDirectory->GetDirectory(mIndexConfig->GetIndexName(), true);
    assert(pkDirectory);
    file_system::FileReaderPtr fileReader = 
        pkDirectory->CreateFileReaderWithoutCache(PRIMARY_KEY_DATA_FILE_NAME, file_system::FSOT_BUFFERED);
    assert(fileReader);
    return fileReader;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_ITERATOR_H
