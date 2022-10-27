#ifndef __INDEXLIB_PRIMARY_KEY_ATTRIBUTE_MERGER_H
#define __INDEXLIB_PRIMARY_KEY_ATTRIBUTE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_merger.h"
#include "indexlib/index/normal/primarykey/in_mem_primary_key_segment_reader_typed.h"

IE_NAMESPACE_BEGIN(index);

template <typename Key>
class PrimaryKeyAttributeMerger : public SingleValueAttributeMerger<Key>
{
public:
    PrimaryKeyAttributeMerger(const std::string& indexName)
        : SingleValueAttributeMerger<Key>(false)
        , mIndexName(indexName)
    {
    }
    ~PrimaryKeyAttributeMerger() {}

protected:
    void OpenSingleValueAttributeReader(
        SingleAttributeSegmentReaderForMerge<Key>& reader,
        const index_base::SegmentInfo& segInfo, segmentid_t segmentId) override;

    file_system::FileWriterPtr CreateDataFileWriter(
        const file_system::DirectoryPtr& indexDirectory) override;

    std::string GetPrimaryKeyAttributeDirName(const std::string &attrName);
private:
    std::string mIndexName;
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, PrimaryKeyAttributeMerger);

template <typename Key>
void PrimaryKeyAttributeMerger<Key>::OpenSingleValueAttributeReader(
        SingleAttributeSegmentReaderForMerge<Key>& reader,
        const index_base::SegmentInfo& segInfo, segmentid_t segmentId)
{
    index_base::PartitionDataPtr partitionData = 
        this->mSegmentDirectory->GetPartitionData();
    assert(partitionData);

    std::string pkAttrDirName = GetPrimaryKeyAttributeDirName(
            this->mAttributeConfig->GetAttrName());

    index_base::SegmentData segmentData = 
        partitionData->GetSegmentData(segmentId);
    file_system::DirectoryPtr indexDirectory = 
        segmentData.GetIndexDirectory(mIndexName, true);
    file_system::DirectoryPtr pkAttrDirectory = 
        indexDirectory->GetDirectory(pkAttrDirName, true);
    reader.OpenWithoutPatch(pkAttrDirectory, segInfo, segmentId);    
}

template<typename Key>
std::string PrimaryKeyAttributeMerger<Key>::GetPrimaryKeyAttributeDirName(
        const std::string &attrName)
{
    return std::string(PK_ATTRIBUTE_DIR_NAME_PREFIX) + '_' + attrName;
}

template<typename Key>
inline file_system::FileWriterPtr PrimaryKeyAttributeMerger<Key>::CreateDataFileWriter(
    const file_system::DirectoryPtr& indexDirectory)
{
    file_system::DirectoryPtr pkDirectory = indexDirectory->GetDirectory(mIndexName, true);
    std::string pkAttrDirName = GetPrimaryKeyAttributeDirName(
        this->mAttributeConfig->GetAttrName());
    pkDirectory->RemoveDirectory(pkAttrDirName, true);
    file_system::DirectoryPtr pkAttrDirectory = pkDirectory->MakeDirectory(pkAttrDirName);
    return pkAttrDirectory->CreateFileWriter(ATTRIBUTE_DATA_FILE_NAME);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_ATTRIBUTE_MERGER_H
