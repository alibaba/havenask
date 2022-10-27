#ifndef __INDEXLIB_PRIMARY_KEY_ATTRIBUTE_READER_H
#define __INDEXLIB_PRIMARY_KEY_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/primarykey/in_mem_primary_key_segment_reader_typed.h"
#include "indexlib/index/normal/attribute/accessor/primary_key_attribute_segment_reader.h"
#include "indexlib/index_base/index_meta/version.h"

IE_NAMESPACE_BEGIN(index);

template <typename Key>
class PrimaryKeyAttributeReader : public SingleValueAttributeReader<Key>
{
public:
    typedef PrimaryKeyAttributeSegmentReader<Key> SegmentReader;
    typedef typename SingleValueAttributeReader<Key>::SegmentReaderPtr SegmentReaderPtr;
    typedef typename SingleValueAttributeReader<Key>::InMemSegmentReader InMemSegmentReader;
    typedef typename SingleValueAttributeReader<Key>::InMemSegmentReaderPtr InMemSegmentReaderPtr;
    typedef typename SingleValueAttributeReader<Key>::BuildingAttributeReaderType BuildingAttributeReaderType ;
    typedef std::tr1::shared_ptr<InMemPrimaryKeySegmentReaderTyped<Key> > InMemPrimaryKeySegmentReaderPtr;

public:
    PrimaryKeyAttributeReader(const std::string& indexName);
    PrimaryKeyAttributeReader(const PrimaryKeyAttributeReader<Key>& reader);
    ~PrimaryKeyAttributeReader() {}
public:
    
    AttributeReader* Clone() const override;

private:
    void InitBuildingAttributeReader(
            const index_base::SegmentIteratorPtr& segIter) override;

    file_system::DirectoryPtr GetAttributeDirectory(
            const index_base::SegmentData& segData,
            const config::AttributeConfigPtr& attrConfig) const override;

    SegmentReaderPtr CreateSegmentReader(
            const config::AttributeConfigPtr& attrConfig, 
            AttributeMetrics* attrMetrics = NULL) override
    {
        return SegmentReaderPtr(new SegmentReader(attrConfig));
    }
    size_t EstimateLoadSize(
            const index_base::PartitionDataPtr& partitionData,
            const config::AttributeConfigPtr& attrConfig,
            const index_base::Version& lastLoadVersion) override;
private:
    std::string mIndexName;

private:
    IE_LOG_DECLARE();
};

template<typename Key>
PrimaryKeyAttributeReader<Key>::PrimaryKeyAttributeReader(const std::string& indexName)
    : mIndexName(indexName)
{
}

template<typename Key>
PrimaryKeyAttributeReader<Key>::PrimaryKeyAttributeReader(
        const PrimaryKeyAttributeReader<Key>& reader)
    : SingleValueAttributeReader<Key>((const SingleValueAttributeReader<Key>&)reader)
    , mIndexName(reader.mIndexName)
{
}

template<typename Key>
size_t PrimaryKeyAttributeReader<Key>::EstimateLoadSize(
        const index_base::PartitionDataPtr& partitionData,
        const config::AttributeConfigPtr& attrConfig,         
        const index_base::Version& lastLoadVersion)
{
    index_base::Version version = partitionData->GetVersion();
    index_base::Version diffVersion = version - lastLoadVersion;
    size_t totalSize = 0;
    for (size_t i = 0; i < diffVersion.GetSegmentCount(); ++i)
    {
        const index_base::SegmentData& segData = 
            partitionData->GetSegmentData(diffVersion[i]);
        if (segData.GetSegmentInfo().docCount == 0)
        {
            continue;
        }

        file_system::DirectoryPtr pkAttrDir = 
            GetAttributeDirectory(segData, attrConfig);
        totalSize += pkAttrDir->EstimateIntegratedFileMemoryUse(
                ATTRIBUTE_DATA_FILE_NAME);
    }
    return totalSize;
}

template<typename Key>
file_system::DirectoryPtr PrimaryKeyAttributeReader<Key>::GetAttributeDirectory(
        const index_base::SegmentData& segData,
        const config::AttributeConfigPtr& attrConfig) const
{
    file_system::DirectoryPtr indexDirectory = 
        segData.GetIndexDirectory(mIndexName, true);
    assert(indexDirectory);
    std::string pkAttrDirName = 
        std::string(PK_ATTRIBUTE_DIR_NAME_PREFIX)
        + '_' + attrConfig->GetAttrName();

    return indexDirectory->GetDirectory(pkAttrDirName, true);
}

template<typename Key>
void PrimaryKeyAttributeReader<Key>::InitBuildingAttributeReader(
        const index_base::SegmentIteratorPtr& buildingIter)
{
    if (!buildingIter)
    {
        return;
    }
    while (buildingIter->IsValid())
    {
        index_base::InMemorySegmentPtr inMemorySegment = buildingIter->GetInMemSegment();
        if (!inMemorySegment)
        {
            buildingIter->MoveToNext();
            continue;
        }
        index::InMemorySegmentReaderPtr segmentReader = inMemorySegment->GetSegmentReader();
        index::IndexSegmentReaderPtr pkIndexReader = 
            segmentReader->GetSingleIndexSegmentReader(mIndexName);

        InMemPrimaryKeySegmentReaderPtr inMemPkReader = DYNAMIC_POINTER_CAST(
                InMemPrimaryKeySegmentReaderTyped<Key>, pkIndexReader);
        assert(inMemPkReader);
        AttributeSegmentReaderPtr attrSegmentReader = inMemPkReader->GetPKAttributeReader();
        assert(attrSegmentReader);

        InMemSegmentReaderPtr inMemPkAttrReader = DYNAMIC_POINTER_CAST(
                InMemSegmentReader, attrSegmentReader);
        if (!this->mBuildingAttributeReader)
        {
            this->mBuildingAttributeReader.reset(new BuildingAttributeReaderType);
        }
        this->mBuildingAttributeReader->AddSegmentReader(
                buildingIter->GetBaseDocId(), inMemPkAttrReader);
        buildingIter->MoveToNext();
    }
}

template<typename Key>
AttributeReader* PrimaryKeyAttributeReader<Key>::Clone() const
{
    return new PrimaryKeyAttributeReader<Key>(*this);
}

IE_LOG_SETUP_TEMPLATE(index, PrimaryKeyAttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_ATTRIBUTE_READER_H
