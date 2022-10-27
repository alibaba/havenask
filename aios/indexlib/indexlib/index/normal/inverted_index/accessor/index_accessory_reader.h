#ifndef __INDEXLIB_INDEX_ACCESSORY_READER_H
#define __INDEXLIB_INDEX_ACCESSORY_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, IndexSchema);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(config, PackageIndexConfig);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index, SectionAttributeReaderImpl);

IE_NAMESPACE_BEGIN(index);

class IndexAccessoryReader
{
public:
    typedef std::map<std::string, size_t> SectionReaderPosMap;
    typedef std::vector<SectionAttributeReaderImplPtr> SectionReaderVec;

    typedef std::vector<std::string> ShareSectionIndexNames;
    typedef std::map<std::string, ShareSectionIndexNames> ShareSectionMap;

public:
    IndexAccessoryReader(const config::IndexSchemaPtr& schema);
    IndexAccessoryReader(const IndexAccessoryReader &other);
    virtual ~IndexAccessoryReader();

public:
    bool Open(const index_base::PartitionDataPtr& partitionData);

    //virtual for ut
    virtual const SectionAttributeReaderImplPtr GetSectionReader(
            const std::string& indexName) const;

    IndexAccessoryReader* Clone() const;

private:
    void CloneSectionReaders(const SectionReaderVec& srcReaders,
                             SectionReaderVec& clonedReaders);

    bool CheckSectionConfig(const config::IndexConfigPtr& indexConfig);

    bool InitSectionReader(const config::IndexConfigPtr& indexConfig,
                           const index_base::PartitionDataPtr& partitionData,
                           ShareSectionMap &shareMap);

    void AddSharedRelations(
            const config::PackageIndexConfigPtr& packIndexConfig,
            ShareSectionMap &shareMap);
    
    bool UseSharedSectionReader(
            const config::PackageIndexConfigPtr& packIndexConfig); 

    // virtual for test
    virtual SectionAttributeReaderImplPtr OpenSectionReader(
            const config::PackageIndexConfigPtr& indexConfig,
            const index_base::PartitionDataPtr& partitionData);

private:
    config::IndexSchemaPtr mIndexSchema;

    SectionReaderPosMap mSectionReaderMap;
    SectionReaderVec    mSectionReaderVec;

    // for unit test
    friend class IndexAccessoryReaderTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexAccessoryReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_ACCESSORY_READER_H
