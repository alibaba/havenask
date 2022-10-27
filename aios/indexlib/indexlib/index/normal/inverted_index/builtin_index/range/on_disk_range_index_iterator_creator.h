#ifndef __INDEXLIB_ON_DISK_RANGE_INDEX_ITERATOR_CREATOR_H
#define __INDEXLIB_ON_DISK_RANGE_INDEX_ITERATOR_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/on_disk_pack_index_iterator.h"

IE_NAMESPACE_BEGIN(index);

class OnDiskRangeIndexIteratorCreator : public OnDiskIndexIteratorCreator
{
public:
    typedef OnDiskPackIndexIterator OnDiskRangeIndexIterator;
    OnDiskRangeIndexIteratorCreator(const index::PostingFormatOption postingFormatOption, 
                                    const config::MergeIOConfig &ioConfig,
                                    const config::IndexConfigPtr &indexConfig,
                                    const std::string& parentIndexName)
        : mPostingFormatOption(postingFormatOption)
        , mIOConfig(ioConfig)
        , mIndexConfig(indexConfig)
        , mParentIndexName(parentIndexName)
    {}

    ~OnDiskRangeIndexIteratorCreator(){}
public:
    OnDiskIndexIterator* Create(
            const index_base::SegmentData& segData) const override;

    OnDiskIndexIterator* CreateBitmapIterator(
            const file_system::DirectoryPtr& indexDirectory) const override
    {
        assert(false);
        return NULL;
    }

private:
    index::PostingFormatOption mPostingFormatOption;   
    config::MergeIOConfig mIOConfig;                                
    config::IndexConfigPtr mIndexConfig;
    std::string mParentIndexName;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskRangeIndexIteratorCreator);
///////////////////////////////////////////////////////////

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ON_DISK_RANGE_INDEX_ITERATOR_CREATOR_H
