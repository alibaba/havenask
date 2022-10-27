#ifndef __INDEXLIB_ON_DISK_INDEX_ITERATOR_CREATOR_H
#define __INDEXLIB_ON_DISK_INDEX_ITERATOR_CREATOR_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/file_system/directory.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"

IE_NAMESPACE_BEGIN(index);

#define DECLARE_ON_DISK_INDEX_ITERATOR_CREATOR(classname)               \
    class Creator : public OnDiskIndexIteratorCreator                   \
    {                                                                   \
    public:                                                             \
        Creator(const index::PostingFormatOption postingFormatOption, \
                const config::MergeIOConfig &ioConfig,                  \
                const config::IndexConfigPtr& indexConfig)              \
            : mPostingFormatOption(postingFormatOption)                 \
            , mIOConfig(ioConfig)                                       \
            , mIndexConfig(indexConfig)                                 \
        {}                                                              \
        OnDiskIndexIterator* Create(                                    \
                const index_base::SegmentData& segData) const            \
        {                                                               \
            file_system::DirectoryPtr indexDirectory = segData.GetIndexDirectory( \
                mIndexConfig->GetIndexName(), false);                   \
            if (indexDirectory &&                                       \
                indexDirectory->IsExist(DICTIONARY_FILE_NAME))          \
            {                                                           \
                return (new classname(mIndexConfig, indexDirectory,     \
                                mPostingFormatOption, mIOConfig));      \
            }                                                           \
            return NULL;                                                \
        }                                                               \
    private:                                                            \
        index::PostingFormatOption mPostingFormatOption;       \
        config::MergeIOConfig mIOConfig;                                \
        config::IndexConfigPtr mIndexConfig;                            \
    };


class OnDiskIndexIteratorCreator
{
public:
    OnDiskIndexIteratorCreator();
    virtual ~OnDiskIndexIteratorCreator();

public:
    virtual OnDiskIndexIterator* Create(
            const index_base::SegmentData& segmentData) const = 0;

    virtual OnDiskIndexIterator* CreateBitmapIterator(
            const file_system::DirectoryPtr& indexDirectory) const;
};

DEFINE_SHARED_PTR(OnDiskIndexIteratorCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ON_DISK_INDEX_ITERATOR_CREATOR_H
