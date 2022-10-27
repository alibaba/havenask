#ifndef __INDEXLIB_ON_DISK_INDEX_ITERATOR_H
#define __INDEXLIB_ON_DISK_INDEX_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_iterator.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_reader.h"
#include "fslib/common/common_type.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"

IE_NAMESPACE_BEGIN(index);

class OnDiskIndexIterator : public IndexIterator
{
public:
    OnDiskIndexIterator(const file_system::DirectoryPtr& indexDirectory,
                        const index::PostingFormatOption& postingFormatOption, 
                        const config::MergeIOConfig &ioConfig = config::MergeIOConfig())
        : mIndexDirectory(indexDirectory)
        , mPostingFormatOption(postingFormatOption)
        , mIOConfig(ioConfig)
    {
    }

    virtual ~OnDiskIndexIterator(){}
    
public:
    virtual void Init() = 0;

protected:
    file_system::DirectoryPtr mIndexDirectory;
    index::PostingFormatOption mPostingFormatOption;
    config::MergeIOConfig mIOConfig;
};

DEFINE_SHARED_PTR(OnDiskIndexIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ON_DISK_INDEX_ITERATOR_H
