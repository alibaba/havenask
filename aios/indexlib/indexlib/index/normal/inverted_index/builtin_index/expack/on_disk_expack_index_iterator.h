#ifndef __INDEXLIB_ON_DISK_EXPACK_INDEX_ITERATOR_H
#define __INDEXLIB_ON_DISK_EXPACK_INDEX_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/on_disk_pack_index_iterator.h"
#include "indexlib/index/normal/inverted_index/format/posting_decoder_impl.h"
#include "indexlib/config/merge_io_config.h"

IE_NAMESPACE_BEGIN(index);

class OnDiskExpackIndexIterator : public OnDiskPackIndexIterator
{
public:
    OnDiskExpackIndexIterator(
            const config::IndexConfigPtr& indexConfig,
            const file_system::DirectoryPtr& indexDirectory,
            const index::PostingFormatOption& postingFormatOption,
            const config::MergeIOConfig &ioConfig = config::MergeIOConfig());

    virtual ~OnDiskExpackIndexIterator();

    DECLARE_ON_DISK_INDEX_ITERATOR_CREATOR(OnDiskExpackIndexIterator);    
protected:
    void CreatePostingDecoder() override
    {
        mDecoder.reset(new index::PostingDecoderImpl(mPostingFormatOption));
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskExpackIndexIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ON_DISK_EXPACK_INDEX_ITERATOR_H
