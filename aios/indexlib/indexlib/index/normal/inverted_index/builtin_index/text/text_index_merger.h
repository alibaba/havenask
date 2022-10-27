#ifndef __INDEXLIB_TEXT_INDEX_MERGER_H
#define __INDEXLIB_TEXT_INDEX_MERGER_H

#include <tr1/memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/on_disk_pack_index_iterator.h"

IE_NAMESPACE_BEGIN(index);

class TextIndexMerger : public IndexMerger
{
public:
    typedef OnDiskPackIndexIterator OnDiskTextIndexIterator;

public:
    TextIndexMerger(){}
    ~TextIndexMerger(){}

    DECLARE_INDEX_MERGER_IDENTIFIER(text);
    DECLARE_INDEX_MERGER_CREATOR(TextIndexMerger, it_text);

public:
    OnDiskIndexIteratorCreatorPtr CreateOnDiskIndexIteratorCreator() override
    {
        return OnDiskIndexIteratorCreatorPtr(new OnDiskTextIndexIterator::Creator(
                        GetPostingFormatOption(), mIOConfig, mIndexConfig));
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TextIndexMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TEXT_INDEX_MERGER_H
