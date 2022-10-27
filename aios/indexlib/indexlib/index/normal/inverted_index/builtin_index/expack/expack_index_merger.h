#ifndef __INDEXLIB_EXPACK_INDEX_MERGER_H
#define __INDEXLIB_EXPACK_INDEX_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/pack_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/expack/on_disk_expack_index_iterator.h"

IE_NAMESPACE_BEGIN(index);

class ExpackIndexMerger : public PackIndexMerger
{
public:
    ExpackIndexMerger();
    virtual ~ExpackIndexMerger();

    DECLARE_INDEX_MERGER_IDENTIFIER(expack);
    DECLARE_INDEX_MERGER_CREATOR(ExpackIndexMerger, it_expack);

public:
    OnDiskIndexIteratorCreatorPtr CreateOnDiskIndexIteratorCreator() override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ExpackIndexMerger);

///////////////////////////////////////////////////////////
ExpackIndexMerger::ExpackIndexMerger() 
{
}

ExpackIndexMerger::~ExpackIndexMerger() 
{
}

OnDiskIndexIteratorCreatorPtr ExpackIndexMerger::CreateOnDiskIndexIteratorCreator()
{
    return OnDiskIndexIteratorCreatorPtr(
            new OnDiskExpackIndexIterator::Creator(
                    GetPostingFormatOption(), mIOConfig, mIndexConfig));
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_EXPACK_INDEX_MERGER_H
