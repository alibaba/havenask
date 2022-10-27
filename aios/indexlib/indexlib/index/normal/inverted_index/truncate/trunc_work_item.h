#ifndef __INDEXLIB_TRUNC_WORK_ITEM_H
#define __INDEXLIB_TRUNC_WORK_ITEM_H

#include <tr1/memory>
#include <autil/WorkItem.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/truncate/single_truncate_index_writer.h"
#include "indexlib/util/resource_control_work_item.h"

IE_NAMESPACE_BEGIN(index);

class TruncWorkItem : public autil::WorkItem
{
public:
    TruncWorkItem(dictkey_t dictKey, 
                  const PostingIteratorPtr& postingIt,
                  const TruncateIndexWriterPtr& indexWriter);
    ~TruncWorkItem();
public:
    void process() override;
    void destroy() override;
    void drop() override;

private:
    dictkey_t mDictKey;
    PostingIteratorPtr mPostingIt;
    TruncateIndexWriterPtr mIndexWriter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncWorkItem);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNC_WORK_ITEM_H
