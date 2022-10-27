#ifndef __INDEXLIB_STRING_INDEX_MERGER_H
#define __INDEXLIB_STRING_INDEX_MERGER_H

#include <tr1/memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/inverted_index/builtin_index/text/text_index_merger.h"

IE_NAMESPACE_BEGIN(index);

class StringIndexMerger : public TextIndexMerger
{
public:
    DECLARE_INDEX_MERGER_IDENTIFIER(string);
    DECLARE_INDEX_MERGER_CREATOR(StringIndexMerger, it_string);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(StringIndexMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_STRING_INDEX_MERGER_H
