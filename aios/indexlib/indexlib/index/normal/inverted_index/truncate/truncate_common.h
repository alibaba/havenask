#ifndef __INDEXLIB_TRUNCATE_COMMON_H
#define __INDEXLIB_TRUNCATE_COMMON_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/priority_queue.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info.h"
#include "indexlib/index/normal/inverted_index/truncate/comparator.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_attribute_reader.h"

IE_NAMESPACE_BEGIN(index);

struct TruncateTriggerInfo
{
    TruncateTriggerInfo()
        : key(-1)
        , df(0)
    {}

    TruncateTriggerInfo(dictkey_t _key, df_t _df)
        : key(_key)
        , df(_df)
    {}

    df_t GetDF() const 
    {
        return df;
    }

    dictkey_t GetDictKey() const
    {
        return key;
    }

    dictkey_t key;
    df_t df;
};

typedef std::map<std::string, TruncateAttributeReaderPtr> TruncateAttributeReaders;

typedef util::PriorityQueue<DocInfo*, DocInfoComparator> DocInfoHeap;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNCATE_COMMON_H
