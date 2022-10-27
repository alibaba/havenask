#ifndef __INDEXLIB_DOC_LIST_SKIP_LIST_FORMAT_H
#define __INDEXLIB_DOC_LIST_SKIP_LIST_FORMAT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/atomic_value_typed.h"
#include "indexlib/common/multi_value.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format_option.h"

IE_NAMESPACE_BEGIN(index);

class DocListSkipListFormat : public common::MultiValue
{
public:
    typedef common::AtomicValueTyped<uint32_t> DocIdValue;
    typedef common::AtomicValueTyped<uint32_t> TotalTFValue;
    typedef common::AtomicValueTyped<uint32_t> OffsetValue;

public:
    DocListSkipListFormat(const DocListFormatOption& option)
        : mDocIdValue(NULL)
        , mTotalTFValue(NULL)
        , mOffsetValue(NULL)
    {
        Init(option);
    }

    ~DocListSkipListFormat() {}

public:
    bool HasTfList() const { return mTotalTFValue != NULL; }

private:
    void Init(const DocListFormatOption& option);

private:
    DocIdValue* mDocIdValue;
    TotalTFValue* mTotalTFValue;
    OffsetValue* mOffsetValue;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocListSkipListFormat);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_LIST_SKIP_LIST_FORMAT_H
