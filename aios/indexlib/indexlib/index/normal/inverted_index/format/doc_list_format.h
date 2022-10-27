#ifndef __INDEXLIB_DOC_LIST_FORMAT_H
#define __INDEXLIB_DOC_LIST_FORMAT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/multi_value.h"
#include "indexlib/common/atomic_value_typed.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format_option.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_skip_list_format.h"

IE_NAMESPACE_BEGIN(index);

typedef common::AtomicValueTyped<docid_t> DocIdValue;
typedef common::AtomicValueTyped<tf_t> TfValue;
typedef common::AtomicValueTyped<docpayload_t> DocPayloadValue;
typedef common::AtomicValueTyped<fieldmap_t> FieldMapValue;

class DocListFormat : public common::MultiValue
{
public:
    DocListFormat(const DocListFormatOption& option)
        : mDocListSkipListFormat(NULL)
        , mDocIdValue(NULL)
        , mTfValue(NULL)
        , mDocPayloadValue(NULL)
        , mFieldMapValue(NULL)
    {
        Init(option);
    }

    ~DocListFormat()
    {
        DELETE_AND_SET_NULL(mDocListSkipListFormat);
    }

public:
    DocIdValue* GetDocIdValue() const { return mDocIdValue; }
    TfValue* GetTfValue() const { return mTfValue; }
    DocPayloadValue* GetDocPayloadValue() const { return mDocPayloadValue; }
    FieldMapValue* GetFieldMapValue() const { return mFieldMapValue; }

    const DocListSkipListFormat* GetDocListSkipListFormat() const 
    { return mDocListSkipListFormat; }

private:
    void Init(const DocListFormatOption& option);
    void InitDocListSkipListFormat(const DocListFormatOption& option);

private:
    DocListSkipListFormat* mDocListSkipListFormat;
    DocIdValue* mDocIdValue;
    TfValue* mTfValue;
    DocPayloadValue* mDocPayloadValue;
    FieldMapValue* mFieldMapValue;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocListFormat);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_LIST_FORMAT_H
