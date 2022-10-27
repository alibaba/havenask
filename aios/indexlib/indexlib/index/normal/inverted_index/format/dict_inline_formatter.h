#ifndef __INDEXLIB_DICT_INLINE_FORMATTER_H
#define __INDEXLIB_DICT_INLINE_FORMATTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"

IE_NAMESPACE_BEGIN(index);

class DictInlineFormatter
{
public:
    DictInlineFormatter(const PostingFormatOption& postingFormatOption);
    DictInlineFormatter(const PostingFormatOption& postingFormatOption, uint64_t dictValue);

    //for test
    DictInlineFormatter(const PostingFormatOption& postingFormatOption,
                        const std::vector<uint32_t>& vec);

    ~DictInlineFormatter() {}
    
public:
    void SetTermPayload(termpayload_t termPayload) 
    { mTermPayload = termPayload; }

    void SetDocId(docid_t docid)
    { mDocId = docid; }

    void SetDocPayload(docpayload_t docPayload) 
    { mDocPayload = docPayload; }

    void SetTermFreq(tf_t termFreq) 
    { mTermFreq = termFreq; }

    void SetFieldMap(fieldmap_t fieldMap)
    { mFieldMap = fieldMap; }

    termpayload_t GetTermPayload() const
    { return mTermPayload; }

    docid_t GetDocId() const
    { return mDocId; }

    docpayload_t GetDocPayload() const
    { return mDocPayload; }

    df_t GetDocFreq() const { return 1; }
    tf_t GetTermFreq() const
    { return (mTermFreq == INVALID_TF) ? 1 : mTermFreq; }

    fieldmap_t GetFieldMap() const
    { return mFieldMap; }

    bool GetDictInlineValue(uint64_t& inlinePostingValue);
    
    static uint8_t CalculateDictInlineItemCount(PostingFormatOption& postingFormatOption);


private:
    void ToVector(std::vector<uint32_t>& vec);
    void FromVector(uint32_t* buffer, size_t size);
    uint32_t CheckAndGetValue(const size_t& curosr, uint32_t* buffer, size_t size);
private:
    PostingFormatOption mPostingFormatOption;
    termpayload_t mTermPayload;
    docid_t mDocId;
    docpayload_t mDocPayload;
    tf_t mTermFreq;
    fieldmap_t mFieldMap;

    // TODO: add position list param in future
private:
    friend class DictInlineFormatterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DictInlineFormatter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DICT_INLINE_FORMATTER_H
