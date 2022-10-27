#ifndef __INDEXLIB_DOC_INFO_EVALUATOR_H
#define __INDEXLIB_DOC_INFO_EVALUATOR_H

#include <tr1/memory>
#include <autil/StringUtil.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info.h"
#include "indexlib/index/normal/inverted_index/truncate/reference_typed.h"

IE_NAMESPACE_BEGIN(tools);
      
class DocInfoEvaluator
{
public:
    DocInfoEvaluator() {}
    virtual ~DocInfoEvaluator() {}
    virtual void Evaluate(std::string value, index::DocInfo* docInfo, index::Reference* refer) = 0;

private:
    IE_LOG_DECLARE();
};


template <typename T>
class DocInfoEvaluatorTyped : public DocInfoEvaluator
{
public:
    DocInfoEvaluatorTyped() {}
    ~DocInfoEvaluatorTyped() {}
    
    void Evaluate(std::string value, index::DocInfo* docInfo, index::Reference* refer)
    {
        T convertedValue;
        autil::StringUtil::fromString(value, convertedValue);
        size_t offset = refer->GetOffset();
        uint8_t* buffer = docInfo->Get(offset);
        *((T*)buffer) = convertedValue;
    }
};

DEFINE_SHARED_PTR(DocInfoEvaluator);

IE_NAMESPACE_END(tools);

#endif //__INDEXLIB_DOC_INFO_EVALUATOR_H
