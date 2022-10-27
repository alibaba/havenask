#ifndef __INDEXLIB_EVALUATOR_CREATOR_H
#define __INDEXLIB_EVALUATOR_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_common.h"
#include "indexlib/index/normal/inverted_index/truncate/evaluator.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_attribute_reader_creator.h"

DECLARE_REFERENCE_CLASS(config, TruncateIndexProperty);

IE_NAMESPACE_BEGIN(index);

class EvaluatorCreator
{
public:
    EvaluatorCreator();
    ~EvaluatorCreator();

public:
    static EvaluatorPtr Create(
            const config::TruncateIndexProperty& truncIndexProperty,
            TruncateAttributeReaderCreator *truncateAttrCreator,
            const DocInfoAllocatorPtr& allocator);

    static EvaluatorPtr CreateEvaluator(
            const std::string& fieldName,
            TruncateAttributeReaderCreator *truncateAttrCreator,
            const DocInfoAllocatorPtr& allocator);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(EvaluatorCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_EVALUATOR_CREATOR_H
