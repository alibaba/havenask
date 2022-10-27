#ifndef __INDEXLIB_SORT_BUILD_CHECKER_H
#define __INDEXLIB_SORT_BUILD_CHECKER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/multi_comparator.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"

DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(index, DocInfo);
DECLARE_REFERENCE_CLASS(index, SortValueEvaluator);
DECLARE_REFERENCE_CLASS(index_base, PartitionMeta);

IE_NAMESPACE_BEGIN(partition);

class SortBuildChecker
{
public:
    SortBuildChecker();
    ~SortBuildChecker();

public:
    bool Init(const config::AttributeSchemaPtr& attrSchema,
              const index_base::PartitionMeta& partMeta);

    bool CheckDocument(const document::DocumentPtr& doc);

private:
    void Evaluate(const document::DocumentPtr& document,
                  index::DocInfo* docInfo);
private:
    uint32_t mCheckCount;
    index::DocInfo* mLastDocInfo;
    index::DocInfo* mCurDocInfo;
    std::vector<index::SortValueEvaluatorPtr> mEvaluators;
    index::MultiComparator mComparator;
    index::DocInfoAllocator mDocInfoAllocator;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SortBuildChecker);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SORT_BUILD_CHECKER_H
