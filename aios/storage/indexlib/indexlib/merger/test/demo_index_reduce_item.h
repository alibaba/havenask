#ifndef __INDEXLIB_DEMO_INDEX_REDUCE_ITEM_H
#define __INDEXLIB_DEMO_INDEX_REDUCE_ITEM_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace merger {

class DemoIndexReduceItem : public index::IndexReduceItem
{
public:
    DemoIndexReduceItem();
    ~DemoIndexReduceItem();

public:
    bool LoadIndex(const file_system::DirectoryPtr& indexDir) override;
    bool UpdateDocId(const DocIdMap& docIdMap) override;

public:
    std::map<docid_t, std::string>::const_iterator Begin() const { return mDataMap.cbegin(); }
    std::map<docid_t, std::string>::const_iterator End() const { return mDataMap.cend(); }
    void SetMergeTaskResources(index_base::MergeTaskResourceVector& taskResources) { mTaskResources = taskResources; }
    bool Add(docid_t docId, const std::string& data);

private:
    friend class DemoIndexReducer;
    std::map<docid_t, std::string> mDataMap;
    static const std::string mFileName;
    index_base::MergeTaskResourceVector mTaskResources;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoIndexReduceItem);
}} // namespace indexlib::merger

#endif //__INDEXLIB_DEMO_INDEX_REDUCE_ITEM_H
