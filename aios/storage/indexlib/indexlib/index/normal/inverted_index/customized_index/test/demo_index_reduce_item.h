#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class DemoIndexReduceItem : public IndexReduceItem
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
    bool Add(docid_t docId, const std::string& data);

private:
    friend class DemoIndexReducer;
    std::map<docid_t, std::string> mDataMap;
    static const std::string mFileName;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoIndexReduceItem);
}} // namespace indexlib::index
