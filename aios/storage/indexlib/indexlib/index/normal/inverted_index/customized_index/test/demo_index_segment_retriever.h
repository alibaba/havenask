#ifndef __INDEXLIB_INDEX_DEMO_INDEX_SEGMENT_RETRIEVER_H
#define __INDEXLIB_INDEX_DEMO_INDEX_SEGMENT_RETRIEVER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class DemoIndexSegmentRetriever : public IndexSegmentRetriever
{
public:
    DemoIndexSegmentRetriever(const util::KeyValueMap& parameters) : IndexSegmentRetriever(parameters) {}
    ~DemoIndexSegmentRetriever() {}

public:
    bool DoOpen(const file_system::DirectoryPtr& indexDir) override;
    MatchInfo Search(const std::string& query, autil::mem_pool::Pool* sessionPool) override;
    size_t EstimateLoadMemoryUse(const file_system::DirectoryPtr& indexDir) const override
    {
        return indexDir->GetFileLength(DemoIndexSegmentRetriever::mFileName);
    }

private:
    typedef std::pair<docid_t, std::string> DocItem;
    std::map<docid_t, std::string> mDataMap;
    static const std::string mFileName;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoIndexSegmentRetriever);
}} // namespace indexlib::index

#endif //__INDEXLIB_INDEX_DEMO_INDEX_SEGMENT_RETRIEVER_H
