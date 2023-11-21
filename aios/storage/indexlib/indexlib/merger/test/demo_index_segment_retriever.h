#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace merger {

class DemoIndexSegmentRetriever : public index::IndexSegmentRetriever
{
public:
    DemoIndexSegmentRetriever(const util::KeyValueMap& parameters) : IndexSegmentRetriever(parameters) {}
    ~DemoIndexSegmentRetriever() {}

public:
    bool DoOpen(const file_system::DirectoryPtr& indexDir) override;
    MatchInfo Search(const std::string& query, autil::mem_pool::Pool* sessionPool) override;
    size_t EstimateLoadMemoryUse(const file_system::DirectoryPtr& indexDir) const override;

private:
    bool LoadData(const file_system::DirectoryPtr& indexDir);

private:
    typedef std::pair<docid_t, std::string> DocItem;
    std::map<docid_t, std::string> mDataMap;
    static const std::string INDEX_FILE_NAME;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoIndexSegmentRetriever);
}} // namespace indexlib::merger
