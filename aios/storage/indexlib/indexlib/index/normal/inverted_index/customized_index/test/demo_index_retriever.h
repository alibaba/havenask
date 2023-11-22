#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/test/demo_index_segment_retriever.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class DemoIndexRetriever : public IndexRetriever
{
public:
    DemoIndexRetriever() {}
    ~DemoIndexRetriever() {}

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoIndexRetriever);
}} // namespace indexlib::index
