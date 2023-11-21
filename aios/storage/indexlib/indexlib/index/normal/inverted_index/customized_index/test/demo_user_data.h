#pragma once

#include <memory>

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_user_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class DemoUserData : public IndexerUserData
{
public:
    DemoUserData() {}
    ~DemoUserData() {}

public:
    virtual bool init(const std::vector<IndexerSegmentData>& segDatas)
    {
        setData("count", autil::StringUtil::toString(segDatas.size()));
        return true;
    }

    virtual size_t estimateInitMemUse(const std::vector<IndexerSegmentData>& segDatas) const
    {
        size_t initSize = segDatas.size() * 10000;
        std::cout << "**** estimate UserData memUse:" << initSize << std::endl;
        return initSize;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoUserData);
}} // namespace indexlib::index
