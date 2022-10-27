#ifndef __INDEXLIB_DEMO_INDEXER_USERDATA_H
#define __INDEXLIB_DEMO_INDEXER_USERDATA_H

#include <tr1/memory>
#include <autil/StringUtil.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_user_data.h"

IE_NAMESPACE_BEGIN(index);

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

    virtual size_t estimateInitMemUse(
            const std::vector<IndexerSegmentData>& segDatas) const
    {
        size_t initSize = segDatas.size() * 10000;
        std::cout << "**** estimate UserData memUse:" << initSize << std::endl;
        return initSize;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoUserData);

IE_NAMESPACE_END(customized_index);

#endif //__INDEXLIB_DEMO_INDEXER_H
