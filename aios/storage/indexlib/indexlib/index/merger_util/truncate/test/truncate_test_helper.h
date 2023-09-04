#ifndef __INDEXLIB_TRUNCATE_TEST_HELPER_H
#define __INDEXLIB_TRUNCATE_TEST_HELPER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/bucket_map.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class TruncateTestHelper
{
public:
    TruncateTestHelper();
    ~TruncateTestHelper();

public:
    static BucketMapPtr CreateBucketMap(uint32_t size);
    static BucketMapPtr CreateBucketMap(const std::string& str);

private:
    static BucketMapPtr CreateBucketMap(const std::vector<uint32_t>& vec);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateTestHelper);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_TRUNCATE_TEST_HELPER_H
