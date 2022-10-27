#ifndef __INDEXLIB_TRUNCATE_TEST_HELPER_H
#define __INDEXLIB_TRUNCATE_TEST_HELPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/bucket_map.h"

IE_NAMESPACE_BEGIN(index);

class TruncateTestHelper
{
public:
    TruncateTestHelper();
    ~TruncateTestHelper();
public:
    static BucketMapPtr CreateBucketMap(uint32_t size);
    static BucketMapPtr CreateBucketMap(const std::string &str);
private:
    static BucketMapPtr CreateBucketMap(const std::vector<uint32_t> &vec);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateTestHelper);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNCATE_TEST_HELPER_H
