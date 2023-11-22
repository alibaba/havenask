#pragma once
#include <memory>

#include "indexlib/index/inverted_index/truncate/BucketMap.h"

namespace indexlib::index {

class TruncateTestHelper
{
public:
    TruncateTestHelper() = default;
    ~TruncateTestHelper() = default;

public:
    static std::shared_ptr<BucketMap> CreateBucketMap(uint32_t size);
    static std::shared_ptr<BucketMap> CreateBucketMap(const std::string& str);

private:
    static std::shared_ptr<BucketMap> CreateBucketMap(const std::vector<uint32_t>& vec);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index