#include "indexlib/index/inverted_index/truncate/test/TruncateTestHelper.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, TruncateTestHelper);

std::shared_ptr<BucketMap> TruncateTestHelper::CreateBucketMap(uint32_t size)
{
    std::vector<uint32_t> order;
    for (size_t i = 0; i < size; ++i) {
        order.push_back(i);
    }
    random_shuffle(order.begin(), order.end());
    return CreateBucketMap(order);
}

std::shared_ptr<BucketMap> TruncateTestHelper::CreateBucketMap(const std::string& str)
{
    std::vector<std::string> vec = autil::StringTokenizer::tokenize(
        autil::StringView(str), ";", autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    std::vector<uint32_t> numberVec;
    autil::StringUtil::fromString(vec, numberVec);
    return CreateBucketMap(numberVec);
}

std::shared_ptr<BucketMap> TruncateTestHelper::CreateBucketMap(const std::vector<uint32_t>& vec)
{
    auto bucketMap = std::make_shared<BucketMap>("ut_bucket_map", "ut_bucket_map_type");
    bucketMap->Init(vec.size());
    for (size_t i = 0; i < vec.size(); ++i) {
        bucketMap->SetSortValue(i, vec[i]);
    }
    return bucketMap;
}
} // namespace indexlib::index
