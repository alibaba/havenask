#include "indexlib/index/normal/inverted_index/truncate/test/truncate_test_helper.h"
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TruncateTestHelper);

TruncateTestHelper::TruncateTestHelper() 
{
}

TruncateTestHelper::~TruncateTestHelper() 
{
}

BucketMapPtr TruncateTestHelper::CreateBucketMap(uint32_t size)
{
    vector<uint32_t> order;
    for (size_t i = 0; i < size; ++i)
    {
        order.push_back(i);
    }
    random_shuffle(order.begin(), order.end());
    return CreateBucketMap(order);
}

BucketMapPtr TruncateTestHelper::CreateBucketMap(const std::string &str)
{
    vector<string> vec = StringTokenizer::tokenize(ConstString(str), ";",
            StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    vector<uint32_t> numberVec;
    StringUtil::fromString(vec, numberVec);
    return CreateBucketMap(numberVec);
}

BucketMapPtr TruncateTestHelper::CreateBucketMap(const std::vector<uint32_t> &vec)
{
    BucketMapPtr bucketMap(new BucketMap);
    bucketMap->Init(vec.size());
    for (size_t i = 0; i < vec.size(); ++i)
    {
        bucketMap->SetSortValue(i, vec[i]);
    }
    return bucketMap;
}

IE_NAMESPACE_END(index);

