#pragma once

#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h"
#include "indexlib/misc/common.h"

namespace isearch {
namespace search {

class QueryExecutorTestHelper {
public:
    QueryExecutorTestHelper();
    ~QueryExecutorTestHelper();

private:
    QueryExecutorTestHelper(const QueryExecutorTestHelper &);
    QueryExecutorTestHelper &operator=(const QueryExecutorTestHelper &);

public:
    static void addSubDocAttrMap(indexlib::index::FakeIndex &fakeIndex,
                                 const std::string &subDocAttrMap);

    static void addSubDocAttrMap(indexlib::index::FakeIndex &fakeIndex,
                                 const std::string &mainToSubAttrMap,
                                 const std::string &subToMainAttrMap);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QueryExecutorTestHelper> QueryExecutorTestHelperPtr;

} // namespace search
} // namespace isearch
