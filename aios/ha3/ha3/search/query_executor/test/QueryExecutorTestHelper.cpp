#include "ha3/search/test/QueryExecutorTestHelper.h"

#include <algorithm>
#include <cstddef>
#include <vector>

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h"
#include "indexlib/indexlib.h"

using namespace std;
using namespace autil;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, QueryExecutorTestHelper);

QueryExecutorTestHelper::QueryExecutorTestHelper() {}

QueryExecutorTestHelper::~QueryExecutorTestHelper() {}

void QueryExecutorTestHelper::addSubDocAttrMap(indexlib::index::FakeIndex &fakeIndex,
                                               const std::string &subDocAttrMap) {
    vector<string> vec = StringTokenizer::tokenize(StringView(subDocAttrMap),
                                                   ",",
                                                   StringTokenizer::TOKEN_TRIM
                                                       | StringTokenizer::TOKEN_IGNORE_EMPTY);
    vector<int> mainDocToSubDoc;
    for (size_t i = 0; i < vec.size(); ++i) {
        mainDocToSubDoc.push_back(StringUtil::fromString<int>(vec[i]));
    }
    vector<int> subDocToMainDoc;
    int j = 0;
    for (int i = 0; i < *(mainDocToSubDoc.rbegin()); ++i) {
        if (i >= mainDocToSubDoc[j]) {
            ++j;
        }
        subDocToMainDoc.push_back(j);
    }
    string subDocToMainDocStr = StringUtil::toString(subDocToMainDoc, ",");

    addSubDocAttrMap(fakeIndex, subDocAttrMap, subDocToMainDocStr);
}

void QueryExecutorTestHelper::addSubDocAttrMap(indexlib::index::FakeIndex &fakeIndex,
                                               const std::string &mainToSubAttrMap,
                                               const std::string &subToMainAttrMap) {
    fakeIndex.attributes
        += std::string(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME) + ": int32_t : " + mainToSubAttrMap
           + ";\n" + SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME + ": int32_t : " + subToMainAttrMap + ";\n";
}

} // namespace search
} // namespace isearch
