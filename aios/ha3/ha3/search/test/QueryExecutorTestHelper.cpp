#include <ha3/search/test/QueryExecutorTestHelper.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, QueryExecutorTestHelper);

QueryExecutorTestHelper::QueryExecutorTestHelper() { 
}

QueryExecutorTestHelper::~QueryExecutorTestHelper() { 
}

void QueryExecutorTestHelper::addSubDocAttrMap(index::FakeIndex &fakeIndex,
        const std::string &subDocAttrMap)
{
    vector<string> vec = StringTokenizer::tokenize(ConstString(subDocAttrMap), ",",
            StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    vector<int> mainDocToSubDoc;
    for (size_t i = 0; i < vec.size(); ++i) {
        mainDocToSubDoc.push_back(StringUtil::fromString<int>(vec[i]));
    }
    vector<int> subDocToMainDoc;
    int j = 0;
    for(int i = 0; i < *(mainDocToSubDoc.rbegin()); ++i) {
        if (i >= mainDocToSubDoc[j]) {
            ++j;
        }
        subDocToMainDoc.push_back(j);
    }
    string subDocToMainDocStr = StringUtil::toString(subDocToMainDoc, ",");

    addSubDocAttrMap(fakeIndex, subDocAttrMap, subDocToMainDocStr);
}

void QueryExecutorTestHelper::addSubDocAttrMap(index::FakeIndex &fakeIndex,
        const std::string &mainToSubAttrMap,
        const std::string &subToMainAttrMap)
{
    fakeIndex.attributes += MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME": int32_t : " + mainToSubAttrMap + ";\n"
                            SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME": int32_t : " + subToMainAttrMap + ";\n";
}

END_HA3_NAMESPACE(search);

