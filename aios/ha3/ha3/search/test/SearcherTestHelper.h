#ifndef ISEARCH_SEARCHERTESTHELPER_H
#define ISEARCH_SEARCHERTESTHELPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/TermDFVisitor.h>
#include <ha3/common/AggregateResult.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>

BEGIN_HA3_NAMESPACE(search);

class SearcherTestHelper
{
public:
    SearcherTestHelper();
    ~SearcherTestHelper();
private:
    SearcherTestHelper(const SearcherTestHelper &);
    SearcherTestHelper& operator=(const SearcherTestHelper &);
public:
    static TermDFMap createTermDFMap(const std::string &str);
    static common::Query *createQuery(
        const std::string &queryStr,
        uint32_t pos = 0,
        const std::string &defaultIndexName = "default",
        bool needAnalyzer = true);
    static std::vector<common::Query*> createQuerys(
        const std::string &queryStr,
        const std::string &defaultIndexName = "default",
        bool needAnalyzer = true);
    static std::vector<std::vector<matchvalue_t> > convertToMatchValues(
            const std::string &matchValueStr);
    static std::vector<docid_t> covertToResultDocIds(const std::string &docIdStr);
    static void checkAggregatorResult(const common::AggregateResults &aggResults,
            const std::string &resultStr);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SearcherTestHelper);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SEARCHERTESTHELPER_H
