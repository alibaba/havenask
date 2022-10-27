#ifndef ISEARCH_PAGEDISTINCTSELECTORTEST_H
#define ISEARCH_PAGEDISTINCTSELECTORTEST_H
#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/PageDistinctSelector.h>
#include <ha3/qrs/PageDistinctHitConvertor.h>

BEGIN_HA3_NAMESPACE(qrs);
class PageDistinctSelectorTest : public TESTBASE {
public:
    PageDistinctSelectorTest();
    ~PageDistinctSelectorTest();
public:
    void setUp();
    void tearDown();
private:
    std::vector<PageDistinctHit<uint32_t>*> createPageDistHits(
            const std::string &hitStr);

public:
    static common::HitPtr createHit(docid_t docid, const std::string &distKey, 
                                    uint32_t keyValue, 
                                    const std::string &sortValue);
    static std::vector<common::HitPtr> createHits(const std::string &keyName,
            const std::string &hitStr);
    static void addHit(std::vector<common::HitPtr> &hits, docid_t docid, 
                       const std::string &distKey, uint32_t keyValue, 
                       const std::string &sortValue);
    static void checkHits(const std::vector<common::HitPtr> &hits,
                   const std::string &expectedHitsStr);
private:
    std::vector<PageDistinctHit<uint32_t>*> _pageDistHitsPool;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_PAGEDISTINCTSELECTORTEST_H
