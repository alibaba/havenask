#ifndef ISEARCH_HA3_MATCHDOCALLOCATOR_H
#define ISEARCH_HA3_MATCHDOCALLOCATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/AttributeItem.h>
#include <ha3/common/ReferenceIdManager.h>
#include <matchdoc/MatchDocAllocator.h>
#include <assert.h>

BEGIN_HA3_NAMESPACE(common);

class Ha3MatchDocAllocator : public matchdoc::MatchDocAllocator
{
public:
    Ha3MatchDocAllocator(autil::mem_pool::Pool *pool, bool useSubDoc = false);
    ~Ha3MatchDocAllocator();
public:
    void initPhaseOneInfoReferences(uint8_t phaseOneInfoFlag);
    uint32_t requireReferenceId(const std::string &refName);
public:
    static common::AttributeItemPtr createAttributeItem(
            matchdoc::ReferenceBase *ref,
            matchdoc::MatchDoc matchDoc);
private:
    void registerTypes();
private:
    ReferenceIdManager *_refIdManager;
private:
    HA3_LOG_DECLARE();
};

typedef std::shared_ptr<Ha3MatchDocAllocator> Ha3MatchDocAllocatorPtr;

END_HA3_NAMESPACE(common);

#endif //ISEARCH_MATCHDOCALLOCATOR_H
