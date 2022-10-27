#ifndef __INDEXLIB_INDEX_RECLAIMER_PARAM_H
#define __INDEXLIB_INDEX_RECLAIMER_PARAM_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(merger);

class ReclaimOprand : public autil::legacy::Jsonizable
{
public:
    ReclaimOprand()
        : indexName("")
        , term("")
    {}
    ~ReclaimOprand() {}
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("reclaim_index", indexName, indexName);
        json.Jsonize("reclaim_term", term, term);
    }
public:
    std::string indexName;
    std::string term;
};

class IndexReclaimerParam : public autil::legacy::Jsonizable
{
public:
    IndexReclaimerParam();
    ~IndexReclaimerParam();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void SetReclaimIndex(const std::string& reclaimIndex)
    { mReclaimIndex = reclaimIndex; }
    const std::string& GetReclaimIndex() const
    { return mReclaimIndex; }

    void SetReclaimTerms(const std::vector<std::string>& reclaimTerms)
    { mReclaimTerms = reclaimTerms; }
    const std::vector<std::string>& GetReclaimTerms() const
    { return mReclaimTerms; }
    const std::string& GetReclaimOperator() const
    {
        return mReclaimOperator;
    }
    const std::vector<ReclaimOprand>& GetReclaimOprands() const
    {
        return mReclaimOprands;
    }

public:
    void TEST_SetReclaimOprands(const std::vector<ReclaimOprand>& ops);

private:
    std::string mReclaimIndex;
    std::vector<std::string> mReclaimTerms;
    std::string mReclaimOperator;
    std::vector<ReclaimOprand> mReclaimOprands;

public:
    static std::string AND_RECLAIM_OPERATOR;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexReclaimerParam);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_INDEX_RECLAIMER_PARAM_H
