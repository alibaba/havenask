#include "indexlib/merger/document_reclaimer/index_reclaimer_param.h"

using namespace std;

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, IndexReclaimerParam);

std::string IndexReclaimerParam::AND_RECLAIM_OPERATOR = string("AND");

IndexReclaimerParam::IndexReclaimerParam()
    : mReclaimIndex("")
    , mReclaimOperator("")
{
}

IndexReclaimerParam::~IndexReclaimerParam() 
{
}

void IndexReclaimerParam::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("reclaim_index", mReclaimIndex, mReclaimIndex);
    json.Jsonize("reclaim_terms", mReclaimTerms, mReclaimTerms);
    json.Jsonize("reclaim_operator", mReclaimOperator, mReclaimOperator);
    json.Jsonize("reclaim_index_info", mReclaimOprands, mReclaimOprands);
}

void IndexReclaimerParam::TEST_SetReclaimOprands(const vector<ReclaimOprand>& ops)
{
    mReclaimOprands = ops;
    mReclaimOperator = AND_RECLAIM_OPERATOR;
}

IE_NAMESPACE_END(merger);

