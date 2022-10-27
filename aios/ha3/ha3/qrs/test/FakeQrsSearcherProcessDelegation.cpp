#include <ha3/qrs/test/FakeQrsSearcherProcessDelegation.h>

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, FakeQrsSearcherProcessDelegation);

FakeQrsSearcherProcessDelegation::FakeQrsSearcherProcessDelegation(
        const HitSummarySchemaCachePtr &hitSummarySchemaCache)
    : QrsSearcherProcessDelegation(hitSummarySchemaCache)
{
}

FakeQrsSearcherProcessDelegation::~FakeQrsSearcherProcessDelegation() {
}

ResultPtr FakeQrsSearcherProcessDelegation::search(RequestPtr &requestPtr) {
    // do nothing
    return ResultPtr(new Result);
}

END_HA3_NAMESPACE(qrs);
