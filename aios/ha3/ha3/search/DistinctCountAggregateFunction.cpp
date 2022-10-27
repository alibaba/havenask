#include <ha3/search/DistinctCountAggregateFunction.h>

using namespace std;

BEGIN_HA3_NAMESPACE(search);
/*
template <typename ExprType, typename SingleExprType = autil::MultiChar>
void DistinctCountAggregateFunction<ExprType, autil::MultiChar>::
    hllCtxAddElement(autil::HllCtx &hllCtx, const autil::MultiChar &value) {
    autil::Hyperloglog::hllCtxAdd(&hllCtx, (const unsigned char *)value.data(),
                                  value.size(), _pool);
    return;
}
*/

END_HA3_NAMESPACE(search);

