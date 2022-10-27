#include <ha3/sql/ops/condition/ConditionVisitor.h>

using namespace autil_rapidjson;
using namespace std;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, ConditionVisitor);
ConditionVisitor::ConditionVisitor()
    : _hasError(false)
{}

ConditionVisitor::~ConditionVisitor() {
}


END_HA3_NAMESPACE(sql);
