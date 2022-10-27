#include <ha3/search/AggregateFunction.h>

using namespace std;
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, AggregateFunction);

AggregateFunction::AggregateFunction(const string &functionName,
                                     const std::string &parameter,
                                     VariableType resultType)
    : _functionName(functionName),
      _parameter(parameter)
{
    _resultType = resultType;
}

AggregateFunction::~AggregateFunction() {
}

END_HA3_NAMESPACE(search);
