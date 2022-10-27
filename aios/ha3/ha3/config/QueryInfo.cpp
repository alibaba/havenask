#include <ha3/config/QueryInfo.h>

using namespace std;
BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, QueryInfo);

QueryInfo::QueryInfo() { 
    _defaultIndexName = "default";
    _defaultOP = OP_AND;
    _useMultiTermOptimize = false;
}

QueryInfo::QueryInfo(const string &defaultIndexName, QueryOperator defaultOP,
                     bool flag)
    : _defaultIndexName(defaultIndexName)
    , _defaultOP(defaultOP)
    , _useMultiTermOptimize(flag)
{
}

QueryInfo::~QueryInfo() { 
}

void QueryInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    string opString;
    if (TO_JSON == json.GetMode()) {
        if (_defaultOP == OP_AND) {
            opString = "AND";
        } else if (_defaultOP == OP_OR) {
            opString = "OR";
        }
    }

    json.Jsonize("default_index", _defaultIndexName, _defaultIndexName);
    json.Jsonize("default_operator", opString, string("AND"));

    if (FROM_JSON == json.GetMode()) {
        if (opString == "AND") {
            _defaultOP = OP_AND;
        } else if (opString == "OR") {
            _defaultOP = OP_OR;
        }
    } 
    json.Jsonize("multi_term_optimize", _useMultiTermOptimize, _useMultiTermOptimize);
}

END_HA3_NAMESPACE(config);

