#include <ha3/sql/ops/tvf/builtin/SortTvfFunc.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/rank/ComparatorCreator.h>
#include <ha3/sql/ops/agg/AggBase.h>

using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(sql);
const SqlTvfProfileInfo sortTvfInfo("sortTvf", "sortTvf");
const string sortTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "sortTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false
    },
    "prototypes": [
      {
        "params": {
          "scalars": [
             {
               "type":"string"
             },
             {
               "type":"string"
             }
         ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        },
        "returns": {
          "new_fields": [
          ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        }
      }
    ]
  }
}
)tvf_json";

SortTvfFunc::SortTvfFunc() {
    _count = 0;
}

SortTvfFunc::~SortTvfFunc() {
}

bool SortTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 2) {
        SQL_LOG(WARN, "param size [%ld] not equal 2", context.params.size());
        return false;
    }
    _sortStr = context.params[0];
    _sortFields = StringUtil::split(_sortStr, ",");
    if (_sortFields.size() == 0) {
        SQL_LOG(WARN, "parse order [%s] fail.", _sortStr.c_str());
        return false;
    }
    for (size_t i = 0; i < _sortFields.size(); i++) {
        const string &field = _sortFields[i];
        if (field.size() == 0) {
            SQL_LOG(WARN, "parse order [%s] fail.", _sortStr.c_str());
            return false;
        }
        if (field[0] != '+' && field[0] != '-') {
            _sortFlags.push_back(false);
        } else if (field[0] == '+') {
            _sortFlags.push_back(false);
            _sortFields[i] = field.substr(1);
        } else {
            _sortFlags.push_back(true);
            _sortFields[i] = field.substr(1);
        }
    }
    if (!StringUtil::fromString(context.params[1], _count)) {
        SQL_LOG(WARN, "parse param [%s] fail.", context.params[1].c_str());
        return false;
    }
    if (_count < 0) {
        _count = std::numeric_limits<int32_t>::max();
    }
    _queryPool = context.queryPool;
    return true;
}

bool SortTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    if (input != nullptr) {
        if (!doCompute(input)) {
            return false;
        }
    }
    if (eof) {
        if (_comparator != nullptr && _table != nullptr) {
            TableUtil::sort(_table, _comparator.get());
        }
        output = _table;
    }
    return true;
}

bool SortTvfFunc::doCompute(const TablePtr &input) {
    if (_comparator == nullptr) {
        _table = input;
        _comparator = ComparatorCreator::createComboComparator(input, _sortFields, _sortFlags, _queryPool);
        if (_comparator == nullptr) {
            SQL_LOG(ERROR, "init sort tvf combo comparator [%s] failed.", _sortStr.c_str());
            return false;
        }
    } else {
        if (!_table->merge(input)) {
            SQL_LOG(ERROR, "sort tvf merge input table failed");
            return false;
        }
    }
    TableUtil::topK(_table, _comparator.get(), _count);
    _table->compact();
    return true;
}

SortTvfFuncCreator::SortTvfFuncCreator()
    : TvfFuncCreator(sortTvfDef, sortTvfInfo)
{}

SortTvfFuncCreator::~SortTvfFuncCreator() {
}

TvfFunc *SortTvfFuncCreator::createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) {
    return new SortTvfFunc();
}

END_HA3_NAMESPACE(builtin);
