#include <ha3/sql/ops/tvf/builtin/DistinctTopNTvfFunc.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/rank/ComboComparator.h>
#include <ha3/sql/rank/ComparatorCreator.h>
#include <ha3/sql/ops/agg/AggBase.h>

using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(sql);
const SqlTvfProfileInfo distinctTopNTvfInfo("distinctTopNTvf", "distinctTopNTvf");
const string distinctTopNTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "distinctTopNTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false,
      "normal_scope": false
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
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             },
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

DistinctTopNTvfFunc::DistinctTopNTvfFunc()
    : _totalLimit(0)
    , _perGroupLimit(0)
    , _partTotalLimit(0)
    , _partPerGroupLimit(0)
{
}

DistinctTopNTvfFunc::~DistinctTopNTvfFunc() {
}

bool DistinctTopNTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() < 6u) {
        SQL_LOG(WARN, "param size [%ld] not equal 6", context.params.size());
        return false;
    }
    _groupStr = context.params[0];
    _groupFields = StringUtil::split(context.params[0], ",");
    _sortStr = context.params[1];
    _sortFields = StringUtil::split(context.params[1], ",");
    if (_sortFields.size() == 0) {
        SQL_LOG(WARN, "parse order [%s] fail.", context.params[1].c_str());
        return false;
    }
    for (size_t i = 0; i < _sortFields.size(); i++) {
        const string &field = _sortFields[i];
        if (field.size() == 0) {
            SQL_LOG(WARN, "parse order [%s] fail.", context.params[1].c_str());
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
    if (!StringUtil::fromString(context.params[2], _totalLimit)) {
        SQL_LOG(WARN, "parse param [%s] fail.", context.params[2].c_str());
        return false;
    }
    if (_totalLimit < 1) {
        SQL_LOG(WARN, "parse param [%s] fail. count < 1", context.params[2].c_str());
        return false;
    }
    if (!StringUtil::fromString(context.params[3], _perGroupLimit)) {
        _perGroupLimit = 0;
    }
    if (_perGroupLimit < 0) {
        SQL_LOG(WARN, "parse param [%s] fail. count < 0", context.params[3].c_str());
        return false;
    }
    StringUtil::fromString(context.params[4], _partTotalLimit);
    if (_partTotalLimit < 0) {
        _partTotalLimit = 0;
    } else if (_partTotalLimit > _totalLimit) {
        _partTotalLimit = _totalLimit;
    }
    StringUtil::fromString(context.params[5], _partPerGroupLimit);
    if (_partPerGroupLimit < 0) {
        _partPerGroupLimit = 0;
    } else if (_partPerGroupLimit > _perGroupLimit) {
        _partPerGroupLimit = _perGroupLimit;
    }
    _queryPool = context.queryPool;
    return true;
}

bool DistinctTopNTvfFunc::computeTable(const TablePtr &table) {
    auto totalLimit = _totalLimit;
    auto perGroupLimit = _perGroupLimit;
    if (getScope() == KernelScope::PARTIAL) {
        totalLimit = _partTotalLimit;
        perGroupLimit = _partPerGroupLimit;
    }
    ComboComparatorPtr comparator = ComparatorCreator::createComboComparator(
            table, _sortFields, _sortFlags, _queryPool);
    if (comparator == nullptr) {
        SQL_LOG(ERROR, "init distinctTopN tvf combo comparator [%s] failed.", _sortStr.c_str());
        return false;
    }
    vector<Row> sortRowVec = table->getRows();
    const auto& cmp = [&comparator](Row a, Row b) -> bool {
        return comparator->compare(a, b);
    };
    sort(sortRowVec.begin(), sortRowVec.end(), cmp);
    table->setRows(sortRowVec);
    sortRowVec.clear();
    if (table->getRowCount() <= (size_t)totalLimit) {
        return true;
    }
    if (_groupFields.empty() || perGroupLimit == 0) {
        table->clearBackRows(table->getRowCount() - (size_t)totalLimit);
        return true;
    }
    vector<size_t> groupKeys;
    if (!AggBase::calculateGroupKeyHash(table, _groupFields, groupKeys)) {
        SQL_LOG(ERROR, "calculate group key [%s] hash failed", _groupStr.c_str());
        return false;
    }
    if (table->getRowCount() != groupKeys.size()) {
        return false;
    }
    unordered_map<size_t, int64_t> groupCountMap;
    vector<Row> backupRowVec;
    groupCountMap.reserve(totalLimit / perGroupLimit);
    sortRowVec.reserve(totalLimit);
    backupRowVec.reserve(totalLimit);
    const auto& rawRowVec = table->getRows();
    for (size_t i = 0; i < rawRowVec.size(); ++i) {
        const auto& row = rawRowVec[i];
        auto groupKey = groupKeys[i];
        auto& groupCount = groupCountMap[groupKey];
        if (groupCount < perGroupLimit) {
            sortRowVec.push_back(row);
            ++groupCount;
            if (sortRowVec.size() == (size_t)totalLimit) {
                break;
            }
        }
        else if (backupRowVec.size() < (size_t)totalLimit) {
            backupRowVec.push_back(row);
        }
    }
    if (sortRowVec.size() == (size_t)totalLimit) {
        table->setRows(sortRowVec);
        return true;
    }
    size_t backupCount = totalLimit - sortRowVec.size();
    if (backupRowVec.size() > backupCount) {
        backupRowVec.erase(backupRowVec.begin() + backupCount, backupRowVec.end());
    }
    vector<Row> outRowVec(totalLimit);
    merge(
        sortRowVec.begin(), sortRowVec.end(),
        backupRowVec.begin(), backupRowVec.end(),
        outRowVec.begin(), cmp);
    table->setRows(outRowVec);
    return true;
}

bool DistinctTopNTvfFunc::doCompute(const TablePtr &input, TablePtr &output) {
    if (computeTable(input)) {
        output = input;
        return true;
    }
    else {
        return false;
    }
}

DistinctTopNTvfFuncCreator::DistinctTopNTvfFuncCreator()
    : TvfFuncCreator(distinctTopNTvfDef, distinctTopNTvfInfo)
{}

DistinctTopNTvfFuncCreator::~DistinctTopNTvfFuncCreator() {
}

TvfFunc *DistinctTopNTvfFuncCreator::createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) {
    return new DistinctTopNTvfFunc();
}

END_HA3_NAMESPACE(builtin);
