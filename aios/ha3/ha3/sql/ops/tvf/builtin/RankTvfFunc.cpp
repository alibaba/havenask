#include <ha3/sql/ops/tvf/builtin/RankTvfFunc.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/rank/ComboComparator.h>
#include <ha3/sql/rank/ComparatorCreator.h>
#include <ha3/sql/ops/agg/AggBase.h>

using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(sql);
const SqlTvfProfileInfo rankTvfInfo("rankTvf", "rankTvf");
const string rankTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "rankTvf",
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

RankTvfFunc::RankTvfFunc() {
    _count = 0;
}

RankTvfFunc::~RankTvfFunc() {
}

bool RankTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 3) {
        SQL_LOG(WARN, "param size [%ld] not equal 3", context.params.size());
        return false;
    }
    _partitionStr = context.params[0];
    _partitionFields = StringUtil::split(context.params[0], ",");
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
    if (!StringUtil::fromString(context.params[2], _count)) {
        SQL_LOG(WARN, "parse param [%s] fail.", context.params[2].c_str());
        return false;
    }
    if (_count < 1) {
        SQL_LOG(WARN, "parse param [%s] fail. count < 1", context.params[2].c_str());
        return false;
    }
    _queryPool = context.queryPool;
    return true;
}

bool RankTvfFunc::doCompute(const TablePtr &input, TablePtr &output) {
    ComboComparatorPtr comparator = ComparatorCreator::createComboComparator(
            input, _sortFields, _sortFlags, _queryPool);
    if (input->getRowCount() <= (size_t)_count) {
        output = input;
        return true;
    }
    if (comparator == nullptr) {
        SQL_LOG(ERROR, "init rank tvf combo comparator [%s] failed.", _sortStr.c_str());
        return false;
    }
    if (_partitionFields.empty()) {
        vector<Row> rowVec = input->getRows();
        vector<Row> sortRowVec = rowVec;
        TableUtil::topK(input, comparator.get(), _count, sortRowVec);
        set<Row> keepRowSet(sortRowVec.begin(), sortRowVec.end());
        vector<Row> keepRowVec; // to keep raw order
        for (auto row : rowVec) {
            if (keepRowSet.count(row) > 0) {
                keepRowVec.push_back(row);
            }
        }
        input->setRows(keepRowVec);
    } else {
        vector<size_t> partitionKeys;
        if (!AggBase::calculateGroupKeyHash(input, _partitionFields, partitionKeys)) {
            SQL_LOG(ERROR, "calculate partition key [%s] hash failed", _partitionStr.c_str());
            return false;
        }
        vector<Row> rowVec = input->getRows();
        if (partitionKeys.size() != rowVec.size()) {
            return false;
        }
        vector<pair<size_t, size_t> > keyOffsetVec;
        keyOffsetVec.resize(partitionKeys.size());
        for (size_t i = 0; i < partitionKeys.size(); i++) {
            keyOffsetVec[i] = make_pair(partitionKeys[i], i);
        }
        stable_sort(keyOffsetVec.begin(), keyOffsetVec.end());
        set<Row> keepRowSet;
        size_t lastKey = 0;
        vector<Row> sortRowVec;
        for (size_t i = 0; i < keyOffsetVec.size(); i++) {
            if (lastKey == keyOffsetVec[i].first) {
                sortRowVec.push_back(rowVec[keyOffsetVec[i].second]);
            } else {
                if (sortRowVec.size() > (size_t)_count) {
                    TableUtil::topK(input, comparator.get(), _count, sortRowVec);
                }
                keepRowSet.insert(sortRowVec.begin(), sortRowVec.end());
                lastKey = keyOffsetVec[i].first;
                sortRowVec.clear();
                sortRowVec.push_back(rowVec[keyOffsetVec[i].second]);
            }
        }
        if (sortRowVec.size() > 0) {
            if (sortRowVec.size() > (size_t)_count) {
                TableUtil::topK(input, comparator.get(), _count, sortRowVec);
            }
            keepRowSet.insert(sortRowVec.begin(), sortRowVec.end());
        }
        vector<Row> keepRowVec; // to keep raw order
        for (auto row : rowVec) {
            if (keepRowSet.count(row) > 0) {
                keepRowVec.push_back(row);
            }
        }
        input->setRows(keepRowVec);
    }
    output = input;
    return true;
}

RankTvfFuncCreator::RankTvfFuncCreator()
    : TvfFuncCreator(rankTvfDef, rankTvfInfo)
{
}

RankTvfFuncCreator::~RankTvfFuncCreator() {
}

TvfFunc *RankTvfFuncCreator::createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) {
    return new RankTvfFunc();
}

END_HA3_NAMESPACE(builtin);
