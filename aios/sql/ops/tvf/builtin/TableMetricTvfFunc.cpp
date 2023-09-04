/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "sql/ops/tvf/builtin/TableMetricTvfFunc.h"

#include <cstddef>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "navi/engine/Resource.h"
#include "sql/ops/tvf/SqlTvfProfileInfo.h"
#include "table/Row.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor
using namespace std;
using namespace table;
using namespace kmonitor;

namespace sql {

class TableMetricTvfOpMetrics : public MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_rowCount, "RowCount");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, size_t *count) {
        REPORT_MUTABLE_METRIC(_rowCount, *count);
    }

private:
    MutableMetric *_rowCount = nullptr;
};
bool TableMetricTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 1u) {
        SQL_LOG(ERROR, "Param size [%ld] not equal 1", context.params.size());
        return false;
    }
    _tagName = context.params[0];

    _metricReporter = context.metricReporter;
    return true;
}

bool TableMetricTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    output = input;

    size_t counter = output->getRowCount();
    if (_metricReporter && !_tagName.empty()) {
        auto reporter
            = _metricReporter->getSubReporter("sql.user_define", {{{"table_name", _tagName}}});
        reporter->report<TableMetricTvfOpMetrics, size_t>(nullptr, &counter);
    }
    SQL_LOG(DEBUG, "Metric reporter: [%p], tag name: [%s]", _metricReporter, _tagName.c_str());

    return true;
}

static const SqlTvfProfileInfo METRICREPORT_TVF_FUNCINFO("tableMetricTvf", "tableMetricTvf");
static const string METRICREPORT_TVF_DEF = R"tvf_json(
{
  "function_version": 1,
  "function_name": "tableMetricTvf",
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

TableMetricTvfFuncCreator::TableMetricTvfFuncCreator()
    : TvfFuncCreatorR(METRICREPORT_TVF_DEF, METRICREPORT_TVF_FUNCINFO) {}

TvfFunc *TableMetricTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new TableMetricTvfFunc();
}

REGISTER_RESOURCE(TableMetricTvfFuncCreator);

} // namespace sql
