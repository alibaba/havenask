#include <ha3/sql/ops/tvf/builtin/PrintTableTvfFunc.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <ha3/sql/data/TableUtil.h>
using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(sql);

const SqlTvfProfileInfo printTableTvfInfo("printTableTvf", "printTableTvf");
const string printTableTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "printTableTvf",
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

PrintTableTvfFunc::PrintTableTvfFunc() {
    _count = 0;
}

PrintTableTvfFunc::~PrintTableTvfFunc() {
}

bool PrintTableTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 1) {
        SQL_LOG(WARN, "param size [%ld] not equal 1", context.params.size());
        return false;
    }
    if (!StringUtil::fromString(context.params[0], _count)) {
        SQL_LOG(WARN, "parse param [%s] fail.", context.params[0].c_str());
        return false;
    }
    return true;
}

bool PrintTableTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    SQL_LOG(INFO, "add debug info:\n %s", TableUtil::toString(input, _count).c_str());
    output = input;
    return true;
}

PrintTableTvfFuncCreator::PrintTableTvfFuncCreator()
    : TvfFuncCreator(printTableTvfDef, printTableTvfInfo)
{}

PrintTableTvfFuncCreator::~PrintTableTvfFuncCreator() {
}

TvfFunc *PrintTableTvfFuncCreator::createFunction(
        const HA3_NS(config)::SqlTvfProfileInfo &info) {
    return new PrintTableTvfFunc();
}

END_HA3_NAMESPACE(builtin);
