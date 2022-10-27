#include <ha3/sql/ops/tvf/builtin/EnableShuffleTvfFunc.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <ha3/sql/data/TableUtil.h>
using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(sql);
const SqlTvfProfileInfo enableShuffleTvfInfo("enableShuffleTvf", "enableShuffleTvf");
const string enableShuffleTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "enableShuffleTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": true
    },
    "prototypes": [
      {
        "params": {
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

EnableShuffleTvfFunc::EnableShuffleTvfFunc() {
}

EnableShuffleTvfFunc::~EnableShuffleTvfFunc() {
}

bool EnableShuffleTvfFunc::init(TvfFuncInitContext &context) {
    return true;
}

bool EnableShuffleTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    output = input;
    return true;
}

EnableShuffleTvfFuncCreator::EnableShuffleTvfFuncCreator()
    : TvfFuncCreator(enableShuffleTvfDef, enableShuffleTvfInfo)
{}

EnableShuffleTvfFuncCreator::~EnableShuffleTvfFuncCreator() {
}

TvfFunc *EnableShuffleTvfFuncCreator::createFunction(
        const HA3_NS(config)::SqlTvfProfileInfo &info)
{
    return new EnableShuffleTvfFunc();
}

END_HA3_NAMESPACE(builtin);
