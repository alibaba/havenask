#include "EchoTvfFunc.h"

#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "sql/common/Log.h"
#include "sql/ops/tvf/TvfFunc.h"
#include "table/Row.h"
#include "table/TableUtil.h"

namespace sql {
class SqlTvfProfileInfo;
} // namespace sql

using namespace std;
using namespace table;
using namespace autil;

namespace sql {
const string echoTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "echoTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
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

EchoTvfFunc::EchoTvfFunc() {
    _count = 0;
}

EchoTvfFunc::~EchoTvfFunc() {}

bool EchoTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 1) {
        SQL_LOG(WARN, "param size [%lu] not equal 1", context.params.size());
        return false;
    }
    if (!StringUtil::fromString(context.params[0], _count)) {
        SQL_LOG(WARN, "parse param [%s] fail.", context.params[0].c_str());
        return false;
    }
    return true;
}

bool EchoTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    SQL_LOG(INFO, "add debug info:\n %s", TableUtil::toString(input, _count).c_str());
    output = input;
    return true;
}

EchoTvfFuncCreator::EchoTvfFuncCreator()
    : TvfFuncCreatorR(echoTvfDef) {}

EchoTvfFuncCreator::~EchoTvfFuncCreator() {}

TvfFunc *EchoTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new EchoTvfFunc();
}

} // namespace sql
