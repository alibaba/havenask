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
#include "sql/ops/tvf/builtin/TransRowsTvfFunc.h"

#include "autil/MultiValueType.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "sql/common/Log.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/Row.h"

using namespace std;
using namespace autil;
using namespace table;

static const StringView emptyValue {"", 0};

namespace sql {
const SqlTvfProfileInfo transRowsTvfInfo("transRowsTvf", "transRowsTvf");
const string transRowsTvfDef = R"tvf_json(
{
    "function_version": 1,
    "function_name": "transRowsTvf",
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
			    "type": "string"
			},
			{
			    "type": "string"
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
			{
			    "field_name": "trans_cola",
			    "field_type": {
				"type": "string"
			    }
			},
			{
			    "field_name": "trans_colb",
			    "field_type": {
				"type": "string"
			    }
			}
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

TransRowsTvfFunc::TransRowsTvfFunc() {}

TransRowsTvfFunc::~TransRowsTvfFunc() {}

bool TransRowsTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 2) {
        SQL_LOG(WARN, "param size [%ld] not equal 2", context.params.size());
        return false;
    }
    if (!context.params[1].empty()) {
        _multiValueSep = context.params[1];
    }
    _transField = context.params[0];
    _dataPool = context.queryPool;
    return true;
}

bool TransRowsTvfFunc::compute(const table::TablePtr &input, bool eof, table::TablePtr &output) {
    ColumnData<MultiChar> *columnData = input->getColumn(_transField)->getColumnData<MultiChar>();
    if (!columnData) {
        SQL_LOG(WARN, "Field [%s] to trans is not exist", _transField.c_str());
        return false;
    }
    output = input;
    output->declareColumn<MultiChar>("trans_cola");
    output->declareColumn<MultiChar>("trans_colb");
    vector<ColumnData<MultiChar> *> colData(2, nullptr);
    colData[0] = input->getColumn("trans_cola")->getColumnData<MultiChar>();
    colData[1] = input->getColumn("trans_colb")->getColumnData<MultiChar>();
    vector<StringView> transString(2);
    for (size_t rIndex = 0; rIndex < input->getRowCount(); rIndex++) {
        MultiChar colValue = columnData->get(rIndex);
        StringView inString(colValue.data(), colValue.size());
        size_t n = inString.find(_multiValueSep);
        if (n != std::string::npos) {
            transString[0] = inString.substr(0, n);
            transString[1] = inString.substr(n + _multiValueSep.size());
        } else {
            transString[0] = inString;
            transString[1] = emptyValue;
        }
        for (size_t i = 0; i < 2; i++) {
            colData[i]->set(rIndex, transString[i].data(), transString[i].size());
        }
    }
    return true;
}

TransRowsTvfFuncCreator::TransRowsTvfFuncCreator()
    : TvfFuncCreatorR(transRowsTvfDef, transRowsTvfInfo) {}

TransRowsTvfFuncCreator::~TransRowsTvfFuncCreator() {}

TvfFunc *TransRowsTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new TransRowsTvfFunc();
}

REGISTER_RESOURCE(TransRowsTvfFuncCreator);
} // namespace sql
