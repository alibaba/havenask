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
#pragma once

#include <memory>
#include <utility>

#include "navi/engine/Data.h"
#include "sql/common/common.h"
#include "sql/data/SqlFormatType.h"
#include "sql/data/SqlQueryRequest.h"

namespace sql {

class SqlFormatData : public navi::Data {
public:
    SqlFormatData(const std::string &format)
        : navi::Data(SqlFormatType::TYPE_ID)
        , _format(format) {}
    SqlFormatData(SqlQueryRequest *sqlQueryRequest)
        : navi::Data(SqlFormatType::TYPE_ID) {
        initFormat(sqlQueryRequest);
    }
    ~SqlFormatData() {}

private:
    SqlFormatData(const SqlFormatData &) = delete;
    SqlFormatData &operator=(const SqlFormatData &) = delete;

public:
    const std::string &getSqlFormat() const {
        return _format;
    }

private:
    void initFormat(SqlQueryRequest *sqlQueryRequest) {
        if (!sqlQueryRequest) {
            return;
        }
        sqlQueryRequest->getValue(SQL_FORMAT_TYPE, _format);
        if (_format.empty()) {
            sqlQueryRequest->getValue(SQL_FORMAT_TYPE_NEW, _format);
        }
    }

private:
    std::string _format;
};

typedef std::shared_ptr<SqlFormatData> SqlFormatDataPtr;

} // namespace sql
