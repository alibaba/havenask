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

#include <map>
#include <memory>
#include <set>
#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "ha3/sql/common/Log.h"    // IWYU pragma: keep
#include "ha3/sql/common/common.h" // IWYU pragma: keep
#include "ha3/sql/framework/ErrorResult.h"
#include "ha3/sql/framework/TsdbResult.h"
#include "table/Table.h"

namespace isearch {
namespace sql {

class TsdbTableFormatterBase {
public:
    TsdbTableFormatterBase() {};
    virtual ~TsdbTableFormatterBase() {};
    TsdbTableFormatterBase(const TsdbTableFormatterBase &) = delete;
    TsdbTableFormatterBase &operator=(const TsdbTableFormatterBase &) = delete;
public:
    virtual ErrorResult convertToTsdb(const table::TablePtr &table,
            const std::string &formatDesc,
            TsdbResults &tsdbResults) = 0;
    virtual ErrorResult convertToTsdb(const table::TablePtr &table,
            const std::string &formatDesc,
            float integrity,
            TsdbResults &tsdbResults) = 0;
};

typedef std::shared_ptr<TsdbTableFormatterBase> TsdbTableFormatterBasePtr;

class TsdbTableFormatter : public TsdbTableFormatterBase {
public:
    TsdbTableFormatter();
    ~TsdbTableFormatter();
    TsdbTableFormatter(const TsdbTableFormatter &) = delete;
    TsdbTableFormatter &operator=(const TsdbTableFormatter &) = delete;
public:
    static TsdbTableFormatterBase* create() { return new TsdbTableFormatter; }
    static const std::string type() { return "TsdbTableFormatter"; }
public:
    ErrorResult convertToTsdb(const table::TablePtr &table,
                                     const std::string &formatDesc,
                                     TsdbResults &tsdbResults);
    ErrorResult convertToTsdb(const table::TablePtr &table,
                                     const std::string &formatDesc,
                                     float integrity,
                                     TsdbResults &tsdbResults);
private:
    static ErrorResult convertTable(const table::TablePtr &table,
                                    std::map<std::string, std::string> &tsdbFieldsMap,
                                    TsdbResults &tsdbResults);
    static ErrorResult convertTable(const table::TablePtr &table,
                                    std::map<std::string, std::string> &tsdbFieldsMap,
                                    float integrity,
                                    TsdbResults &tsdbResults,
                                    size_t memoryLimit = SQL_TSDB_RESULT_MEM_LIMIT);

    static bool parseTsdbFormatDesc(const std::string &formatDesc,
                                    std::map<std::string, std::string> &tsdbFieldsMap);

private:
    static const std::string SQL_TSDB_DPS;
    static const std::string SQL_TSDB_STRDPS;
    static const std::string SQL_TSDB_METRIC;
    static const std::string SQL_TSDB_SUMMARY;
    static const std::string SQL_TSDB_MESSAGE_WATERMARK;
    static const std::string SQL_TSDB_INTEGRITY;
    static const std::string SQL_TSDB_PAIR_SEP;
    static const std::string SQL_TSDB_KV_SEP;
    static std::set<std::string> tsdbKeySet;

private:
    static const bool registered;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TsdbTableFormatter> TsdbTableFormatterPtr;
} // namespace sql
} // namespace isearch
