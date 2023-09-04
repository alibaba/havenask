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
#include <string>

namespace table {
class Table;

typedef std::shared_ptr<Table> TablePtr;
} // namespace table

namespace sql {

class PushDownOp {
public:
    PushDownOp() {}
    virtual ~PushDownOp() {};

public:
    virtual bool init() = 0;
    virtual bool compute(table::TablePtr &table, bool &eof) = 0;
    virtual std::string getName() const = 0;
    virtual void setReuseTable(bool reuse) = 0;
    virtual bool isReuseTable() const = 0;
};

typedef std::shared_ptr<PushDownOp> PushDownOpPtr;

} // namespace sql
