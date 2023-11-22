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

#include "table/Table.h"

namespace sql {

struct LookupJoinBatch {
    std::shared_ptr<table::Table> table;
    size_t offset = 0;
    size_t count = 0;

    void reset(const std::shared_ptr<table::Table> &table_) {
        table = table_;
        offset = 0;
        count = 0;
    }
    bool hasNext() const {
        if (!table || offset + count >= table->getRowCount()) {
            return false;
        } else {
            return true;
        }
    }
    void next(size_t batchNum) {
        offset += count;
        if (table) {
            count = std::min(table->getRowCount() - offset, batchNum);
        } else {
            count = 0;
        }
    }
};

} // namespace sql
