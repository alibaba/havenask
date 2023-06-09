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
#ifndef __INDEXLIB_SIMPLE_TABLE_READER_H
#define __INDEXLIB_SIMPLE_TABLE_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/table/table_reader.h"
#include "indexlib/test/result.h"

namespace indexlib { namespace test {

class SimpleTableReader : public table::TableReader
{
public:
    SimpleTableReader() {}
    ~SimpleTableReader() {}

public:
    virtual ResultPtr Search(const std::string& query) const = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SimpleTableReader);
}} // namespace indexlib::test

#endif //__INDEXLIB_SIMPLE_TABLE_READER_H
