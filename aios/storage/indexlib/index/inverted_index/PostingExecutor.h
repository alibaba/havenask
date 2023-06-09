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

#include "indexlib/base/Constant.h"
#include "indexlib/index/inverted_index/Constant.h"

namespace indexlib::index {

class PostingExecutor
{
public:
    PostingExecutor() : _current(INVALID_DOCID) {}
    virtual ~PostingExecutor() {}

public:
    virtual df_t GetDF() const = 0;
    docid_t Seek(docid_t id)
    {
        if (id > _current) {
            _current = DoSeek(id);
        }
        return _current;
    }

    bool Test(docid_t id)
    {
        if (id < 0 || id == END_DOCID) {
            return false;
        }
        return Seek(id) == id;
    }

private:
    virtual docid_t DoSeek(docid_t id) = 0;

protected:
    docid_t _current;
};

} // namespace indexlib::index
