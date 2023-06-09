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

#include "indexlib/index/inverted_index/format/DocListFormat.h"
#include "indexlib/index/inverted_index/format/PositionListFormat.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"

namespace indexlib::index {

class PostingFormat
{
public:
    PostingFormat(const PostingFormatOption& postingFormatOption);
    ~PostingFormat();

    DocListFormat* GetDocListFormat() const { return _docListFormat; }
    PositionListFormat* GetPositionListFormat() const { return _positionListFormat; }

private:
    DocListFormat* _docListFormat;
    PositionListFormat* _positionListFormat;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
