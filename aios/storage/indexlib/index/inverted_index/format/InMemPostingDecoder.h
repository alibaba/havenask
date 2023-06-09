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

#include "indexlib/index/inverted_index/format/InMemDocListDecoder.h"
#include "indexlib/index/inverted_index/format/InMemPositionListDecoder.h"

namespace indexlib::index {

class InMemPostingDecoder
{
public:
    InMemPostingDecoder();
    ~InMemPostingDecoder();

    void SetDocListDecoder(InMemDocListDecoder* docListDecoder) { _docListDecoder = docListDecoder; }

    InMemDocListDecoder* GetInMemDocListDecoder() const { return _docListDecoder; }

    void SetPositionListDecoder(InMemPositionListDecoder* positionListDecoder)
    {
        _positionListDecoder = positionListDecoder;
    }

    InMemPositionListDecoder* GetInMemPositionListDecoder() const { return _positionListDecoder; }

private:
    InMemDocListDecoder* _docListDecoder;
    InMemPositionListDecoder* _positionListDecoder;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
