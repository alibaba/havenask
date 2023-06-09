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

#include "autil/Log.h"
#include "indexlib/file_system/ByteSliceReader.h"

namespace indexlib { namespace index {

class TermPostingInfo
{
public:
    TermPostingInfo();
    ~TermPostingInfo();

    uint32_t GetPostingSkipSize() const { return _postingSkipSize; }
    void SetPostingSkipSize(uint32_t postingSkipSize) { _postingSkipSize = postingSkipSize; }
    uint32_t GetPostingListSize() const { return _postingListSize; }
    void SetPostingListSize(uint32_t postingListSize) { _postingListSize = postingListSize; }
    uint32_t GetPositionSkipSize() const { return _positionSkipSize; }
    void SetPositionSkipSize(uint32_t positionSkipSize) { _positionSkipSize = positionSkipSize; }
    uint32_t GetPositionListSize() const { return _positionListSize; }
    void SetPositionListSize(uint32_t positionListSize) { _positionListSize = positionListSize; }
    void LoadPosting(file_system::ByteSliceReader* sliceReader);
    void LoadPositon(file_system::ByteSliceReader* sliceReader);

private:
    uint32_t _postingSkipSize;
    uint32_t _postingListSize;
    uint32_t _positionSkipSize;
    uint32_t _positionListSize;

    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::index
