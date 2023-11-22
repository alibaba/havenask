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
#include "indexlib/index/inverted_index/TermPostingInfo.h"

using namespace indexlib::file_system;

namespace indexlib { namespace index {
AUTIL_LOG_SETUP(indexlib.index, TermPostingInfo);

TermPostingInfo::TermPostingInfo()
    : _postingSkipSize(0)
    , _postingListSize(0)
    , _positionSkipSize(0)
    , _positionListSize(0)
{
}

TermPostingInfo::~TermPostingInfo() {}

void TermPostingInfo::LoadPosting(ByteSliceReader* sliceReader)
{
    assert(sliceReader != nullptr);
    _postingSkipSize = sliceReader->ReadVUInt32().GetOrThrow();
    _postingListSize = sliceReader->ReadVUInt32().GetOrThrow();
}

void TermPostingInfo::LoadPositon(ByteSliceReader* sliceReader)
{
    assert(sliceReader != nullptr);
    _positionSkipSize = sliceReader->ReadVUInt32().GetOrThrow();
    _positionListSize = sliceReader->ReadVUInt32().GetOrThrow();
}
}} // namespace indexlib::index
