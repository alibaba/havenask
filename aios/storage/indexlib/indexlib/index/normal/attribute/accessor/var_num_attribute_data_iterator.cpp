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
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_data_iterator.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, VarNumAttributeDataIterator);

bool VarNumAttributeDataIterator::HasNext()
{
    if (mCursor == INVALID_CURSOR) {
        return mOffsets->Size() > 0;
    }

    return mCursor < mOffsets->Size() - 1;
}

void VarNumAttributeDataIterator::Next()
{
    if (mCursor == INVALID_CURSOR) {
        mCursor = 0;
    } else {
        mCursor++;
    }

    if (mCursor >= mOffsets->Size()) {
        return;
    }

    mCurrentOffset = (*mOffsets)[mCursor];
    uint8_t* buffer = mData->Search(mCurrentOffset);
    mDataLength = *(uint32_t*)buffer;
    mCurrentData = buffer + sizeof(uint32_t);
}

void VarNumAttributeDataIterator::GetCurrentData(uint64_t& dataLength, uint8_t*& data)
{
    if (mCursor == INVALID_CURSOR || mCursor == mOffsets->Size()) {
        IE_LOG(ERROR, "current cursor is invalid. [%lu]", mCursor);
        data = NULL;
        dataLength = 0;
        return;
    }
    dataLength = mDataLength;
    data = mCurrentData;
}

void VarNumAttributeDataIterator::GetCurrentOffset(uint64_t& offset)
{
    if (mCursor == INVALID_CURSOR || mCursor == mOffsets->Size()) {
        IE_LOG(ERROR, "current cursor is invalid. [%lu]", mCursor);
        offset = 0;
        return;
    }
    offset = mCurrentOffset;
}
}} // namespace indexlib::index
