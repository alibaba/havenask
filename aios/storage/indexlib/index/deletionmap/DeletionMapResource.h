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

#include "autil/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/IResource.h"
#include "indexlib/util/ExpandableBitmap.h"

namespace indexlibv2::index {

class DeletionMapResource : public indexlibv2::framework::IResource
{
public:
    DeletionMapResource(size_t docCount) : _docCount(docCount), _bitmap((uint32_t)docCount) {}
    DeletionMapResource(size_t docCount, indexlib::util::ExpandableBitmap bitmap) : _docCount(docCount), _bitmap(bitmap)
    {
    }

    bool Delete(docid_t docId)
    {
        if (docId >= _docCount) {
            _docCount = docId + 1;
        }
        return _bitmap.Set(docId);
    }
    bool IsDeleted(docid_t docId) const
    {
        if (_docCount > docId) {
            return _bitmap.Test(docId);
        }
        return false;
    }

    bool ApplyBitmap(indexlib::util::Bitmap* bitmap)
    {
        if (bitmap && bitmap->GetItemCount() == _bitmap.GetItemCount()) {
            _bitmap |= *bitmap;
            return true;
        }
        return false;
    }

    size_t CurrentMemmoryUse() const override { return _bitmap.Size(); }
    static std::string GetResourceName(segmentid_t segmentId)
    {
        return "deletionmap_data_resource_name_" + std::to_string(segmentId);
    }
    std::shared_ptr<IResource> Clone() override { return std::make_shared<DeletionMapResource>(_docCount, _bitmap); }

    static std::shared_ptr<DeletionMapResource> DeletionMapResourceCreator(size_t docCount)
    {
        return std::make_shared<DeletionMapResource>(docCount);
    }

private:
    size_t _docCount;
    indexlib::util::ExpandableBitmap _bitmap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
