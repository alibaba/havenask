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
#ifndef __INDEXLIB_ORDERED_PRIMARY_KEY_ITERATOR_H
#define __INDEXLIB_ORDERED_PRIMARY_KEY_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename Key>
class OrderedPrimaryKeyIterator : public PrimaryKeyIterator<Key>
{
public:
    OrderedPrimaryKeyIterator(const config::IndexConfigPtr& indexConfig) : PrimaryKeyIterator<Key>(indexConfig) {}
    virtual ~OrderedPrimaryKeyIterator() {}

public:
    uint64_t GetPkCount() const override { return 0; }
    uint64_t GetDocCount() const override { return 0; }

    /*    typedef PKPair<Key> PKPair; */

    /* public: */
    /*     virtual void Init(const std::vector<index_base::SegmentData>& segmentDatas) = 0; */
    /*     virtual bool HasNext() const = 0; */
    /*     virtual void Next(PKPair& pair) =  0; */

    /* protected: */
    /*     config::IndexConfigPtr mIndexConfig; */

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index

#endif //__INDEXLIB_ORDERED_PRIMARY_KEY_ITERATOR_H
