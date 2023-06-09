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

#include "indexlib/index/inverted_index/InDocPositionState.h"
#include "indexlib/util/ObjectPool.h"

namespace indexlib::index {

class BitmapInDocPositionState : public InDocPositionState
{
public:
    BitmapInDocPositionState();
    ~BitmapInDocPositionState();

    InDocPositionIterator* CreateInDocIterator() const override;
    void Clone(BitmapInDocPositionState* state) const;

    void SetDocId(docid_t docId) { _docId = docId; }
    docid_t GetDocId() const { return _docId; }

    void SetOwner(util::ObjectPool<BitmapInDocPositionState>* owner) { _owner = owner; }
    void Free() override;

protected:
    util::ObjectPool<BitmapInDocPositionState>* _owner;
    docid_t _docId; // local Id
    friend class BitmapInDocPositionIterator;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
