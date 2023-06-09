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
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"

#include "indexlib/index/inverted_index/builtin_index/bitmap/OnDiskBitmapIndexIterator.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlib { namespace index { namespace legacy {

OnDiskIndexIteratorCreator::OnDiskIndexIteratorCreator() {}

OnDiskIndexIteratorCreator::~OnDiskIndexIteratorCreator() {}

OnDiskIndexIterator* OnDiskIndexIteratorCreator::CreateBitmapIterator(const DirectoryPtr& indexDirectory) const
{
    return new OnDiskBitmapIndexIterator(indexDirectory);
}
}}} // namespace indexlib::index::legacy
