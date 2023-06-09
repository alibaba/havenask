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
#include "build_service/reader/FilePatternBase.h"

#include "build_service/reader/FilePattern.h"

using namespace std;

namespace build_service { namespace reader {

bool FilePatternBase::isPatternConsistent(const FilePatternBasePtr& lft, const FilePatternBasePtr& rhs)
{
    FilePatternPtr leftPattern = std::dynamic_pointer_cast<FilePattern>(lft);
    FilePatternPtr rightPattern = std::dynamic_pointer_cast<FilePattern>(rhs);
    if (!leftPattern || !rightPattern) {
        return false;
    }
    return leftPattern->getFileCount() == rightPattern->getFileCount();
}

}} // namespace build_service::reader
