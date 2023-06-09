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
#include "indexlib/base/FieldTypeUtil.h"

namespace indexlib {

bool IsIntegerField(FieldType ft)
{
    return ft == ft_int8 || ft == ft_int16 || ft == ft_int32 || ft == ft_int64 || ft == ft_uint8 || ft == ft_uint16 ||
           ft == ft_uint32 || ft == ft_uint64;
}

bool IsNumericField(FieldType ft) { return IsIntegerField(ft) || ft == ft_float || ft == ft_double; }

} // namespace indexlib
