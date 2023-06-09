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

namespace indexlib { namespace index {
typedef int32_t summaryfieldid_t;
typedef int32_t summarygroupid_t;
}} // namespace indexlib::index

namespace indexlibv2 { namespace index {
using indexlib::index::summaryfieldid_t;
using indexlib::index::summarygroupid_t;
}} // namespace indexlibv2::index
