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
#include "indexlib/index/attribute/AttributeReaderFactoryRegister.h"

namespace indexlibv2::index {

DECLARE_ATTRIBUTE_READER_CREATOR_CLASS(StringAttributeReader, ft_string)
template <>
__attribute__((unused)) void RegisterAttributeString(AttributeFactory<AttributeReader, AttributeReaderCreator>* factory)
{
    factory->RegisterSingleValueCreator(std::make_unique<StringAttributeReaderCreator>());
}

} // namespace indexlibv2::index
