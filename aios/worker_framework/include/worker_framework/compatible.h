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

#if __cplusplus >= 201103L
#include <functional>
#include <memory>

#define C_STD_TR1 std
#define UNIQUE_PTR std::unique_ptr

#else
#include <functional>
#include <memory>

#define C_STD_TR1 std::tr1
#define UNIQUE_PTR std::auto_ptr

#endif
