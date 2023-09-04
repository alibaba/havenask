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

#if defined(UTHREAD_SPLIT_STACK)
#include <future_lite/uthread/internal/split_stack.h>
#endif

namespace future_lite {
namespace uthread {
namespace internal {

struct stack_context {
  char* bottom = nullptr;
  std::size_t size = 0;
#if defined(UTHREAD_SPLIT_STACK) && !defined(FL_ASAN_ENABLED)
  using split_stack_context = void* [NUMBER_OFFSETS];
  split_stack_context ss_ctx{0};
  split_stack_context link{0};
#endif
};

inline void* allocate_stack(stack_context& sc, std::size_t size) {
  sc.size = size;
#if defined(UTHREAD_SPLIT_STACK) && !defined(FL_ASAN_ENABLED)
  sc.bottom = static_cast<char*>(__splitstack_makecontext(sc.size, sc.ss_ctx, &sc.size));
  int off = 0;
  __splitstack_block_signals_context(sc.ss_ctx, &off, 0);
#else

#ifdef SEASTAR_THREAD_STACK_GUARDS
  sc.bottom = new (with_alignment(getpagesize())) char[sc.size];
#else
  sc.bottom = new char[sc.size];
#endif

#ifdef FL_ASAN_ENABLED
  std::fill_n(sc.bottom, sc.size, 0);
#endif

#endif
  return sc.bottom;
}

inline void deallocate_stack(stack_context& sc) noexcept {
#if defined(UTHREAD_SPLIT_STACK) && !defined(FL_ASAN_ENABLED)
  __splitstack_releasecontext(sc.ss_ctx);
#else

#ifdef SEASTAR_THREAD_STACK_GUARDS
  operator delete[](sc.bottom, with_alignment(getpagesize()));
#else
  delete[] sc.bottom;
#endif

#endif
}

#if defined(UTHREAD_SPLIT_STACK)
inline void split_stack_swap_context(stack_context::split_stack_context from,
                                     stack_context::split_stack_context to) {
  __splitstack_getcontext(from);
  __splitstack_setcontext(to);
}
#endif

} // namespace internal
} // namespace uthread
} // namespace fl
