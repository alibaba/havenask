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

namespace future_lite {
namespace uthread {
namespace internal {

///////////////////////////
// forward declaration for splitstack-functions defined in libgcc
// https://github.com/gcc-mirror/gcc/blob/master/libgcc/generic-morestack.c
// https://github.com/gcc-mirror/gcc/blob/master/libgcc/config/i386/morestack.S

struct dynamic_allocation_blocks {
  /* The next block in the list.  */
  struct dynamic_allocation_blocks *next;
  /* The size of the allocated memory.  */
  size_t size;
  /* The allocated memory.  */
  void *block;
};

struct stack_segment {
  /* The previous stack segment--when a function running on this stack
     segment returns, it will run on the previous one.  */
  struct stack_segment *prev;
  /* The next stack segment, if it has been allocated--when a function
     is running on this stack segment, the next one is not being
     used.  */
  struct stack_segment *next;
  /* The total size of this stack segment.  */
  size_t size;
  /* The stack address when this stack was created.  This is used when
     popping the stack.  */
  void *old_stack;
  /* A list of memory blocks allocated by dynamic stack
     allocation.  */
  struct dynamic_allocation_blocks *dynamic_allocation;
  /* A list of dynamic memory blocks no longer needed.  */
  struct dynamic_allocation_blocks *free_dynamic_allocation;
  /* An extra pointer in case we need some more information some
     day.  */
  void *extra;
};

enum splitstack_context_offsets {
  MORESTACK_SEGMENTS = 0,
  CURRENT_SEGMENT = 1,
  CURRENT_STACK = 2,
  STACK_GUARD = 3,
  INITIAL_SP = 4,
  INITIAL_SP_LEN = 5,
  BLOCK_SIGNALS = 6,

  NUMBER_OFFSETS = 10
};

extern "C" {
  void *__splitstack_makecontext(std::size_t, void* [NUMBER_OFFSETS], std::size_t*);

  void __splitstack_setcontext(void* [NUMBER_OFFSETS]);

  void __splitstack_getcontext(void* [NUMBER_OFFSETS]);

  void __splitstack_releasecontext(void* [NUMBER_OFFSETS]);

  void __splitstack_resetcontext(void* [NUMBER_OFFSETS]);

  void __splitstack_block_signals_context(void* [NUMBER_OFFSETS], int* new_value, int* old_value);
}

} // namespace internal
} // namespace uthread
} // namespace fl
