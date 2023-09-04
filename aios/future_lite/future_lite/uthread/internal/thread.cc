/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#ifdef FL_ASAN_ENABLED
#include <ucontext.h>
#endif
#include <algorithm>
#include <string>

#include <future_lite/Common.h>
#include <future_lite/uthread/internal/thread.h>

namespace future_lite {
namespace uthread {
namespace internal {

thread_local jmp_buf_link g_unthreaded_context;
thread_local jmp_buf_link* g_current_context = nullptr;

static const std::string uthread_stack_size = "UTHREAD_STACK_SIZE_KB";
size_t get_base_stack_size() {
  static size_t stack_size = 0;
  if (stack_size) {
    return stack_size;
  }
  auto env = std::getenv(uthread_stack_size.data());
  if (env) {
    auto kb = std::strtoll(env, nullptr, 10);
    if (kb > 0 && kb < std::numeric_limits<size_t>::max()) {
      stack_size = 1024 * kb;
      return stack_size;
    }
  }
  stack_size = default_base_stack_size;
  return stack_size;
}

#ifdef FL_ASAN_ENABLED

namespace {

// ASan provides two functions as a means of informing it that user context
// switch has happened. First __sanitizer_start_switch_fiber() needs to be
// called with a place to store the fake stack pointer and the new stack
// information as arguments. Then, ucontext switch may be performed after which
// __sanitizer_finish_switch_fiber() needs to be called with a pointer to the
// current context fake stack and a place to store stack information of the
// previous ucontext.

extern "C" {
void __sanitizer_start_switch_fiber(void** fake_stack_save, const void* stack_bottom, size_t stack_size);
void __sanitizer_finish_switch_fiber(void* fake_stack_save, const void** stack_bottom_old, size_t* stack_size_old);
}

}  // namespace

void jmp_buf_link::switch_in() {
  link = std::exchange(g_current_context, this);
  if (FL_UNLIKELY(!link)) {
    link = &g_unthreaded_context;
  }
  __sanitizer_start_switch_fiber(&link->fake_stack, stack_bottom, stack_size);
  fcontext = _fl_jump_fcontext(fcontext, thread).fctx;
  __sanitizer_finish_switch_fiber(g_current_context->fake_stack, &stack_bottom, &stack_size);
}

void jmp_buf_link::switch_out() {
  g_current_context = link;
  __sanitizer_start_switch_fiber(&fake_stack, g_current_context->stack_bottom, g_current_context->stack_size);
  auto from = _fl_jump_fcontext(link->fcontext, nullptr).fctx;
  link->fcontext = from;
  __sanitizer_finish_switch_fiber(g_current_context->fake_stack, &link->stack_bottom, &link->stack_size);
}

void jmp_buf_link::initial_switch_in_completed() {
  // This is a new thread and it doesn't have the fake stack yet. ASan will
  // create it lazily, for now just pass nullptr.
  __sanitizer_finish_switch_fiber(nullptr, &link->stack_bottom, &link->stack_size);
}

#else

inline void jmp_buf_link::switch_in() {
  link = std::exchange(g_current_context, this);
  if (FL_UNLIKELY(!link)) {
    link = &g_unthreaded_context;
  }
#if defined(UTHREAD_SPLIT_STACK)
  split_stack_swap_context(thread->stack_.link, thread->stack_.ss_ctx);
#endif
  fcontext = _fl_jump_fcontext(fcontext, thread).fctx;
}

inline void jmp_buf_link::switch_out() {
  g_current_context = link;
#if defined(UTHREAD_SPLIT_STACK)
  split_stack_swap_context(thread->stack_.ss_ctx, thread->stack_.link);
#endif
  auto from = _fl_jump_fcontext(link->fcontext, nullptr).fctx;
  link->fcontext = from;
}

inline void jmp_buf_link::initial_switch_in_completed() {}

#endif

thread_context::thread_context(std::function<void()> func,
                               Executor* executor,
                               CoroSpecStorage *parent)
#ifdef SEASTAR_THREAD_STACK_GUARDS
  : stack_size_(get_base_stack_size() + getpagesize())
#else
  : stack_size_(get_base_stack_size())
#endif
  , func_(std::move(func))
  , storage_(parent) {
  done_.setExecutor(executor);
  if (!allocate_stack(stack_, stack_size_)) {
    throw std::bad_alloc{};
  }
  setup();
}

thread_context::~thread_context() {
#ifdef SEASTAR_THREAD_STACK_GUARDS
  auto mp_result = mprotect(stack_.bottom, getpagesize(), PROT_READ | PROT_WRITE);
  assert(mp_result == 0);
#endif
  for (auto& data : storage_._varMap) {
    data.second.destroy();
  }
  storage_._varMap.clear();
  deallocate_stack(stack_);
}

void thread_context::setup() {
#ifdef SEASTAR_THREAD_STACK_GUARDS
  size_t page_size = getpagesize();
  assert(align_up(stack_.bottom, page_size) == stack_.bottom);
  auto mp_status = mprotect(stack_.bottom, page_size, PROT_READ);
  throw_system_error_on(mp_status != 0, "mprotect");
#endif
  context_.fcontext =
    _fl_make_fcontext(stack_.bottom + stack_.size, stack_.size, thread_context::s_main);
  context_.thread = this;
#ifdef FL_ASAN_ENABLED
  context_.stack_bottom = stack_.bottom;
  context_.stack_size = stack_.size;
#endif
  context_.switch_in();
}

void thread_context::switch_in() {
  context_.switch_in();
}

void thread_context::switch_out() {
  context_.switch_out();
}

void thread_context::s_main(transfer_t t) {
  auto q = reinterpret_cast<thread_context*>(t.data);
  q->context_.link->fcontext = t.fctx;
  q->main();
}

void thread_context::main() {
#ifdef __x86_64__
  // There is no caller of main() in this context. We need to annotate this frame like this so that
  // unwinders don't try to trace back past this frame.
  // See https://github.com/scylladb/scylla/issues/1909.
  asm(".cfi_undefined rip");
#elif defined(__PPC__)
  asm(".cfi_undefined lr");
#elif defined(__aarch64__)
  asm(".cfi_undefined x30");
#else
#warning "Backtracing from seastar threads may be broken"
#endif
  context_.initial_switch_in_completed();
  try {
    func_();
    done_.setValue(true);
  } catch (...) {
    done_.setException(std::current_exception());
  }

#ifdef FL_ASAN_ENABLED
  context_.fake_stack = nullptr;
#endif
  context_.switch_out();
}

bool morestack_triggered() {
#if defined(UTHREAD_SPLIT_STACK)
  if (!thread_impl::can_switch_out()) {
    return false;
  }
  auto ss = reinterpret_cast<stack_segment*>(g_current_context->thread->stack_.ss_ctx[MORESTACK_SEGMENTS]);
  return ss->next != nullptr;
#else
  return false;
#endif
}

uint32_t stack_segment_num() {
#if defined(UTHREAD_SPLIT_STACK)
  if (!thread_impl::can_switch_out()) {
    return 0;
  }
  auto ss = reinterpret_cast<stack_segment*>(g_current_context->thread->stack_.ss_ctx[MORESTACK_SEGMENTS]);
  uint32_t count = 0;
  while (ss != nullptr) {
    ++count;
    ss = ss->next;
  }
  return count;
#else
  return 0;
#endif
}

namespace thread_impl {

void switch_in(thread_context* to) {
  to->switch_in();
}

void switch_out(thread_context* from) {
  from->switch_out();
}

bool can_switch_out() {
  return g_current_context && g_current_context->thread;
}

}  // namespace thread_impl

} // namespace internal
} // namespace uthread
} // namespace fl
