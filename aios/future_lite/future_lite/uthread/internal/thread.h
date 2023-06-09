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

#pragma once

#include <memory>
#include <type_traits>

#include "future_lite/Future.h"
#include "future_lite/uthread/internal/thread_impl.h"
#include "future_lite/uthread/internal/stack_context.h"
#include "future_lite/CoroSpecStorage.h"

namespace future_lite {
namespace uthread {
namespace internal {

static constexpr size_t default_base_stack_size = 512 * 1024;
size_t get_base_stack_size();
bool morestack_triggered();
uint32_t stack_segment_num();
struct stack_context;

class thread_context {
  const size_t stack_size_;
  stack_context stack_;
  std::function<void()> func_;
  jmp_buf_link context_;

 public:
  bool joined_ = false;
  Promise<bool> done_;
  CoroSpecStorage storage_;

 private:
  static void s_main(transfer_t t);
  void setup();
  void main();

 public:
  explicit thread_context(std::function<void()> func,
                          Executor* executor,
                          CoroSpecStorage *parent = nullptr);
  ~thread_context();
  void switch_in();
  void switch_out();
  friend bool morestack_triggered();
  friend uint32_t stack_segment_num();
  friend void thread_impl::switch_in(thread_context*);
  friend void thread_impl::switch_out(thread_context*);
  friend struct jmp_buf_link;
};

inline CoroSpecStorage *get_parent_css() {
  if (thread_impl::can_switch_out()) {
    return &(g_current_context->thread->storage_);
  }
  return static_cast<CoroSpecStorage *>(nullptr);
}

} // namespace internal
} // namespace uthread
} // namespace fl
