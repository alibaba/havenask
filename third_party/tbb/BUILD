load('//bazel:defs.bzl', 'rpm_library')
hdrs = [
    'aggregator.h', 'aligned_space.h', 'atomic.h', 'blocked_range.h',
    'blocked_range2d.h', 'blocked_range3d.h', 'cache_aligned_allocator.h',
    'combinable.h', 'compat/condition_variable', 'compat/ppl.h',
    'compat/thread', 'compat/tuple', 'concurrent_hash_map.h',
    'concurrent_lru_cache.h', 'concurrent_priority_queue.h',
    'concurrent_queue.h', 'concurrent_unordered_map.h',
    'concurrent_unordered_set.h', 'concurrent_vector.h', 'critical_section.h',
    'enumerable_thread_specific.h', 'flow_graph.h',
    'internal/_aggregator_impl.h', 'internal/_concurrent_queue_impl.h',
    'internal/_concurrent_unordered_impl.h', 'internal/_flow_graph_impl.h',
    'internal/_flow_graph_item_buffer_impl.h',
    'internal/_flow_graph_join_impl.h', 'internal/_flow_graph_node_impl.h',
    'internal/_flow_graph_or_impl.h',
    'internal/_flow_graph_tagged_buffer_impl.h',
    'internal/_flow_graph_types_impl.h', 'internal/_tbb_windef.h',
    'machine/gcc_armv7.h', 'machine/gcc_generic.h', 'machine/gcc_ia32_common.h',
    'machine/ibm_aix51.h', 'machine/icc_generic.h', 'machine/linux_common.h',
    'machine/linux_ia32.h', 'machine/linux_ia64.h', 'machine/linux_intel64.h',
    'machine/mac_ppc.h', 'machine/macos_common.h', 'machine/mic_common.h',
    'machine/msvc_ia32_common.h', 'machine/sunos_sparc.h',
    'machine/windows_api.h', 'machine/windows_ia32.h',
    'machine/windows_intel64.h', 'machine/xbox360_ppc.h', 'memory_pool.h',
    'mutex.h', 'null_mutex.h', 'null_rw_mutex.h', 'parallel_do.h',
    'parallel_for.h', 'parallel_for_each.h', 'parallel_invoke.h',
    'parallel_reduce.h', 'parallel_scan.h', 'parallel_sort.h',
    'parallel_while.h', 'partitioner.h', 'pipeline.h', 'queuing_mutex.h',
    'queuing_rw_mutex.h', 'reader_writer_lock.h', 'recursive_mutex.h',
    'runtime_loader.h', 'scalable_allocator.h', 'spin_mutex.h',
    'spin_rw_mutex.h', 'task.h', 'task_arena.h', 'task_group.h',
    'task_scheduler_init.h', 'task_scheduler_observer.h', 'tbb.h',
    'tbb_allocator.h', 'tbb_config.h', 'tbb_exception.h', 'tbb_machine.h',
    'tbb_profiling.h', 'tbb_stddef.h', 'tbb_thread.h', 'tbbmalloc_proxy.h',
    'tick_count.h'
]
static_lib = 'libtbb.a'
shared_lib = 'libtbb.so'
rpm_library(
    name='tbb',
    rpms=['@tbb-package//file:file', '@tbb-devel-package//file:file'],
    hdrs=hdrs,
    shared_lib=shared_lib,
    include_path='usr/include/tbb',
    lib_path='usr/lib64',
    include_prefix='tbb'
)
