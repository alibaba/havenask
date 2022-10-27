 # Python
from __future__ import print_function
import tensorflow as tf
from tensorflow.python.ops import control_flow_ops

ways = [1, 2, 4, 8, 16, 32]

def body(eof, seek_iterator, aggregator):
    output_iterator, matchdocs, seek_eof = turing_agg_module.seek_op(
        seek_iterator, batch_size = 10240, name = "seek_docs")
    output_aggregator, output_matchdocs = turing_agg_module.aggregator_op(
        aggregator, matchdocs, name = "agg_docs")
    eof_seek = turing_agg_module.match_doc_release_op(seek_eof, output_matchdocs, name = "release_docs")
    return eof_seek, output_iterator, output_aggregator


def condition(eof_seek, seek_iterator, aggregator):
    return tf.equal(False, eof_seek, name ="cond_equal")


for way_count in ways:
    g = tf.Graph()
    with g.as_default():
        turing_agg_module = tf.load_op_library('/ha3_develop/source_code/ha3/build/release64/lib/libha3_suez_turing_agg_opdef.so')
        input_request = tf.placeholder(tf.variant, name='ha3_request')
        request = turing_agg_module.request_init_op(input_request, name = 'init_request')
        layer_metas = turing_agg_module.layer_metas_create_op(request, name = 'create_layer_metas')

        split_layer_metas = turing_agg_module.range_split_op(layer_metas, N = way_count,
                                                             name = 'split_ranges')
        aggres = []
        for layer_metas in split_layer_metas:
            expression_resource = turing_agg_module.prepare_expression_resource_op(
                request, name = "prepare_expression_resource")
            seek_iterator = turing_agg_module.seek_iterator_prepare_op(
                layer_metas, expression_resource, name = "prepare_seek_iterator")
            with tf.control_dependencies([seek_iterator]):
                aggregator = turing_agg_module.agg_prepare_op(expression_resource,
                                                              name = "agg_prepare")
            eof = tf.constant(False)
            #eof = tf.Variable(False, name="seek_end")
            ret_vals = control_flow_ops.while_loop(condition, body, [eof, seek_iterator, aggregator],
                                                   shape_invariants = [tf.TensorShape(None),
                                                                       tf.TensorShape(None),
                                                                       tf.TensorShape(None)],
                                                   parallel_iterations = 1)
            aggres.append(ret_vals[2])

        aggresult = turing_agg_module.agg_merge_op(aggres)
        ha3result = turing_agg_module.agg_result_construct_op(aggresult, name = "ha3_result")
        with tf.control_dependencies([ha3result]):
            node6 = tf.no_op(name="ha3_search_done")

        g_def = g.as_graph_def()

        with open("./default_agg_%d_while.def" % way_count, 'w') as f:
            f.write(g_def.SerializeToString())
        with open("./default_agg_%d_while.pbtxt" % way_count, 'w') as f:
            f.write(str(g_def))
