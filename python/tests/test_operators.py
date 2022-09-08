# SPDX-FileCopyrightText: Copyright (c) 2022 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import typing

import pytest

import srf
from srf.core import operators as ops


@pytest.fixture
def ex_runner():

    def run_exec(segment_init):
        pipeline = srf.Pipeline()

        pipeline.make_segment("my_seg", segment_init)

        options = srf.Options()

        # Set to 1 thread
        options.topology.user_cpuset = "0-0"

        executor = srf.Executor(options)

        executor.register_pipeline(pipeline)

        executor.start()

        executor.join()

    return run_exec


@pytest.fixture
def run_segment(ex_runner):

    def run(input_data, node_fn):

        actual = []
        raised_error = None
        did_complete = False

        def segment_fn(seg: srf.Builder):
            source = seg.make_source("source", producer(input_data))

            node = seg.make_node_full("test", node_fn)
            seg.make_edge(source, node)

            node.launch_options.pe_count = 1

            def sink_on_next(x):
                actual.append(x)

            def sink_on_error(x):
                nonlocal raised_error
                raised_error = x

            def sink_on_completed():
                nonlocal did_complete
                did_complete = True

            sink = seg.make_sink("sink", sink_on_next, sink_on_error, sink_on_completed)
            seg.make_edge(node, sink)

        ex_runner(segment_fn)

        assert did_complete, "Sink on_completed was not called"

        return actual, raised_error

    return run


def producer(to_produce):

    for x in to_produce:
        yield x


def test_map(run_segment):

    input_data = [0, 1, 2, 3, 4]
    expected = [1, 2, 3, 4, 5]
    actual = []

    def node_fn(input: srf.Observable, output: srf.Subscriber):

        input.pipe(ops.map(lambda x: x + 1)).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_flat_map(run_segment):

    input_data = [0, 1, 2, 3, 4]
    expected = [0, 1, 2, 1, 2, 3, 2, 3, 4, 3, 4, 5, 4, 5, 6]
    actual = []

    def node_fn(input: srf.Observable, output: srf.Subscriber):

        def flat_map_fn(x):
            arr = [x, x + 1, x + 2]

            return srf.Observable.iterate(arr)

        input.pipe(ops.flat_map(flat_map_fn)).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_flatten(run_segment):

    input_data = [[1, 2, 3, 4, 5], ["one", "two", "three", "four", "five"], [1, "two", 3]]
    expected = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]

    def node_fn(input: srf.Observable, output: srf.Subscriber):

        input.pipe(ops.flatten()).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_filter(run_segment):

    input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    expected = [3, 4, 5, 3]

    def node_fn(input: srf.Observable, output: srf.Subscriber):

        input.pipe(ops.filter(lambda x: isinstance(x, int) and x >= 3)).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_on_complete(run_segment):

    input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    expected = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3, "after_completed"]

    def node_fn(input: srf.Observable, output: srf.Subscriber):

        input.pipe(ops.on_completed(lambda: "after_completed")).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_on_complete_none(run_segment):

    input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    expected = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    on_completed_hit = False

    def node_fn(input: srf.Observable, output: srf.Subscriber):

        def on_completed_fn():
            nonlocal on_completed_hit
            on_completed_hit = True
            # Do not return anything

        input.pipe(ops.on_completed(on_completed_fn)).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected
    assert on_completed_hit, "Did not hit on_complete_fn"


def test_pairwise(run_segment):

    input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    expected = [(1, 2), (2, 3), (3, 4), (4, 5), (5, "one"), ("one", "two"), ("two", "three"), ("three", "four"),
                ("four", "five"), ("five", 1), (1, "two"), ("two", 3)]

    def node_fn(input: srf.Observable, output: srf.Subscriber):

        input.pipe(ops.pairwise()).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_to_list(run_segment):

    input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    expected = [[1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]]

    def node_fn(input: srf.Observable, output: srf.Subscriber):

        input.pipe(ops.to_list()).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_to_list_empty(run_segment):

    input_data = []
    expected = []

    def node_fn(input: srf.Observable, output: srf.Subscriber):

        input.pipe(ops.to_list()).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_map_async(run_segment):

    input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    expected = [4, 6, 8, 10, 12, "oneone", "twotwo", "threethree", "fourfour", "fivefive", 4, "twotwo", 8]

    # make a dask cluster
    from dask.distributed import Client
    from dask.distributed import Future

    client = Client()

    def node_fn(input: srf.Observable, output: srf.Subscriber):

        def double(x):
            return x + x

        def map_fn(x):

            f = client.submit(double, x)

            return f

        def after_map_fn(x):

            if (isinstance(x, int)):
                return x + 2

            return x

        input.pipe(ops.map_async(map_fn), ops.map(after_map_fn)).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


# def test_map_async_srf_future(run_segment):

#     input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
#     expected = [4, 6, 8, 10, 12, "oneone", "twotwo", "threethree", "fourfour", "fivefive", 4, "twotwo", 8]

#     from concurrent.futures import ThreadPoolExecutor

#     executor = ThreadPoolExecutor(max_workers=10)

#     def work_fn(x, output_f: srf.Future):

#         time.sleep(2.0)

#         output_f.set_result(x + x)

#     def node_fn(input: srf.Observable, output: srf.Subscriber):

#         def map_fn(x):

#             f = srf.Future()

#             executor.submit(work_fn, x, output_f=f)

#             return f

#         def after_map_fn(x):

#             if (isinstance(x, int)):
#                 return x + 2

#             return x

#         input.pipe(ops.map_async(map_fn), ops.map(after_map_fn)).subscribe(output)

#     actual, raised_error = run_segment(input_data, node_fn)

#     assert actual == expected


# @pytest.mark.parametrize("input_data", ([1, 2, 3, 4], [1, "two", ]), ids=("list", ))
@pytest.mark.parametrize(
    "input_data",
    [
        pytest.param([1, 2, 3, 4], id="list_int"),
        pytest.param(["one", "two", "three"], id="list_str"),
        pytest.param([1, "two", 3, "four"], id="list_mixed"),
        pytest.param(range(50), id="range"),
        pytest.param({
            "left": 1, "right": 2
        }, id="dict"),
        pytest.param({
            "num": [4, 5], "str": ["one", "two"], "comb": ["three", 4]
        }, id="dict_with_list"),
        pytest.param((1, "two", 3, "four"), id="tuple"),
        pytest.param((1, ["two"], {
            3: 3
        }, ("four", )), id="tuple_mixed"),
    ])
def test_map_async_with_iterables(run_segment, input_data):

    # Utility class to apply function to all nested elements
    def pack_data(wrapper, func):
        if (isinstance(wrapper, dict)):
            return {k: pack_data(v, func) for k, v, in wrapper.items()}
        elif (isinstance(wrapper, (tuple, list, set, frozenset)) and not isinstance(wrapper, str)):
            # Handle the case where we can create concrete classes
            return type(wrapper)([pack_data(v, func) for v in wrapper])
        elif (isinstance(wrapper, typing.Iterable) and not isinstance(wrapper, str)):
            # Return a list for all other types (like `range(3)`)
            return list([pack_data(v, func) for v in wrapper])
        else:
            return func(wrapper)

    def double(x):
        return x + x

    def after_map_fn(x):

        def apply_fn(y):
            if (isinstance(y, int)):
                return y + 2

        return pack_data(x, apply_fn)

    # Wrap it in a list and repeat it
    input_data = [input_data] * 10

    expected = pack_data(input_data, lambda x: after_map_fn(pack_data(x, double)))

    # make a dask cluster
    from dask.distributed import Client

    client = Client()

    def node_fn(input: srf.Observable, output: srf.Subscriber):

        def map_fn(x):

            return pack_data(x, lambda y: client.submit(double, y))

        input.pipe(ops.map_async(map_fn), ops.map(after_map_fn)).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


@pytest.mark.parametrize(
    "input_data",
    [
        pytest.param([1, 2, 3, 4], id="list_int"),
        pytest.param(["one", "two", "three"], id="list_str"),
        pytest.param([1, "two", 3, "four"], id="list_mixed"),
        pytest.param(range(50), id="range"),
        pytest.param({
            "left": 1, "right": 2
        }, id="dict"),
        pytest.param({
            "num": [4, 5], "str": ["one", "two"], "comb": ["three", 4]
        }, id="dict_with_list"),
        pytest.param((1, "two", 3, "four"), id="tuple"),
        pytest.param((1, ["two"], {
            3: 3
        }, ("four", )), id="tuple_mixed"),
    ])
def test_map_async_nested(run_segment, input_data):

    # Utility class to apply function to all nested elements
    def pack_data(wrapper, func):
        if (isinstance(wrapper, dict)):
            return {k: pack_data(v, func) for k, v, in wrapper.items()}
        elif (isinstance(wrapper, (tuple, list, set, frozenset)) and not isinstance(wrapper, str)):
            # Handle the case where we can create concrete classes
            return type(wrapper)([pack_data(v, func) for v in wrapper])
        elif (isinstance(wrapper, typing.Iterable) and not isinstance(wrapper, str)):
            # Return a list for all other types (like `range(3)`)
            return list([pack_data(v, func) for v in wrapper])
        else:
            return func(wrapper)

    def double_fn(x):
        return x + x

    def add_fn(x):
        if (isinstance(x, int)):
            return x + 2

        return x

    # Wrap it in a list and repeat it
    input_data = [input_data] * 10

    expected = pack_data(input_data, lambda x: add_fn(double_fn(x)))

    # make a dask cluster
    from dask.distributed import Client

    client = Client()

    def node_fn(input: srf.Observable, output: srf.Subscriber):

        def map_double_fn(x):

            return pack_data(x, lambda y: client.submit(double_fn, y))

        def map_add_fn(x):

            return pack_data(x, lambda y: client.submit(add_fn, y))

        # Do a normal map to return the future then async second
        input.pipe(ops.map(map_double_fn), ops.map_async(map_add_fn)).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_combination(run_segment):

    input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    expected = [5, 5, 6, 6, 7, 7, 5, 5, 1, 2, "one", "two", "three", "four", "five", 1, "two"]

    def node_fn(input: srf.Observable, output: srf.Subscriber):

        filtered_out = []

        def map_fn(x):
            if (isinstance(x, int) and x >= 3):
                return [x + 2] * 2
            else:
                filtered_out.append(x)
                return []

        def on_completed_fn():
            return filtered_out

        input.pipe(ops.map(map_fn),
                   ops.filter(lambda x: len(x) > 0),
                   ops.flatten(),
                   ops.to_list(),
                   ops.on_completed(on_completed_fn),
                   ops.flatten()).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


if (__name__ == "__main__"):
    pytest.main(['-s', 'tests/test_operators.py::test_filter_error'])
