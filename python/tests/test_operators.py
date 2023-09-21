# SPDX-FileCopyrightText: Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import asyncio

import pytest

import mrc
import mrc.core.executor
from mrc.core import operators as ops


@pytest.fixture
def ex_runner():

    def run_exec(segment_init):
        pipeline = mrc.Pipeline()

        pipeline.make_segment("my_seg", segment_init)

        options = mrc.Options()

        # Set to 1 thread
        options.topology.user_cpuset = "0-0"

        executor = mrc.Executor(options)

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

        def segment_fn(seg: mrc.Builder):
            source = seg.make_source("source", producer(input_data))

            node = seg.make_node("test", ops.build(node_fn))

            seg.make_edge(source, node)

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

            node.launch_options.engines_per_pe = 10

        ex_runner(segment_fn)

        assert did_complete, "Sink on_completed was not called"

        return actual, raised_error

    return run


def producer(to_produce):

    for x in to_produce:
        yield x


def test_build(run_segment):

    input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    expected = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]

    def node_fn(input: mrc.Observable, output: mrc.Subscriber):

        input.subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_map(run_segment):

    input_data = list(range(0, 10))
    expected = [x + 1 for x in input_data]
    actual = []

    def node_fn(input: mrc.Observable, output: mrc.Subscriber):

        input.pipe(ops.map(lambda x: x + 1)).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_map_async(run_segment):

    input_data = list(range(0, 10))
    expected = [x + 1 for x in input_data]
    actual = []

    async def async_map_fn(x):

        print(f"In async_map_fn {x}. Sleeping")

        await mrc.core.executor.sleep(1000)

        # try:
        #     asyncio.get_running_loop()
        # except RuntimeError:
        #     print("No running loop. Making one")

        #     loop = asyncio.new_event_loop()

        #     asyncio.set_event_loop(loop)

        # await asyncio.sleep(10)

        print(f"Done sleeping {x}. Returning")

        return x + 1

    def node_fn(input: mrc.Observable, output: mrc.Subscriber):

        input.pipe(ops.map_async(async_map_fn)).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_flatten(run_segment):

    input_data = [[1, 2, 3, 4, 5], ["one", "two", "three", "four", "five"], [1, "two", 3]]
    expected = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]

    def node_fn(input: mrc.Observable, output: mrc.Subscriber):

        input.pipe(ops.flatten()).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_filter(run_segment):

    input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    expected = [3, 4, 5, 3]

    def node_fn(input: mrc.Observable, output: mrc.Subscriber):

        input.pipe(ops.filter(lambda x: isinstance(x, int) and x >= 3)).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_on_complete(run_segment):

    input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    expected = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3, "after_completed"]

    def node_fn(input: mrc.Observable, output: mrc.Subscriber):

        input.pipe(ops.on_completed(lambda: "after_completed")).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_on_complete_none(run_segment):

    input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    expected = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    on_completed_hit = False

    def node_fn(input: mrc.Observable, output: mrc.Subscriber):

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

    def node_fn(input: mrc.Observable, output: mrc.Subscriber):

        input.pipe(ops.pairwise()).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_to_list(run_segment):

    input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    expected = [[1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]]

    def node_fn(input: mrc.Observable, output: mrc.Subscriber):

        input.pipe(ops.to_list()).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_to_list_empty(run_segment):

    input_data = []
    expected = []

    def node_fn(input: mrc.Observable, output: mrc.Subscriber):

        input.pipe(ops.to_list()).subscribe(output)

    actual, raised_error = run_segment(input_data, node_fn)

    assert actual == expected


def test_combination(run_segment):

    input_data = [1, 2, 3, 4, 5, "one", "two", "three", "four", "five", 1, "two", 3]
    expected = [5, 5, 6, 6, 7, 7, 5, 5, 1, 2, "one", "two", "three", "four", "five", 1, "two"]

    def node_fn(input: mrc.Observable, output: mrc.Subscriber):

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
