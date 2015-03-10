import os
import tempfile
import glob
import logging
import json
from copy import deepcopy

import networkx as nx
import yaml

from tool_new import jseval, get_proc_args_and_redirects

log = logging.getLogger(__name__)


def listify_properties(obj):
    """
    Modify obj in place to create single-item lists
    for certain properties whose values can be either lists or single items.
    """
    props = 'inputs', 'outputs', 'links', 'baseCmd', 'arguments', 'schemaDefs', 'steps'
    if isinstance(obj, list):
        for val in obj:
            listify_properties(val)
    elif isinstance(obj, dict):
        for key, val in obj.iteritems():
            if key in props and not isinstance(val, list):
                obj[key] = [val]
            listify_properties(val)


def load_url(url, parent=None):
    """ Create appropriate class from url or path contents. """
    if parent:
        url = os.path.join(os.path.dirname(parent), url)
    with open(url) as fp:  # TODO: fetch actual URLs
        doc = yaml.load(fp)
    if not isinstance(doc, dict):
        raise TypeError('Document must be a JSON or YAML object.')
    listify_properties(doc)
    cls = doc.get('class')
    if cls not in ('CommandLineTool', 'ExpressionTool', 'Workflow'):
        raise ValueError('Unknown type: %s' % cls)
    return {
        'CommandLineTool': CLTool,
        'ExpressionTool': ExpressionTool,
        'Workflow': Workflow,
    }[cls](doc, url)


class BaseApp(object):
    """ Base class for CWL runnables. Handles implicit iteration. """
    def __init__(self, d, url):
        self.d = d
        self.url = url

    def run(self, inputs):
        def depth(o):
            if not isinstance(o, list) or not o:
                return 0
            return depth(o[0]) + 1
        inputs = inputs or {}
        expected_depths = {i['id'][1:]: i.get('depth', 0) for i in self.d.get('inputs', [])}
        actual_depths = {k: depth(v) for k, v in inputs.iteritems()}
        expected_depths = {k: v for k, v in expected_depths.iteritems() if k in actual_depths}
        if expected_depths == actual_depths:
            return self._run(inputs)

        mismatch = {k: (v, actual_depths[k]) for k, v in expected_depths.iteritems() if actual_depths[k] != v}
        log.debug('Depth mismatch: %s', mismatch)
        if len(mismatch) > 1:
            raise ValueError('Depth mismatch on more than one port.')
        expected, actual = mismatch.values()[0]
        if actual < expected:
            raise ValueError('Actual depth less than expected.')
        if actual - expected != 1:
            raise NotImplementedError('Currently only handling iteration on one level of nesting.')
        port = mismatch.keys()[0]
        results = []
        for item in inputs[port]:
            inps = deepcopy(inputs)
            inps[port] = item
            results.append(self._run(inps))
        outputs = reduce(set.union, [set(r.keys()) for r in results], set())
        return {k: [r.get(k) for r in results] for k in outputs}

    def _run(self, inputs):
        raise NotImplementedError()


class CLTool(BaseApp):
    def _run(self, inputs):
        job = {
            'inputs': inputs,
            'allocatedResources': {
                'cpu': 1,
                'mem': 2048,
            },
        }
        argv, stdin, stdout = get_proc_args_and_redirects(self.d, job)
        line = ' '.join(argv)
        if stdin:
            line += ' < ' + stdin
        if stdout:
            line += ' > ' + stdout
        log.debug('Cmd: %s', line)
        job_dir = tempfile.mkdtemp()
        os.chdir(job_dir)
        with open('job.cwl.json', 'w') as fp:
            json.dump(job, fp)
        if os.system(line):  # TODO: shell escape or use Popen
            raise RuntimeError('Process failed.')

        if os.path.isfile('result.cwl.json'):
            with open('result.cwl.json') as fp:
                return json.load(fp)
        result = {}
        for out in self.d.get('outputs', []):
            adapter = out.get('outputBinding')
            if adapter is None and isinstance(out.get('type'), dict):
                adapter = out['type'].get('outputBinding')
            if adapter is None or not adapter.get('glob'):
                continue
            matches = glob.glob(adapter['glob'])
            if out['type'] == 'File' or out['type']['type'] == 'File':
                result[out['id'][1:]] = {"@type": "File", "path": os.path.abspath(matches[0])}
                continue
            if out['type']['type'] == 'array':
                result[out['id'][1:]] = [{"@type": "File", "path": os.path.abspath(p)} for p in matches]
                continue
        log.debug('RESULT: %s', result)
        return result


class ExpressionTool(BaseApp):
    def _run(self, inputs):
        result = jseval({'inputs': inputs}, self.d['expression']['value'])
        log.debug('RESULT: %s', result)
        return result


class Workflow(BaseApp):
    def __init__(self, d, url):
        super(Workflow, self).__init__(d, url)
        self.g = g = nx.DiGraph()
        self.result = {}

        # Create nodes from ports and steps, edges from data links.
        for inp in d.get('inputs', []):
            g.add_node(inp['id'], type='port', depth=inp.get('depth', 0), val=inp.get('value'))
        for out in d.get('outputs', []):
            g.add_node(out['id'], type='port', depth=out.get('depth', 0))
            self.result[out['id'][1:]] = None
            for link in out.get('links', []):
                g.add_edge(link['source'], out['id'], pos=link.get('position', 0))
        for step in d['steps']:
            step_id = step['id']
            impl = load_url(step['impl'], parent=url)
            g.add_node(step_id, type='step', impl=impl)
            for inp in step.get('inputs', []):
                g.add_node(inp['id'], type='port', depth=inp.get('depth', 0), val=inp.get('value'))
                g.add_edge(inp['id'], step_id)
                for link in inp.get('links', []):
                    g.add_edge(link['source'], inp['id'], pos=link.get('position', 0))
            for out in step.get('outputs', []):
                g.add_node(out['id'], type='port', depth=out.get('depth', 0))
                g.add_edge(step_id, out['id'])
        assert nx.is_directed_acyclic_graph(g), 'Workflow contains a cycle.'

    def set_inputs(self, inputs):
        inputs = inputs or {}
        for k, v in inputs.iteritems():
            self.g.node['#' + k]['val'] = v

    def finish(self, node, result):
        self.g.node[node].update(status='done', result=result)

    def next(self):
        for node, data in self.g.nodes_iter(True):
            if 'status' in data:
                continue
            pre = [self.g.node[p] for p in self.g.predecessors_iter(node)]
            if all(p.get('status') == 'done' for p in pre):
                data['val'] = self._make_val(node, data)
                data['status'] = 'running'
                return node, data
        return None, None

    def execute(self, node):
        data = self.g.node[node]
        if data['type'] == 'port':
            return self.finish(node, data['val'])
        app = data['impl']
        return self.finish(node, app.run(data['val']))

    def _run(self, inputs):
        self.set_inputs(inputs)
        while True:
            node, data = self.next()
            if not node:
                result = self._make_outputs()
                log.debug('RESULT: %s', result)
                return result
            self.execute(node)

    def _make_outputs(self):
        for out in self.result.keys():
            self.result[out] = self.g.node['#' + out]['result']
        return self.result

    def _make_val(self, node, data):
        pre = self.g.predecessors(node)
        if not pre:
            return data.get('val')
        if data['type'] == 'port' and len(pre) == 1:
            p = self.g.node[pre[0]]
            return p['result'] if p['type'] == 'port' else p['result'].get(node.split('/')[-1])
        if data['type'] == 'port' and len(pre) > 1:
            edges = self.g.in_edges(node, True)
            edges.sort(key=lambda edge: edge[2].get('pos', 0))
            return [self.g.node[e[0]]['result'] for e in edges]
        return {p.split('/')[-1]: self.g.node[p]['result'] for p in pre}


def test(path, inputs, outputs):
    """ Assert that running the thing on <path> with <inputs> produces <outputs>. """
    def path_to_name(o):
        """ Convert File objects to just their name (string). """
        if isinstance(o, list):
            return [path_to_name(i) for i in o]
        if isinstance(o, dict):
            if o.get('@type') == 'File':
                return os.path.basename(o['path'])
            else:
                return {k: path_to_name(v) for k, v in o.iteritems()}
        return o

    result = load_url(path).run(inputs)
    log.info('-- Complete --\n %s', result)
    assert path_to_name(result) == outputs, 'expected %s' % outputs


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    EX = os.path.join(os.path.dirname(__file__), '../../examples/')
    # test(EX + 'simple/wf-square-sum.json', {'arr': [1, 2]}, {'square_sum': 9})
    # test(EX + 'simple/wf-nested-simple.json', {'arr': [1, 2]}, {'square_sum_times_two': 18})
    # test(EX + 'cat4-tool.json', {
    #     'file1': {"@type": "File", "path": EX + 'hello.txt'}
    # }, {
    #     'output': "output.txt"
    # })
    test(EX + 'wf-count-lines.json', {
        'files': [{"@type": "File", "path": EX + 'lines1.txt'}, {"@type": "File", "path": EX + 'lines2.txt'}],
        'pattern': 'find_me',
    }, {'result': 3})
