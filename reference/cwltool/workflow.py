import yaml
import os
import networkx as nx

from tool_new import jseval, get_proc_args_and_redirects


def listify_properties(obj):
    props = 'inputs', 'outputs', 'links', 'baseCmd', 'inputBindings', 'schemaDefs', 'steps'
    if isinstance(obj, list):
        for val in obj:
            listify_properties(val)
    elif isinstance(obj, dict):
        for key, val in obj.iteritems():
            if key in props and not isinstance(val, list):
                obj[key] = [val]
            listify_properties(val)


def load_url(url, parent=None):
    if parent:
        url = os.path.join(os.path.dirname(parent), url)
    with open(url) as fp:
        doc = yaml.load(fp)
    if not isinstance(doc, dict):
        raise Exception('Document must be an object.')
    listify_properties(doc)
    cls = doc.get('class')
    if cls not in ('CommandLineTool', 'ExpressionTool', 'Workflow'):
        raise Exception('Unknown type: %s' % cls)
    return {
        'CommandLineTool': CLTool,
        'ExpressionTool': ExpressionTool,
        'Workflow': Workflow,
    }[cls](doc, url)


class CLTool(object):
    def __init__(self, d, url):
        self.d = d
        self.url = url

    def run(self, inputs):
        job = {
            'inputs': inputs,
            'allocatedResources': {
                'cpu': 1,
                'mem': 2048,
            },
        }
        argv, stdin, stdout = get_proc_args_and_redirects(self.d, job)
        return {}


class ExpressionTool(object):
    def __init__(self, d, url):
        self.d = d
        self.url = url

    def run(self, inputs):
        result = jseval({'inputs': inputs}, self.d['expression']['value'])
        return result


class Workflow(object):
    def __init__(self, d, url):
        self.d = d
        self.url = url
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
        assert nx.is_directed_acyclic_graph(g), 'Cycles found; aborting.'

    def set_inputs(self, inputs):
        inputs = inputs or {}
        for k, v in inputs.iteritems():
            self.g.node['#'+k]['val'] = v

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

    def run(self, inputs):
        self.set_inputs(inputs)
        while True:
            node, data = self.next()
            if not node:
                return self._make_outputs()
            self.execute(node)

    def _make_outputs(self):
        for out in self.result.keys():
            self.result[out] = self.g.node['#'+out]['result']
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
    assert load_url(path).run(inputs) == outputs


if __name__ == '__main__':
    EX = os.path.join(os.path.dirname(__file__), '../../examples/')
    test(EX + 'simple/wf-square-sum.json', {'arr': [1, 2]}, {'square_sum': 9})