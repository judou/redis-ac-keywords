import redis

class RedisACKeywords(object):
    '''
    (1) Efficient String Matching: An Aid to Bibliographic Search
    (2) Construction of Aho Corasick Automaton in Linear Time for Integer Alphabets
    '''
    # %s is name
    KEYWORD_KEY=u'%s:keyword'
    PREFIX_KEY=u'%s:prefix'
    SUFFIX_KEY=u'%s:suffix'

    # %s is keyword
    OUTPIUT_KEY=u'%s:output'
    NODE_KEY=u'%s:node'

    def __init__(self, host='localhost', port=6379, db=12, name='RedisACKeywords', encoding='utf8'):
        '''
        db: 7+5 because 1975
        '''
        self.encoding = encoding

        self.client = redis.Redis(host=host, port=port, db=db)
        self.client.ping()

        self.name = self.smart_unicode(name)

        # Init trie root
        self.client.zadd(self.PREFIX_KEY % self.name, u'', 1.0)


    def add(self, keyword):
        keyword = keyword.strip().lower()
        assert keyword
        keyword = self.smart_unicode(keyword)

        # Add keyword in keyword set
        self.client.sadd(self.KEYWORD_KEY % self.name, keyword)

        self._build_trie(keyword)

        num = self.client.scard(self.KEYWORD_KEY % self.name)
        return num

    def remove(self, keyword):
        assert keyword
        keyword = keyword.strip().lower()
        keyword = self.smart_unicode(keyword)

        self._remove(keyword)

        self.client.srem(self.KEYWORD_KEY % self.name, keyword)
        num = self.client.scard(self.KEYWORD_KEY % self.name)
        return num

    def find(self, text):
        ret = []
        i = 0
        state = u''
        utext = self.smart_unicode(text)
        while i < len(utext):
            c = utext[i]
            next_state = self._go(state, c)
            if next_state is None:
                next_state = self._fail(state)
                _next_state = self._go(next_state, c)
                if _next_state is None:
                    _next_state = self._fail(next_state + c)
                next_state = _next_state

            pos = i - 1
            outputs = self._output(state)
            ret.extend(outputs)

            state = next_state
            i += 1

        # check last state
        pos = i - 1
        outputs = self._output(state)
        ret.extend(outputs)
        return ret

    def flush(self):
        keywords = self.client.smembers(self.KEYWORD_KEY % self.name)
        for keyword in keywords:
            self.client.delete(self.OUTPIUT_KEY % self.smart_unicode(keyword))
            self.client.delete(self.NODE_KEY % self.smart_unicode(keyword))
        self.client.delete(self.PREFIX_KEY % self.name)
        self.client.delete(self.SUFFIX_KEY % self.name)
        self.client.delete(self.KEYWORD_KEY % self.name)

    def info(self):
        return {
            'keywords':self.client.scard(self.KEYWORD_KEY % self.name),
            'nodes':self.client.zcard(self.PREFIX_KEY % self.name),
        }

    def suggest(self, input):
        input = self.smart_unicode(input)
        ret = []
        rank = self.client.zrank(self.PREFIX_KEY % self.name, input)
        a = self.client.zrange(self.PREFIX_KEY % self.name, rank, rank)
        while a:
            node = self.smart_unicode(a[0])
            if node.startswith(input) and self.client.sismember(self.KEYWORD_KEY % self.name, node):
                ret.append(node)
            rank += 1
            a = self.client.zrange(self.PREFIX_KEY % self.name, rank, rank)
        return ret

    def _go(self, state, c):
        assert type(state) is unicode
        next_state = state + c
        i = self.client.zscore(self.PREFIX_KEY % self.name, next_state)
        if i is None:
            return None
        return next_state

    def _build_trie(self, keyword):
        assert type(keyword) is unicode
        l = len(keyword)
        for i in xrange(l): # trie depth increase
            prefix = keyword[:i+1] # every prefix is a node
            _suffix = u''.join(reversed(prefix))
            if self.client.zscore(self.PREFIX_KEY % self.name, prefix) is None: # node does not exist
                self.client.zadd(self.PREFIX_KEY % self.name, prefix, 1.0)
                self.client.zadd(self.SUFFIX_KEY % self.name, _suffix, 1.0) # reversed suffix node

                self._rebuild_output(_suffix)
            else:
                if (self.client.sismember(self.KEYWORD_KEY % self.name, prefix)): # node may change, rebuild affected nodes
                    self._rebuild_output(_suffix)

    def smart_unicode(self, s):
        ret = s
        if type(s) is str:
            ret = s.decode(self.encoding)
        return ret

    def smart_str(self, s):
        ret = s
        if type(s) is unicode:
            ret = s.encode(self.encoding)
        return ret

    def _rebuild_output(self, _suffix):
        assert type(_suffix) is unicode
        rank = self.client.zrank(self.SUFFIX_KEY % self.name, _suffix)
        a = self.client.zrange(self.SUFFIX_KEY % self.name, rank, rank)
        while a:
            suffix_ = self.smart_unicode(a[0])
            if suffix_.startswith(_suffix):
                state = u''.join(reversed(suffix_))
                self._build_output(state)
            else:
                break
            rank += 1
            a = self.client.zrange(self.SUFFIX_KEY % self.name, rank, rank)

    def _build_output(self, state):
        assert type(state) is unicode
        outputs = []
        if self.client.sismember(self.KEYWORD_KEY % self.name, state):
            outputs.append(state)
        fail_state = self._fail(state)
        fail_output = self._output(fail_state)
        if fail_output:
            outputs.extend(fail_output)
        if outputs:
            self.client.sadd(self.OUTPIUT_KEY % state, *outputs)
            for k in outputs:
                self.client.sadd(self.NODE_KEY % k, state) # ref node for delete keywords in output

    def _fail(self, state):
        assert type(state) is unicode
        # max suffix node will be the failed node
        next_state = u''
        for i in xrange(1, len(state)+1): # depth increase
            next_state = state[i:]
            if self.client.zscore(self.PREFIX_KEY % self.name, next_state):
                break
        return next_state

    def _output(self, state):
        assert type(state) is unicode
        return [self.smart_unicode(k) for k in self.client.smembers(self.OUTPIUT_KEY % state)]

    def debug_print(self):
        keywords = self.client.smembers(self.KEYWORD_KEY % self.name)
        if keywords:
            print '-',  self.KEYWORD_KEY % self.name, u' '.join(keywords)
        prefix = self.client.zrange(self.PREFIX_KEY % self.name, 0, -1)
        if prefix:
            prefix[0] = u'.'
            print '-',  self.PREFIX_KEY % self.name, u' '.join(prefix)
        suffix = self.client.zrange(self.SUFFIX_KEY % self.name, 0, -1)
        if suffix:
            print '-',  self.SUFFIX_KEY % self.name, u' '.join(suffix)

        outputs = []
        for node in prefix:
            output = self._output(self.smart_unicode(node))
            outputs.append(output)
        if outputs:
            print '-',  'outputs', outputs

        nodes = []
        for keyword in keywords:
            keyword_nodes = self.client.smembers(self.NODE_KEY % self.smart_unicode(keyword))
            nodes.append(keyword_nodes)
        if nodes:
            print '-', 'nodes', nodes

    def _remove(self, keyword):
        assert type(keyword) is unicode
        nodes = self.client.smembers(self.NODE_KEY % keyword)
        for node in nodes:
            self.client.srem(self.OUTPIUT_KEY % self.smart_unicode(node), keyword)
        self.client.delete(self.NODE_KEY % keyword)

        # remove nodes if need
        l = len(keyword)
        for i in xrange(l, 0, -1): # depth decrease
            prefix = keyword[:i]
            if self.client.sismember(self.KEYWORD_KEY % self.name, prefix) and i!=l:
                break
            _suffix = u''.join(reversed(prefix))

            rank = self.client.zrank(self.PREFIX_KEY % self.name, prefix)
            if rank is None:
                break
            a = self.client.zrange(self.PREFIX_KEY % self.name, rank+1, rank+1)
            if a:
                prefix_ = self.smart_unicode(a[0])
                if not prefix_.startswith(prefix):
                    self.client.zrem(self.PREFIX_KEY % self.name, prefix)
                    self.client.zrem(self.SUFFIX_KEY % self.name, _suffix)
                else:
                    break
            else:
                self.client.zrem(self.PREFIX_KEY % self.name, prefix)
                self.client.zrem(self.SUFFIX_KEY % self.name, _suffix)

if __name__ == '__main__':
    keywords = RedisACKeywords(name='test')

    ks = ['her', 'he', 'his']
    for k in ks:
        keywords.add(k)

    input = 'he'
    print 'suggest %s: ' % input, keywords.suggest(input) # her, he

    text = 'ushers'
    print 'text: %s' % text
    print 'keywords: %s added. ' % ' '.join(ks), keywords.find(text) # her, he

    ks2 = ['she', 'hers']
    for k in ks2:
        keywords.add(k)
    print 'keywords: %s added. ' % ' '.join(ks2), keywords.find(text) # her, he, she, hers

    keywords.add('h')
    print 'h added. ', keywords.find(text) # her, he, she, hers, h

    keywords.remove('h')
    print 'h removed. ', keywords.find(text) # her, he, she, hers

    keywords.flush()
    print 'flushed. ', keywords.find(text) # []
