import redis

from redis_ac_keywords import RedisACKeywords

def print_used_memory(client):
    for k, v in client.info().items():
        if k.find('memory') != -1:
            print k, v

if __name__ == '__main__':
    '''
    used_memory_peak_human 1.05M
    used_memory 1055552
    used_memory_lua 31744
    used_memory_rss 1769472
    used_memory_human 1.01M
    used_memory_peak 1105472
    20828
    used_memory_peak_human 35.54M
    used_memory 37303440
    used_memory_lua 31744
    used_memory_rss 39735296
    used_memory_human 35.58M
    used_memory_peak 37268368
    '''
    client = redis.Redis(db=12)
    print_used_memory(client)

    f = open('/Users/ant/projects/haokanbu/code/haokanbu/walle/keywords.txt')
    i = 0

    keywords = RedisACKeywords(name='benchmark')
    for line in f.readlines():
        keyword = line.strip()
        keywords.add(keyword)
        i += 1
    print i
    print_used_memory(client)
