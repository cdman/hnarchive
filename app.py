import datetime
import logging
import re
import webapp2

from google.appengine.ext import ndb
from google.appengine.api import urlfetch

import bs4


class Node(ndb.Model):
    parent = ndb.KeyProperty('Node', indexed=False)
    title = ndb.StringProperty(indexed=False)
    url = ndb.TextProperty(indexed=False)
    user = ndb.StringProperty(indexed=False)
    body = ndb.TextProperty(indexed=False)
    score = ndb.IntegerProperty(indexed=False)
    comment_count = ndb.IntegerProperty(indexed=False)
    added_at = ndb.DateTimeProperty(indexed=False)
    retrieved_at = ndb.DateTimeProperty(auto_now_add=True, indexed=False)


class PendingNode(ndb.Model):
    added_at = ndb.DateTimeProperty(auto_now_add=True, indexed=True)


class MinMax(ndb.Model):
    INSTANCE_KEY = ndb.Key('MinMax', 1)

    low_bound = ndb.IntegerProperty(default=0, indexed=False)
    upper_bound = ndb.IntegerProperty(default=1, indexed=False)
    processed_nodes = ndb.IntegerProperty(default=0, indexed=False)


class Webpage(ndb.Model):
    url = ndb.StringProperty(indexed=False)
    fetched_at = ndb.DateTimeProperty(auto_now_add=True, indexed=False)
    html = ndb.BlobProperty(indexed=False, compressed=True)


def get(url):
    assert url.startswith('https://news.ycombinator.com/')
    result = urlfetch.fetch(url=url,
        headers={'User-Agent': 'HNArchive - dify.ltd@gmail.com / https://github.com/cdman/hnarchive'})
    logging.info('Retrieved %s', url)
    assert result.status_code == 200
    assert 'Hacker News' in result.content
    ndb.non_transactional(Webpage(url=url, html=result.content).put)()
    return bs4.BeautifulSoup(result.content, 'lxml')


@ndb.non_transactional(allow_existing=True)
def skipExisting(ids):
    nodes = ndb.get_multi([ndb.Key(Node, i) for i in ids if i > 0])
    keys = set([0] + [n.key.id() for n in nodes if n])
    return [i for i in ids if not i in keys]


def extractUniqueIds(page):
    return set([
        long(re.sub(r'.*?(\d+)', r'\1', link['href']))
        for link in page.find_all(href=re.compile(r'item\?id=\d+'))
    ])


@ndb.transactional(xg=True, propagation=ndb.TransactionOptions.INDEPENDENT)
def fetchListing(url):
    listing = get(url)
    minMax = MinMax.INSTANCE_KEY.get()
    if not minMax:
        minMax = MinMax(key=MinMax.INSTANCE_KEY)
    ids = extractUniqueIds(listing)
    new_ids = skipExisting(ids)
    if not new_ids:
        logging.info('No new ids found')
        return
    if max(new_ids) > minMax.upper_bound:
        minMax.upper_bound = max(new_ids)
        minMax.put()
        logging.info('New upper bound: %d', max(new_ids))
    ndb.non_transactional(ndb.put_multi)([
        PendingNode(key=ndb.Key(PendingNode, i)) for i in new_ids])
    logging.info('Discovered new nodes: %s', new_ids)


def fetchFrontPage():
    fetchListing('https://news.ycombinator.com/')


def fetchNewest():
    fetchListing('https://news.ycombinator.com/newest')


@ndb.transactional(xg=True, propagation=ndb.TransactionOptions.INDEPENDENT)
def fetchMin():
    minMax = MinMax.INSTANCE_KEY.get()
    if not minMax:
        minMax = MinMax(key=MinMax.INSTANCE_KEY)
    while True:
        minMax.low_bound += 1
        if minMax.low_bound >= minMax.upper_bound:
            return
        if ndb.non_transactional(ndb.Key(Node, minMax.low_bound).get)() is None:
            break
    ndb.put_multi([minMax, PendingNode(key=ndb.Key(PendingNode, minMax.low_bound))])


def extractMatch(text, pattern):
    match = re.search(pattern, text)
    if match is None: return
    return match.group(1)


def populateFromMeta(node, meta, parent_id):
    meta_text = meta.text
    node.user = meta.find(href=re.compile(r'^user\?id=.+'))['href'].replace('user?id=', '')
    node.key = ndb.Key(Node, long(
        meta.find(href=re.compile(r'^item\?id=.+'))['href'].replace('item?id=', '')))
    if extractMatch(meta_text, r'(\d+) points?'):
        node.score = long(extractMatch(meta_text, r'(\d+) points?'))
    if extractMatch(meta_text, r'(\d+) (?:minute|hour|day)s? ago'):
        qty = long(extractMatch(meta_text, r'(\d+) (?:minute|hour|day)s? ago'))
        metric = extractMatch(meta_text, r'\d+ (minute|hour|day)s? ago')
        node.added_at = datetime.datetime.utcnow()
        if metric == 'minute':
            node.added_at -= datetime.timedelta(minutes=qty)
        elif metric == 'hour':
            node.added_at -= datetime.timedelta(hours=qty)
        elif metric == 'day':
            node.added_at -= datetime.timedelta(days=qty)
        else:
            assert False
    if extractMatch(meta_text, r'(\d+) comments?'):
        node.comment_count = long(extractMatch(meta_text, r'(\d+) comments?'))
    parent = meta.find('a', text='parent')
    if parent:
        node.parent = ndb.Key(Node, long(parent['href'].replace('item?id=', '')))
    else:
        node.parent = ndb.Key(Node, parent_id)


@ndb.non_transactional
def parseTable(t, parent_id):
    head = t.find('td', class_='title')
    ids = []
    if head is not None:
        node = Node()
        node.title = head.text
        node.url = head.find('a')['href']
        populateFromMeta(node, head.parent.parent.find_all('tr')[1], parent_id)

        text = ''.join([unicode(n) for n in head.parent.parent.find_all('tr')[2:] if n.text.strip()])
        text, _ = re.subn(r'</?t[dr]>', '', text)
        if text:
            node.body = text
        node.put()
        ids.append(node.key.id())
        logging.info('Saved %d', node.key.id())
    for comment in t.find_all('td', class_='default'):
        parent_table = comment
        while parent_table and parent_table.name != 'table':
            parent_table = parent_table.parent
        if parent_table and parent_table.find('a', text='link'):
            pparent_id = long(parent_table.find('a', text='link')['href'].replace('item?id=', ''))
        else:
            pparent_id = parent_id

        node = Node()
        populateFromMeta(node, comment.find('span', class_='comhead'), pparent_id)
        node.body = ''.join(
            [unicode(c).strip() for c in comment.find('span', class_='comment').contents])
        node.body = node.body.replace('<font color="#000000">', '').replace('</font>', '')
        node.put()
        ids.append(node.key.id())
        logging.info('Saved %d', node.key.id())
    return ids


@ndb.transactional(xg=True, propagation=ndb.TransactionOptions.INDEPENDENT)
def processsOneNode(pending_node):
    page = get('https://news.ycombinator.com/item?id=%d' % pending_node.id())
    ids = extractUniqueIds(page)

    node_count = 0
    for t in page.find_all('table'):
        try:
            table_ids = parseTable(t, pending_node.id())
            ids -= set(table_ids)
            node_count += len(table_ids)
        except Exception:
            logging.exception('Parsing failed')

    new_ids = skipExisting(ids)
    ndb.non_transactional(ndb.put_multi)([
        PendingNode(key=ndb.Key(PendingNode, i)) for i in new_ids])
    logging.info('Discovered new nodes: %s', new_ids)
    pending_node.delete()
    logging.info('Processed %d', pending_node.id())

    minMax = MinMax.INSTANCE_KEY.get()
    if not minMax:
        minMax = MinMax(key=MinMax.INSTANCE_KEY)
    minMax.processed_nodes += node_count
    minMax.put()


@ndb.non_transactional
def fetchNode():
    pending_node = PendingNode.query().order(PendingNode.added_at).fetch(1, keys_only=True)
    if len(pending_node) == 0: return
    pending_node = pending_node[0]
    processsOneNode(pending_node)


class CrawlingPhase(ndb.Model):
    INSTANCE_KEY = ndb.Key('CrawlingPhase', 1)
    _STEPS = [fetchFrontPage, fetchNewest, fetchMin] + [fetchNode for _ in xrange(0, 7)]

    state = ndb.IntegerProperty(default=0, indexed=False)


    @staticmethod
    @ndb.transactional(xg=True, propagation=ndb.TransactionOptions.INDEPENDENT)
    def runNext():
        instance = CrawlingPhase.INSTANCE_KEY.get()
        if not instance:
            instance = CrawlingPhase(key=CrawlingPhase.INSTANCE_KEY)
        step = CrawlingPhase._STEPS[instance.state]
        instance.state = (instance.state + 1) % len(CrawlingPhase._STEPS)
        instance.put()

        try:
            step()
        except Exception:
            logging.exception('Step %s failed', step)      


class Crawler(webapp2.RequestHandler):
    def get(self):
        CrawlingPhase.runNext()
        self.response.write('Done')


app = webapp2.WSGIApplication([
    ('/task/crawl', Crawler),
])

